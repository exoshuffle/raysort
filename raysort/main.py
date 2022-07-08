import csv
import functools
import logging
import os
import random
import time
from typing import Callable, Dict, Iterable, List, Tuple, Union

import numpy as np
import ray

from raysort import (
    config,
    constants,
    logging_utils,
    ray_utils,
    s3_utils,
    sort_utils,
    sortlib,
    tracing_utils,
)
from raysort.config import AppConfig, JobConfig
from raysort.typing import BlockInfo, PartId, PartInfo, SpillingMode


def flatten(xss: List[List]) -> List:
    return [x for xs in xss for x in xs]


def _dummy_sort_and_partition(part: np.ndarray, bounds: List[int]) -> List[BlockInfo]:
    N = len(bounds)
    offset = 0
    size = int(np.ceil(part.size / N))
    blocks = []
    for _ in range(N):
        blocks.append((offset, size))
        offset += size
    return blocks


def mapper_sort_blocks(
    cfg: AppConfig, bounds: List[int], pinfo: PartInfo
) -> Tuple[np.ndarray, List[Tuple[int, int]]]:
    start_time = time.time()
    part = sort_utils.load_partition(cfg, pinfo)
    load_duration = time.time() - start_time
    tracing_utils.record_value("map_load_time", load_duration)
    sort_fn = (
        _dummy_sort_and_partition if cfg.skip_sorting else sortlib.sort_and_partition
    )
    blocks = sort_fn(part, bounds)
    return part, blocks


# Memory usage: input_part_size = 2GB
# Plasma usage: input_part_size = 2GB
@ray.remote(num_cpus=0)
@tracing_utils.timeit("map")
def mapper(
    cfg: AppConfig, _mapper_id: PartId, bounds: List[int], pinfo: PartInfo
) -> List[np.ndarray]:
    part, blocks = mapper_sort_blocks(cfg, bounds, pinfo)
    if cfg.use_put:
        ret = [ray.put(part[offset : offset + size]) for offset, size in blocks]
    else:
        ret = [part[offset : offset + size] for offset, size in blocks]
    # Return an extra object for tracking task progress.
    return ret + [None]


@ray.remote(num_cpus=0)
@tracing_utils.timeit("map")
def mapper_yield(
    cfg: AppConfig, _mapper_id: PartId, bounds: List[int], pinfo: PartInfo
) -> List[np.ndarray]:
    part, blocks = mapper_sort_blocks(cfg, bounds, pinfo)
    for offset, size in blocks:
        yield part[offset : offset + size]
    # Return an extra object for tracking task progress.
    yield None


def _dummy_merge(
    num_blocks: int,
    get_block: Callable[[int, int], np.ndarray],
    _n: int = 0,
    _a: bool = True,
) -> Iterable[np.ndarray]:
    blocks = [((i, 0), get_block(i, 0)) for i in range(num_blocks)]
    while len(blocks) > 0:
        (m, d), block = blocks.pop(random.randrange(len(blocks)))
        yield block
        d_ = d + 1
        block = get_block(m, d_)
        if block is None:
            continue
        blocks.append(((m, d_), block))


def spill_block(cfg: AppConfig, pinfo: PartInfo, data: np.ndarray):
    if cfg.spilling == SpillingMode.DISK:
        # TODO(@lsf): make sure we write to all mounted disks.
        with open(pinfo.path, "wb", buffering=cfg.io_size) as fout:
            data.tofile(fout)
    elif cfg.spilling == SpillingMode.S3:
        s3_utils.upload_s3_buffer(cfg, data, pinfo, use_threads=False)
    else:
        raise RuntimeError(f"{cfg}")


def restore_block(cfg: AppConfig, pinfo: PartInfo) -> np.ndarray:
    if cfg.spilling == SpillingMode.DISK:
        with open(pinfo.path, "rb", buffering=cfg.io_size) as fin:
            ret = np.fromfile(fin, dtype=np.uint8)
        os.remove(pinfo.path)
        return ret
    if cfg.spilling == SpillingMode.S3:
        return s3_utils.download_s3(pinfo, use_threads=False)
    raise RuntimeError(f"{cfg}")


def _get_block(blocks: np.ndarray, i: int, d: int):
    if i >= len(blocks) or d > 0:
        return None
    ret = blocks[i]
    blocks[i] = None
    return ret


# Memory usage: input_part_size * merge_factor / (N/W) * 2 = 320MB
# Plasma usage: input_part_size * merge_factor * 2 = 8GB
@ray.remote(num_cpus=0)
@tracing_utils.timeit("merge")
def merge_blocks(
    cfg: AppConfig,
    merge_id: PartId,
    bounds: List[int],
    blocks: Tuple[np.ndarray],
) -> Union[List[PartInfo], List[np.ndarray]]:
    blocks = list(blocks)
    if isinstance(blocks[0], ray.ObjectRef):
        blocks = ray.get(blocks)

    M = len(blocks)
    total_bytes = sum(b.size for b in blocks)
    num_records = constants.bytes_to_records(total_bytes / len(bounds) * 2)
    get_block = functools.partial(_get_block, blocks)

    merge_fn = _dummy_merge if cfg.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, get_block, num_records, False, bounds)

    ret = []
    spill_tasks = []
    spill_remote = ray_utils.remote(spill_block)
    for i, datachunk in enumerate(merger):
        if cfg.spilling == SpillingMode.RAY:
            if cfg.use_put:
                ret.append(ray.put(datachunk))
            else:
                ret.append(np.copy(datachunk))
            # TODO(@lsf): this function is using more memory in this branch
            # than in the else branch. `del datachunk` helped a little but
            # memory usage is still high. This does not make sense since
            # ray.put() should only use shared memory. Need to investigate.
            del datachunk
            continue
        part_id = constants.merge_part_ids(merge_id, i)
        pinfo = sort_utils.part_info(
            cfg, part_id, kind="temp", s3=(cfg.spilling == SpillingMode.S3)
        )
        if cfg.merge_io_parallelism > 0:
            spill_tasks.append(spill_remote.remote(cfg, pinfo, datachunk))
            if i >= cfg.merge_io_parallelism:
                ray_utils.wait(
                    spill_tasks, num_returns=i - cfg.merge_io_parallelism + 1
                )
        else:
            spill_block(cfg, pinfo, datachunk)
        ret.append(pinfo)
    assert len(ret) == len(bounds), (ret, bounds)
    ray_utils.wait(spill_tasks, wait_all=True)
    return ret


@ray.remote(num_cpus=0)
@tracing_utils.timeit("merge")
def merge_blocks_yield(
    cfg: AppConfig,
    _merge_id: PartId,
    bounds: List[int],
    blocks: Tuple[np.ndarray],
) -> Union[List[PartInfo], List[np.ndarray]]:
    blocks = list(blocks)
    if isinstance(blocks[0], ray.ObjectRef):
        blocks = ray.get(blocks)

    M = len(blocks)
    total_bytes = sum(b.size for b in blocks)
    num_records = constants.bytes_to_records(total_bytes / len(bounds) * 2)
    get_block = functools.partial(_get_block, blocks)

    merge_fn = _dummy_merge if cfg.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, get_block, num_records, False, bounds)

    for datachunk in merger:
        yield datachunk


# Memory usage: merge_partitions.batch_num_records * RECORD_SIZE = 100MB
# Plasma usage: input_part_size = 2GB
@ray.remote
@tracing_utils.timeit("reduce")
def final_merge(
    cfg: AppConfig,
    worker_id: PartId,
    reducer_id: PartId,
    *parts: List,
) -> List[PartInfo]:
    if isinstance(parts[0], PartInfo):
        if cfg.reduce_io_parallelism > 0:
            parts = ray_utils.schedule_tasks(
                restore_block,
                [(cfg, p) for p in parts],
                parallelism=cfg.reduce_io_parallelism,
            )
        else:
            parts = [restore_block(cfg, p) for p in parts]

    M = len(parts)

    def get_block(i: int, d: int) -> np.ndarray:
        if i >= M or d > 0:
            return None
        part = parts[i]
        if part is None:
            return None
        if isinstance(part, np.ndarray):
            return part
        if isinstance(part, ray.ObjectRef):
            ret = ray.get(part)
            assert ret is None or isinstance(ret, np.ndarray), type(ret)
            return ret
        raise RuntimeError(f"{type(part)} {part}")

    part_id = constants.merge_part_ids(worker_id, reducer_id)
    pinfo = sort_utils.part_info(cfg, part_id, kind="output", s3=bool(cfg.s3_buckets))
    merge_fn = _dummy_merge if cfg.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, get_block)
    return sort_utils.save_partition(cfg, pinfo, merger)


def get_boundaries(
    num_map_returns: int, num_merge_returns: int = -1
) -> Tuple[List[int], List[List[int]]]:
    if num_merge_returns == -1:
        return sortlib.get_boundaries(num_map_returns), []
    merge_bounds_flat = sortlib.get_boundaries(num_map_returns * num_merge_returns)
    merge_bounds = (
        np.array(merge_bounds_flat, dtype=np.uint64)
        .reshape(num_map_returns, num_merge_returns)
        .tolist()
    )
    map_bounds = [b[0] for b in merge_bounds]
    return map_bounds, merge_bounds


def get_node_aff(cfg: AppConfig, pinfo: PartInfo, part_id: PartId) -> Dict:
    if pinfo.node:
        return ray_utils.node_ip_aff(cfg, pinfo.node)
    return ray_utils.node_i(cfg, part_id)


@ray.remote(num_cpus=0)
@tracing_utils.timeit("reduce_master")
def reduce_master(cfg: AppConfig, worker_id: int, merge_parts: List) -> List[PartInfo]:
    tasks = []
    for r in range(cfg.num_reducers_per_worker):
        if r >= cfg.reduce_parallelism:
            ray_utils.wait(tasks[: r - cfg.reduce_parallelism + 1], wait_all=True)
        tasks.append(
            final_merge.options(**ray_utils.node_i(cfg, worker_id)).remote(
                cfg, worker_id, r, *merge_parts[r]
            )
        )
    return flatten(ray.get(tasks))


@tracing_utils.timeit("reduce_stage")
def reduce_stage(
    cfg: AppConfig,
    merge_results: np.ndarray,
    get_reduce_master_args: Callable[[int], List],
) -> List[ray.ObjectRef]:
    if cfg.skip_final_reduce:
        ray_utils.wait(merge_results.flatten(), wait_all=True)
        return []

    tasks = [
        reduce_master.options(**ray_utils.node_i(cfg, w)).remote(
            cfg, w, get_reduce_master_args(w)
        )
        for w in range(cfg.num_workers)
    ]
    return flatten(ray.get(tasks))


def sort_simple(cfg: AppConfig, parts: List[PartInfo]) -> List[PartInfo]:
    bounds, _ = get_boundaries(cfg.num_reducers)

    mapper_opt = {"num_returns": cfg.num_reducers + 1}
    map_results = np.empty((cfg.num_mappers, cfg.num_reducers), dtype=object)
    num_map_tasks_per_round = cfg.num_workers * cfg.map_parallelism

    for part_id in range(cfg.num_mappers):
        pinfo = parts[part_id]
        opt = dict(**mapper_opt, **get_node_aff(cfg, pinfo, part_id))
        map_results[part_id, :] = mapper.options(**opt).remote(
            cfg, part_id, bounds, pinfo
        )[: cfg.num_reducers]
        # TODO(@lsf): try memory-aware scheduling
        if part_id > 0 and part_id % num_map_tasks_per_round == 0:
            # Wait for at least one map task from this round to finish before
            # scheduling the next round.
            ray_utils.wait(
                map_results[:, 0], num_returns=part_id - num_map_tasks_per_round + 1
            )

    ray_utils.fail_and_restart_node(cfg)

    return reduce_stage(
        cfg,
        map_results[:, 0],
        lambda w: [
            map_results[:, w * cfg.num_reducers_per_worker + r]
            for r in range(cfg.num_reducers_per_worker)
        ],
    )


def sort_riffle(cfg: AppConfig, parts: List[PartInfo]) -> List[PartInfo]:
    round_merge_factor = cfg.merge_factor // cfg.map_parallelism

    start_time = time.time()
    map_bounds, _ = get_boundaries(1)
    merge_bounds, _ = get_boundaries(cfg.num_reducers)

    mapper_opt = {"num_returns": 2}
    merger_opt = {"num_returns": cfg.num_reducers}
    merge_results = np.empty(
        (cfg.num_workers * cfg.num_mergers_per_worker, cfg.num_reducers),
        dtype=object,
    )
    num_map_tasks_per_round = cfg.num_workers * cfg.map_parallelism

    all_map_results = []
    part_id = 0
    for rnd in range(cfg.num_rounds):
        # Submit map tasks.
        num_map_tasks = min(num_map_tasks_per_round, cfg.num_mappers - part_id)
        map_results = np.empty((cfg.num_workers, cfg.map_parallelism), dtype=object)
        for i in range(num_map_tasks):
            pinfo = parts[part_id]
            opt = dict(**mapper_opt, **get_node_aff(cfg, pinfo, part_id))
            m = part_id % num_map_tasks_per_round
            map_results[i % cfg.num_workers, i // cfg.num_workers] = mapper.options(
                **opt
            ).remote(cfg, part_id, map_bounds, pinfo)[0]
            part_id += 1
        all_map_results.append(map_results)

        if (rnd + 1) % round_merge_factor == 0:
            # Make sure all merge tasks previous rounds finish.
            num_extra_rounds = (
                rnd + 1
            ) // round_merge_factor - cfg.num_concurrent_rounds
            if num_extra_rounds > 0:
                ray_utils.wait(
                    merge_results[:, 0].flatten(),
                    num_returns=num_extra_rounds * cfg.num_workers,
                )

            # Submit merge tasks.
            merge_start = rnd - round_merge_factor + 1
            merge_end = rnd + 1
            map_results_to_merge = np.concatenate(
                all_map_results[merge_start:merge_end], axis=1
            )
            # Release map result references.
            for i in range(merge_start, merge_end):
                all_map_results[i] = []
            m = rnd
            for w in range(cfg.num_workers):
                map_blocks = map_results_to_merge[w, :]
                merge_id = constants.merge_part_ids(w, m)
                merge_results[w + m * cfg.num_workers, :] = merge_blocks.options(
                    **merger_opt, **ray_utils.node_i(cfg, w)
                ).remote(cfg, merge_id, merge_bounds, map_blocks.tolist())

        if start_time > 0 and (time.time() - start_time) > cfg.fail_time:
            ray_utils.fail_and_restart_node(cfg)
            start_time = -1

        # Wait for at least one map task from this round to finish before
        # scheduling the next round.
        ray_utils.wait(map_results.flatten())

    ray_utils.fail_and_restart_node(cfg)

    return reduce_stage(
        cfg,
        merge_results,
        lambda w: [
            merge_results[:, w * cfg.num_reducers_per_worker + r]
            for r in range(cfg.num_reducers_per_worker)
        ],
    )


def sort_two_stage(cfg: AppConfig, parts: List[PartInfo]) -> List[PartInfo]:
    start_time = time.time()
    ref_recorder = tracing_utils.ObjectRefRecorder(cfg.record_object_refs)
    map_bounds, merge_bounds = get_boundaries(
        cfg.num_workers, cfg.num_reducers_per_worker
    )

    map_fn = mapper_yield if cfg.use_yield else mapper
    merge_fn = merge_blocks_yield if cfg.use_yield else merge_blocks

    mapper_opt = {"num_returns": cfg.num_workers + 1}
    merger_opt = {"num_returns": cfg.num_reducers_per_worker}
    merge_results = np.empty(
        (cfg.num_workers, cfg.num_mergers_per_worker, cfg.num_reducers_per_worker),
        dtype=object,
    )
    num_map_tasks_per_round = cfg.num_workers * cfg.map_parallelism

    all_map_results = []  # For Magnet.
    map_tasks = []
    part_id = 0
    for rnd in range(cfg.num_rounds):
        # Wait for the previous round of map tasks to finish.
        num_extra_rounds = rnd - cfg.num_concurrent_rounds + 1
        if num_extra_rounds > 0:
            ray_utils.wait(map_tasks)

        # Submit a new round of map tasks.
        num_map_tasks = min(num_map_tasks_per_round, cfg.num_mappers - part_id)
        map_results = np.empty((num_map_tasks, cfg.num_workers + 1), dtype=object)
        for _ in range(num_map_tasks):
            pinfo = parts[part_id]
            opt = dict(**mapper_opt, **get_node_aff(cfg, pinfo, part_id))
            m = part_id % num_map_tasks_per_round
            refs = map_fn.options(**opt).remote(cfg, part_id, map_bounds, pinfo)
            map_results[m, :] = refs
            ref_recorder.record(
                refs, lambda i, part_id=part_id: f"map_{part_id:010x}_{i}"
            )
            part_id += 1

        # Keep references to the map tasks but not to map output blocks.
        map_tasks = map_results[:, cfg.num_workers]

        # Wait for the previous round of merge tasks to finish.
        num_extra_rounds = rnd - cfg.num_concurrent_rounds + 1
        if num_extra_rounds > 0:
            ray_utils.wait(
                merge_results[:, :, 0].flatten(),
                num_returns=num_extra_rounds * cfg.merge_parallelism * cfg.num_workers,
            )

        # Submit a new round of merge tasks.
        f = int(np.ceil(num_map_tasks / cfg.merge_parallelism))
        for j in range(cfg.merge_parallelism):
            m = rnd * cfg.merge_parallelism + j
            for w in range(cfg.num_workers):
                map_blocks = map_results[j * f : (j + 1) * f, w]
                merge_id = constants.merge_part_ids(w, m)
                refs = merge_fn.options(
                    **merger_opt, **ray_utils.node_i(cfg, w)
                ).remote(cfg, merge_id, merge_bounds[w], map_blocks.tolist())
                merge_results[w, m, :] = refs
                ref_recorder.record(
                    refs, lambda i, merge_id=merge_id: f"merge_{merge_id:010x}_{i}"
                )

        ref_recorder.flush()

        # Delete references to map output blocks as soon as possible.
        if cfg.magnet:
            all_map_results.append(map_results)
        else:
            map_results = None

        if cfg.fail_node and start_time and time.time() - start_time > cfg.fail_time:
            ray_utils.fail_and_restart_node(cfg)
            start_time = None

    return reduce_stage(
        cfg,
        merge_results,
        lambda w: [merge_results[w, :, r] for r in range(cfg.num_reducers_per_worker)],
    )


def sort_reduce_only(cfg: AppConfig) -> List[PartInfo]:
    num_returns = cfg.num_reducers_per_worker
    bounds, _ = get_boundaries(num_returns)
    merger_opt = {"num_returns": num_returns + 1}
    merge_results = np.empty(
        (cfg.num_workers, cfg.num_mergers_per_worker, num_returns),
        dtype=object,
    )
    for rnd in range(cfg.num_rounds):
        for j in range(cfg.merge_parallelism):
            m = rnd * cfg.merge_parallelism + j
            for w in range(cfg.num_workers):
                merge_results[w, m, :] = mapper_yield.options(
                    **merger_opt, **ray_utils.node_i(cfg, w)
                ).remote(cfg, None, bounds, None)[:num_returns]

    return reduce_stage(
        cfg,
        merge_results,
        lambda w: [merge_results[w, :, r] for r in range(cfg.num_reducers_per_worker)],
    )


@tracing_utils.timeit("sort", log_to_wandb=True)
def sort_main(cfg: AppConfig):
    parts = sort_utils.load_manifest(cfg)

    if cfg.simple_shuffle:
        results = sort_simple(cfg, parts)
    elif cfg.riffle:
        results = sort_riffle(cfg, parts)
    elif cfg.skip_first_stage:
        results = sort_reduce_only(cfg)
    else:
        results = sort_two_stage(cfg, parts)

    if not cfg.skip_output:
        with open(sort_utils.get_manifest_file(cfg, kind="output"), "w") as fout:
            writer = csv.writer(fout)
            for pinfo in results:
                writer.writerow(pinfo.to_csv_row())


def init(job_cfg: JobConfig) -> ray.actor.ActorHandle:
    logging_utils.init()
    logging.info("Job config: %s", job_cfg.name)
    ray_utils.init(job_cfg)
    sort_utils.init(job_cfg.app)
    return tracing_utils.create_progress_tracker(job_cfg)


def main():
    job_cfg = config.get()
    tracker = init(job_cfg)
    cfg = job_cfg.app
    try:
        if cfg.generate_input:
            sort_utils.generate_input(cfg)

        if cfg.sort:
            sort_main(cfg)

        if cfg.validate_output:
            sort_utils.validate_output(cfg)
    finally:
        ray.get(tracker.performance_report.remote())
        ray.shutdown()


if __name__ == "__main__":
    main()
