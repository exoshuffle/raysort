import csv
import logging
import os
import random
import time
from typing import Callable, Dict, Iterable, List, Tuple, Union

import numpy as np
import ray

from raysort import config
from raysort import constants
from raysort import logging_utils
from raysort import ray_utils
from raysort import s3_utils
from raysort import sortlib
from raysort import sort_utils
from raysort import tracing_utils
from raysort.config import AppConfig, JobConfig
from raysort.typing import BlockInfo, PartId, PartInfo, Path, SpillingMode


def _dummy_sort_and_partition(part: np.ndarray, bounds: List[int]) -> List[BlockInfo]:
    N = len(bounds)
    offset = 0
    size = int(np.ceil(part.size / N))
    blocks = []
    for _ in range(N):
        blocks.append((offset, size))
        offset += size
    return blocks


# Memory usage: input_part_size = 2GB
# Plasma usage: input_part_size = 2GB
@ray.remote(num_cpus=0)
@tracing_utils.timeit("map")
def mapper(
    cfg: AppConfig,
    mapper_id: PartId,
    bounds: List[int],
    path: Path,
) -> List[np.ndarray]:
    start_time = time.time()
    part = sort_utils.load_partition(cfg, path)
    assert part.size == cfg.input_part_size, (part.shape, path, cfg)
    load_duration = time.time() - start_time
    tracing_utils.record_value("map_load_time", load_duration)
    sort_fn = (
        _dummy_sort_and_partition if cfg.skip_sorting else sortlib.sort_and_partition
    )
    blocks = sort_fn(part, bounds)
    if cfg.use_put:
        ret = [ray.put(part[offset : offset + size]) for offset, size in blocks]
    else:
        ret = [part[offset : offset + size] for offset, size in blocks]
    # Return an extra object for tracking task progress.
    return ret + [None]


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
        s3_utils.upload_s3_buffer(cfg, data, pinfo.path)
    else:
        raise RuntimeError(f"{cfg}")


def restore_block(cfg: AppConfig, part: PartInfo) -> np.ndarray:
    if cfg.spilling == SpillingMode.DISK:
        with open(part.path, "rb", buffering=cfg.io_size) as fin:
            ret = np.fromfile(fin, dtype=np.uint8)
        os.remove(part.path)
        return ret
    if cfg.spilling == SpillingMode.S3:
        return s3_utils.download_s3(cfg.s3_bucket, part.path)
    raise RuntimeError(f"{cfg}")


# Memory usage: input_part_size * merge_factor / (N/W) * 2 = 320MB
# Plasma usage: input_part_size * merge_factor * 2 = 8GB
@ray.remote(num_cpus=0)
@tracing_utils.timeit("merge")
def merge_mapper_blocks(
    cfg: AppConfig,
    worker_id: PartId,
    merge_id: PartId,
    bounds: List[int],
    *blocks: Tuple[np.ndarray],
) -> Union[List[PartInfo], List[np.ndarray]]:
    blocks = list(blocks)
    if isinstance(blocks[0], ray.ObjectRef):
        blocks = ray.get(blocks)

    M = len(blocks)
    total_bytes = sum(b.size for b in blocks)
    num_records = int(total_bytes / len(bounds) * 2 // constants.RECORD_SIZE)

    def get_block(i, d):
        if i >= M or d > 0:
            return None
        return blocks[i]

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
        part_id = constants.merge_part_ids(worker_id, merge_id, i)
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


# Memory usage: merge_partitions.batch_num_records * RECORD_SIZE = 100MB
# Plasma usage: input_part_size = 2GB
@ray.remote
@tracing_utils.timeit("reduce")
def final_merge(
    cfg: AppConfig,
    worker_id: PartId,
    reducer_id: PartId,
    *parts: List,
) -> PartInfo:
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
        raise RuntimeError(f"{cfg}")

    part_id = constants.merge_part_ids(worker_id, reducer_id)
    pinfo = sort_utils.part_info(cfg, part_id, kind="output", s3=cfg.s3_bucket)
    merge_fn = _dummy_merge if cfg.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, get_block)
    sort_utils.save_partition(cfg, pinfo.path, merger)
    return pinfo


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


def _get_node_res(cfg: AppConfig, pinfo: PartInfo, part_id: PartId) -> Dict:
    if pinfo.node:
        return ray_utils.node_res(pinfo.node)
    return ray_utils.node_i(cfg, part_id)


def reduce_stage(
    cfg: AppConfig,
    merge_results: np.ndarray,
    get_reduce_args: Callable[[int, int], List],
    post_reduce_callback: Callable[[int], None] = lambda _: None,
) -> List[PartInfo]:
    if cfg.skip_final_reduce:
        ray_utils.wait(merge_results.flatten(), wait_all=True)
        return []

    def get_task_options(w: int) -> Dict:
        if cfg.free_scheduling:
            return {
                "resources": {constants.WORKER_RESOURCE: 1 / cfg.reduce_parallelism},
                "scheduling_strategy": "SPREAD",
            }
        else:
            return ray_utils.node_i(cfg, w)

    # Submit second-stage reduce tasks.
    reduce_results = np.empty(
        (cfg.num_workers, cfg.num_reducers_per_worker), dtype=object
    )
    for r in range(cfg.num_reducers_per_worker):
        # At most (cfg.reduce_parallelism * cfg.num_workers) concurrent tasks.
        if r >= cfg.reduce_parallelism:
            ray_utils.wait(
                reduce_results[:, : r - cfg.reduce_parallelism + 1].flatten(),
                wait_all=True,
            )
        reduce_results[:, r] = [
            final_merge.options(**get_task_options(w)).remote(
                cfg, w, r, *get_reduce_args(w, r)
            )
            for w in range(cfg.num_workers)
        ]
        post_reduce_callback(r)

    return ray.get(reduce_results.flatten().tolist())


def sort_simple(cfg: AppConfig, parts: List[PartInfo]) -> List[PartInfo]:
    bounds, _ = get_boundaries(cfg.num_reducers)

    mapper_opt = {"num_returns": cfg.num_reducers + 1}
    map_results = np.empty((cfg.num_mappers, cfg.num_reducers), dtype=object)
    num_map_tasks_per_round = cfg.num_workers * cfg.map_parallelism

    for part_id in range(cfg.num_mappers):
        pinfo = parts[part_id]
        opt = dict(**mapper_opt, **_get_node_res(cfg, pinfo, part_id))
        map_results[part_id, :] = mapper.options(**opt).remote(
            cfg, part_id, bounds, pinfo.path
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
        lambda w, r: map_results[:, w * cfg.num_reducers_per_worker + r],
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
    for round in range(cfg.num_rounds):
        # Submit map tasks.
        num_map_tasks = min(num_map_tasks_per_round, cfg.num_mappers - part_id)
        map_results = np.empty((cfg.num_workers, cfg.map_parallelism), dtype=object)
        for i in range(num_map_tasks):
            pinfo = parts[part_id]
            opt = dict(**mapper_opt, **_get_node_res(cfg, pinfo, part_id))
            m = part_id % num_map_tasks_per_round
            map_results[i % cfg.num_workers, i // cfg.num_workers] = mapper.options(
                **opt
            ).remote(cfg, part_id, map_bounds, pinfo.path)[0]
            part_id += 1
        all_map_results.append(map_results)

        if (round + 1) % round_merge_factor == 0:
            # Make sure all merge tasks previous rounds finish.
            num_extra_rounds = (
                round + 1
            ) // round_merge_factor - cfg.num_concurrent_rounds
            if num_extra_rounds > 0:
                ray_utils.wait(
                    merge_results[:, 0].flatten(),
                    num_returns=num_extra_rounds * cfg.num_workers,
                )

            # Submit merge tasks.
            merge_start = round - round_merge_factor + 1
            merge_end = round + 1
            map_results_to_merge = np.concatenate(
                all_map_results[merge_start:merge_end], axis=1
            )
            # Release map result references.
            for i in range(merge_start, merge_end):
                all_map_results[i] = []
            m = round
            for w in range(cfg.num_workers):
                map_blocks = map_results_to_merge[w, :]
                merge_results[w + m * cfg.num_workers, :] = merge_mapper_blocks.options(
                    **merger_opt, **ray_utils.node_i(cfg, w)
                ).remote(cfg, w, m, merge_bounds, *map_blocks)

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
        lambda w, r: merge_results[:, w * cfg.num_reducers_per_worker + r],
    )


def sort_two_stage(cfg: AppConfig, parts: List[PartInfo]) -> List[PartInfo]:
    start_time = time.time()
    map_bounds, merge_bounds = get_boundaries(
        cfg.num_workers, cfg.num_reducers_per_worker
    )

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
    for round in range(cfg.num_rounds):
        # Wait for the previous round of map tasks to finish.
        num_extra_rounds = round - cfg.num_concurrent_rounds + 1
        if num_extra_rounds > 0:
            ray_utils.wait(map_tasks, wait_all=True)

        # Submit a new round of map tasks.
        num_map_tasks = min(num_map_tasks_per_round, cfg.num_mappers - part_id)
        map_results = np.empty((num_map_tasks, cfg.num_workers + 1), dtype=object)
        for _ in range(num_map_tasks):
            pinfo = parts[part_id]
            opt = dict(**mapper_opt, **_get_node_res(args, pinfo, part_id))
            m = part_id % num_map_tasks_per_round
            map_results[m, :] = mapper.options(**opt).remote(
                cfg, part_id, map_bounds, pinfo.path
            )
            part_id += 1

        # Keep references to the map tasks but not to map output blocks.
        map_tasks = map_results[:, cfg.num_workers]

        # Wait for the previous round of merge tasks to finish.
        num_extra_rounds = round - cfg.num_concurrent_rounds + 1
        if num_extra_rounds > 0:
            ray_utils.wait(
                merge_results[:, :, 0].flatten(),
                num_returns=num_extra_rounds * cfg.merge_parallelism * cfg.num_workers,
            )

        # Submit a new round of merge tasks.
        f = int(np.ceil(num_map_tasks / cfg.merge_parallelism))
        for j in range(cfg.merge_parallelism):
            m = round * cfg.merge_parallelism + j
            for w in range(cfg.num_workers):
                map_blocks = map_results[j * f : (j + 1) * f, w]
                merge_results[w, m, :] = merge_mapper_blocks.options(
                    **merger_opt, **ray_utils.node_i(cfg, w)
                ).remote(cfg, w, m, merge_bounds[w], *map_blocks)

        # Delete references to map output blocks as soon as possible.
        if cfg.magnet:
            all_map_results.append(map_results)
        else:
            map_results = None

        if cfg.fail_node and start_time and time.time() - start_time > cfg.fail_time:
            ray_utils.fail_and_restart_node(cfg)
            start_time = None

    def post_reduce(r: int) -> None:
        merge_results[:, :, r] = None

    return reduce_stage(
        cfg,
        merge_results,
        lambda w, r: merge_results[w, :, r],
        post_reduce,
    )


@tracing_utils.timeit("sort", log_to_wandb=True)
def sort_main(cfg: AppConfig):
    parts = sort_utils.load_manifest(cfg)

    if cfg.simple_shuffle:
        results = sort_simple(cfg, parts)
    elif cfg.riffle:
        results = sort_riffle(cfg, parts)
    else:
        results = sort_two_stage(cfg, parts)

    if not cfg.skip_output:
        with open(sort_utils.get_manifest_file(cfg, kind="output"), "w") as fout:
            writer = csv.writer(fout)
            writer.writerows(results)


def init(job_cfg: JobConfig, job_cfg_name: str) -> ray.actor.ActorHandle:
    logging_utils.init()
    logging.info(f"Job config: {job_cfg_name}")
    ray_utils.init(job_cfg)
    sort_utils.init(job_cfg.app)
    return tracing_utils.create_progress_tracker(job_cfg)


def main():
    job_cfg, job_cfg_name = config.get()
    tracker = init(job_cfg, job_cfg_name)
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


if __name__ == "__main__":
    main()
