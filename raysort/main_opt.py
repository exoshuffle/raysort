import csv
import functools
import logging
from typing import Iterable, Optional, Union

import numpy as np
import ray

from raysort import (
    config,
    constants,
    logging_utils,
    ray_utils,
    sort_utils,
    sortlib,
    tracing_utils,
)
from raysort.config import AppConfig, JobConfig
from raysort.typing import PartId, PartInfo


def flatten(xss: list[list]) -> list:
    return [x for xs in xss for x in xs]


def mapper_sort_blocks(
    cfg: AppConfig, bounds: list[int], pinfolist: list[PartInfo]
) -> tuple[np.ndarray, list[tuple[int, int]]]:
    with tracing_utils.timeit("map_load", report_completed=False):
        part = sort_utils.load_partitions(cfg, pinfolist)
    blocks = sortlib.sort_and_partition(part, bounds)
    # TODO(@lsf): block until object store has enough memory for these blocks to
    # prevent spilling.
    return part, blocks


@ray.remote(num_cpus=0)
def mapper_yield(
    cfg: AppConfig, _mapper_id: PartId, bounds: list[int], pinfolist: list[PartInfo]
) -> Iterable[Optional[np.ndarray]]:
    with tracing_utils.timeit("map"):
        part, blocks = mapper_sort_blocks(cfg, bounds, pinfolist)
        for offset, size in blocks:
            yield part[offset : offset + size]
        # Return an extra object for tracking task progress.
        yield None


def _get_block(blocks: np.ndarray, i: int, d: int):
    if i >= len(blocks) or d > 0:
        return None
    ret = blocks[i]
    # TODO(@lsf): This doesn't free the primary copies because Ray's distributed
    # ref counting is at task granularity. i.e. Objects don't get freed even if
    # their Python references are gone until the task completes.
    blocks[i] = None
    return ret


def _merge_blocks_prep(
    cfg: AppConfig,
    bounds: list[int],
    blocks: tuple[ray.ObjectRef],
    allow_timeouts: bool,
) -> tuple[Iterable[np.ndarray], list[ray.ObjectRef]]:
    refs = list(blocks)
    timeout_refs = []
    with tracing_utils.timeit("shuffle", report_completed=False):
        if allow_timeouts:
            ready, not_ready = ray.wait(
                refs,
                num_returns=int(len(refs) * cfg.shuffle_wait_percentile),
            )
            late, timeout_refs = ray.wait(
                not_ready,
                num_returns=len(not_ready),
                timeout=cfg.shuffle_wait_timeout,
            )
            refs = ready + late
            if timeout_refs:
                print(f"got {len(refs)}/{len(blocks)}, timeout {len(timeout_refs)}")
        blocks = ray.get(refs)
        if isinstance(blocks[0], ray.ObjectRef):
            blocks = ray.get(blocks)

    total_bytes = sum(b.size for b in blocks)
    num_records = constants.bytes_to_records(total_bytes / len(bounds) * 2)
    get_block = functools.partial(_get_block, blocks)

    return (
        sortlib.merge_partitions(len(blocks), get_block, num_records, False, bounds),
        timeout_refs,
    )


@ray.remote(num_cpus=0)
def merge_blocks_yield(
    cfg: AppConfig,
    _merge_id: PartId,
    bounds: list[int],
    blocks: tuple[np.ndarray],
    allow_timeouts: bool = False,
) -> Union[list[ray.ObjectRef], Iterable[PartInfo], Iterable[np.ndarray]]:
    merger, timeouts = _merge_blocks_prep(cfg, bounds, blocks, allow_timeouts)
    yield timeouts
    with tracing_utils.timeit("merge"):
        for datachunk in merger:
            yield datachunk


# Memory usage: merge_partitions.batch_num_records * RECORD_SIZE = 100MB
# Plasma usage: input_part_size = 2GB
@ray.remote(num_cpus=0)
def final_merge(
    cfg: AppConfig,
    worker_id: PartId,
    reducer_id: PartId,
    *parts: list,
) -> list[PartInfo]:
    with tracing_utils.timeit("reduce"):
        M = len(parts)

        def get_block(i: int, d: int) -> Optional[np.ndarray]:
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
        pinfo = sort_utils.part_info(
            cfg, part_id, kind="output", cloud=cfg.cloud_storage
        )
        merger = sortlib.merge_partitions(M, get_block)
        return sort_utils.save_partition(cfg, pinfo, merger)


def get_boundaries(
    num_map_returns: int, num_merge_returns: int = -1
) -> tuple[list[int], list[list[int]]]:
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


@ray.remote(num_cpus=0)
def reduce_master(cfg: AppConfig, worker_id: int, merge_parts: list) -> list[PartInfo]:
    with tracing_utils.timeit("reduce_master"):
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


def sort_optimized(cfg: AppConfig, parts: list[PartInfo]) -> list[PartInfo]:
    map_bounds, merge_bounds = get_boundaries(
        cfg.num_workers, cfg.num_reducers_per_worker
    )

    mapper_opt = {"num_returns": cfg.num_workers + 1}
    merger_opt = {"num_returns": cfg.num_reducers_per_worker + 1}

    part_id = 0
    num_shards = cfg.num_shards_per_mapper
    num_rounds = int(np.ceil(cfg.num_mappers / cfg.num_workers / cfg.merge_factor))
    num_map_tasks_per_round = cfg.num_workers * cfg.merge_factor
    num_concurrent_rounds = cfg.map_parallelism // 4 * 3
    merge_tasks_in_flight = []
    timeout_map_blocks = [[]] * cfg.num_workers
    merge_results = np.empty(
        (cfg.num_workers, num_rounds + 1, cfg.num_reducers_per_worker),
        dtype=object,
    )

    def submit_merge_tasks(m, get_map_blocks, allow_timeouts=True):
        ret = []
        for w in range(cfg.num_workers):
            map_blocks = get_map_blocks(w)
            if len(map_blocks) == 0:
                continue
            merge_id = constants.merge_part_ids(w, m)
            refs = merge_blocks_yield.options(
                **merger_opt, **ray_utils.node_i(cfg, w)
            ).remote(
                cfg,
                merge_id,
                merge_bounds[w],
                map_blocks,
                allow_timeouts=allow_timeouts,
            )
            ret.append(refs[0])
            merge_results[w, m, :] = refs[1:]
        return ret

    for rnd in range(num_rounds):
        # Submit a new round of map tasks.
        num_map_tasks = min(num_map_tasks_per_round, cfg.num_mappers - part_id)
        map_results = np.empty((num_map_tasks, cfg.num_workers + 1), dtype=object)
        for _ in range(num_map_tasks):
            pinfolist = parts[part_id * num_shards : (part_id + 1) * num_shards]
            opt = dict(**mapper_opt, **ray_utils.node_ip_aff(cfg, pinfolist[0].node))
            m = part_id % num_map_tasks_per_round
            refs = mapper_yield.options(**opt).remote(
                cfg, part_id, map_bounds, pinfolist
            )
            map_results[m, :] = refs
            part_id += 1

        # Wait for all merge tasks from the previous round to finish.
        if rnd >= num_concurrent_rounds:
            tasks = merge_tasks_in_flight.pop(0)
            timeout_map_blocks = ray.get(tasks)
            # TODO(@lsf): do something about the timeout map blocks.
            # For example, launch competing tasks?

        # Submit a new round of merge tasks.
        merge_tasks = submit_merge_tasks(
            rnd,
            lambda w: map_results[:, w].tolist() + timeout_map_blocks[w],
        )
        merge_tasks_in_flight.append(merge_tasks)

        # Delete references to map output blocks as soon as possible.
        del map_results

    # Handle the last few rounds' timeout map blocks.
    timeout_map_blocks = np.sum(
        [ray.get(merge_tasks) for merge_tasks in merge_tasks_in_flight], axis=0
    )
    submit_merge_tasks(
        num_rounds, lambda w: timeout_map_blocks[w], allow_timeouts=False
    )

    # Reduce stage.
    get_reduce_master_args = lambda w: [
        merge_results[w, :, r] for r in range(cfg.num_reducers_per_worker)
    ]
    tasks = [
        reduce_master.options(**ray_utils.node_i(cfg, w)).remote(
            cfg, w, get_reduce_master_args(w)
        )
        for w in range(cfg.num_workers)
    ]
    return flatten(ray.get(tasks))


def sort_main(cfg: AppConfig):
    parts = sort_utils.load_manifest(cfg)
    results = sort_optimized(cfg, parts)

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
            with tracing_utils.timeit("sort", log_to_wandb=True):
                sort_main(cfg)

        if cfg.validate_output:
            sort_utils.validate_output(cfg)
    finally:
        ray.get(tracker.performance_report.remote())
        ray.shutdown()


if __name__ == "__main__":
    main()
