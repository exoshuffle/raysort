import csv
import functools
import logging
import time
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
            start = time.time()
            ready, not_ready = ray.wait(
                refs,
                num_returns=int(len(refs) * cfg.shuffle_wait_percentile),
            )
            # print("shuffle first half wait", len(ready), time.time() - start)
            start = time.time()
            late, timeout_refs = ray.wait(
                not_ready,
                num_returns=len(not_ready),
                timeout=cfg.shuffle_wait_timeout,
            )
            refs = ready + late
            # print("shuffle second half wait", len(refs), time.time() - start)
            if timeout_refs:
                logging.info(
                    f"got {len(refs)}/{len(blocks)}, timeout {len(timeout_refs)}"
                )
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

    map_id = 0
    num_shards = cfg.num_shards_per_mapper
    num_rounds = int(np.ceil(cfg.num_mappers / cfg.num_workers / cfg.merge_factor))
    num_map_tasks_per_round = cfg.num_workers * cfg.merge_factor
    num_concurrent_rounds = cfg.map_parallelism // cfg.merge_factor - 1
    merge_tasks_in_flight = []
    timeout_map_blocks = [[]] * cfg.num_workers
    last_merge_submit_time = None
    merge_results = np.empty(
        (cfg.num_workers, num_rounds + 1, cfg.num_reducers_per_worker),
        dtype=object,
    )

    def submit_merge_tasks(m, get_map_blocks, allow_timeouts=True):
        nonlocal last_merge_submit_time
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
        t = time.perf_counter()
        if last_merge_submit_time is not None:
            tracing_utils.record_value("merge_gap", t - last_merge_submit_time)
        last_merge_submit_time = t
        return ret

    for rnd in range(num_rounds):
        # Submit a new round of map tasks.
        num_map_tasks = min(num_map_tasks_per_round, cfg.num_mappers - map_id)
        map_results = np.empty((num_map_tasks, cfg.num_workers + 1), dtype=object)
        for _ in range(num_map_tasks):
            pinfolist = parts[map_id * num_shards : (map_id + 1) * num_shards]
            opt = dict(**mapper_opt, **ray_utils.node_i(cfg, map_id))
            m = map_id % num_map_tasks_per_round
            refs = mapper_yield.options(**opt).remote(
                cfg, map_id, map_bounds, pinfolist
            )
            map_results[m, :] = refs
            map_id += 1

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
    merge_task_returns = [ray.get(tasks) for tasks in merge_tasks_in_flight]
    timeout_map_blocks = [sum(reflists, []) for reflists in zip(*merge_task_returns)]
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


def sort_optimized_2(cfg: AppConfig, parts: list[PartInfo]) -> list[PartInfo]:
    assert cfg.merge_factor == 1, cfg
    map_bounds, merge_bounds = get_boundaries(
        cfg.num_workers, cfg.num_reducers_per_worker
    )

    mapper_opt = {"num_returns": cfg.num_workers + 1}
    merger_opt = {"num_returns": cfg.num_reducers_per_worker + 1}

    num_shards = cfg.num_shards_per_mapper
    wait_batch = 1
    merge_round = 0
    map_in_flight = {}
    merges_in_flight = {}
    all_merge_out = []
    node_slots = np.zeros(cfg.num_workers, dtype=np.int8)
    node_usage_counter = np.zeros(cfg.num_workers, dtype=np.int8)

    def _get_map_node():
        ret = np.argmin(node_slots)
        node_slots[ret] += 1
        node_usage_counter[ret] += 1
        return ret

    def _wait_map_node(num_returns: int = wait_batch):
        ready, _ = ray.wait(
            list(map_in_flight.keys()), num_returns=num_returns, fetch_local=False
        )
        for task in ready:
            node = map_in_flight.pop(task)
            node_slots[node] -= 1

    def _submit_map(map_id: int):
        pinfolist = parts[map_id * num_shards : (map_id + 1) * num_shards]
        node_id = _get_map_node()
        opt = dict(**mapper_opt, **ray_utils.node_i(cfg, node_id))
        refs = mapper_yield.options(**opt).remote(cfg, map_id, map_bounds, pinfolist)
        task = refs[cfg.num_workers]
        map_in_flight[task] = node_id
        return refs[: cfg.num_workers]

    def _submit_merge(w: int, map_out: np.ndarray):
        refs = merge_blocks_yield.options(
            **merger_opt, **ray_utils.node_i(cfg, w)
        ).remote(
            cfg,
            constants.merge_part_ids(w, merge_round),
            merge_bounds[w],
            map_out,
            allow_timeouts=False,
        )
        merges_in_flight[refs[0]] = w
        return refs

    def _submit_merge_round(all_map_out):
        return np.array(
            [_submit_merge(w, all_map_out[:, w]) for w in range(cfg.num_workers)]
        )

    def _submit_merge_round_with_wait(all_map_out):
        tasks = np.empty(
            (cfg.num_workers, cfg.num_reducers_per_worker + 1), dtype=object
        )
        start = time.time()
        not_ready = list(merges_in_flight.keys())
        num_submitted = 0
        num_waited = 0
        while num_submitted < cfg.num_workers:
            ready, not_ready = ray.wait(
                not_ready, num_returns=wait_batch, fetch_local=False
            )
            num_waited += len(ready)
            for task in ready:
                w = merges_in_flight.pop(task)
                if tasks[w, 0] is None:
                    tasks[w, :] = _submit_merge(w, all_map_out[:, w])
                    num_submitted += 1
                    # TODO(@lsf) What if we don't wait for this tail?
                    # These are the stragglers that we have been hunting for.
                    if num_submitted >= cfg.shuffle_wait_percentile * cfg.num_workers:
                        not_ready_nodes = [
                            w for w in range(cfg.num_workers) if tasks[w, 0] is None
                        ]
                        logging.info(
                            "Waited %.1f seconds; %d nodes ready; not ready: %s; %d merge tasks completed",
                            time.time() - start,
                            num_submitted,
                            not_ready_nodes,
                            num_waited,
                        )
                        for w in not_ready_nodes:
                            tasks[w, :] = _submit_merge(w, all_map_out[:, w])
                            num_submitted += 1
        return tasks

    def _merge_map_out(all_map_out):
        nonlocal merge_round
        all_map_out = np.array(all_map_out)
        if len(merges_in_flight) >= cfg.merge_parallelism * cfg.num_workers:
            tasks = _submit_merge_round_with_wait(all_map_out)
        else:
            tasks = _submit_merge_round(all_map_out)
        logging.info("Submitted merge round %d", merge_round)
        all_merge_out.append(tasks[:, 1:])
        merge_round += 1

    # Main loop.
    all_map_out = []
    for map_id in range(cfg.num_mappers):
        if len(map_in_flight) >= cfg.map_parallelism * cfg.num_workers:
            _wait_map_node()
        all_map_out.append(_submit_map(map_id))
        if len(all_map_out) >= cfg.merge_factor * cfg.num_workers:
            _merge_map_out(all_map_out)
            all_map_out = []
    if all_map_out:
        _merge_map_out(all_map_out)

    merge_results = np.array(all_merge_out)
    # Reduce stage.
    get_reduce_master_args = lambda w: [
        merge_results[:, w, r] for r in range(cfg.num_reducers_per_worker)
    ]
    tasks = [
        reduce_master.options(**ray_utils.node_i(cfg, w)).remote(
            cfg, w, get_reduce_master_args(w)
        )
        for w in range(cfg.num_workers)
    ]
    ret = flatten(ray.get(tasks))
    logging.info("Mapper nodes usage: %s", node_usage_counter)
    return ret


def sort_main(cfg: AppConfig):
    parts = sort_utils.load_manifest(cfg)
    # results = sort_optimized(cfg, parts)
    results = sort_optimized_2(cfg, parts)

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

        with tracing_utils.timeit("sort", log_to_wandb=True):
            sort_main(cfg)

        if cfg.validate_output:
            sort_utils.validate_output(cfg)
    finally:
        ray.get(tracker.performance_report.remote())
        ray.shutdown()


if __name__ == "__main__":
    main()
