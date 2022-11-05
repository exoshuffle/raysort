import collections
import csv
import functools
import logging
import threading
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
            block = np.zeros(0, dtype=np.uint8) if cfg.skip_sorting else part[offset : offset + size]
            yield block
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
) -> tuple[Iterable[np.ndarray], list[ray.ObjectRef]]:
    refs = list(blocks)
    timeout_refs = []
    with tracing_utils.timeit("shuffle", report_completed=False):
        blocks = ray.get(refs)
        if isinstance(blocks[0], ray.ObjectRef):
            blocks = ray.get(blocks)

    total_bytes = sum(b.size for b in blocks)
    num_records = constants.bytes_to_records(total_bytes / len(bounds) * 2)
    get_block = functools.partial(_get_block, blocks)

    if cfg.skip_sorting:

        def noop_merge():
            chunksize = total_bytes // len(bounds)
            if chunksize == 0:
                chunksize = 20_000_000
            for _ in bounds:
                yield np.zeros(chunksize, dtype=np.uint8)

        return noop_merge(), timeout_refs

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
) -> Union[list[ray.ObjectRef], Iterable[PartInfo], Iterable[np.ndarray]]:
    merger, timeouts = _merge_blocks_prep(cfg, bounds, blocks)
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
    reduce_idx: PartId,
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

        part_id = constants.merge_part_ids(worker_id, reduce_idx)
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
            # TODO(@lsf) change this to yielding returns.
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

    def submit_merge_tasks(m, get_map_blocks):
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
            tasks = merge_tasks_in_flight.pop(0)  # This is O(N)
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
    submit_merge_tasks(num_rounds, lambda w: timeout_map_blocks[w])

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


class NodeScheduler:
    def __init__(self, cfg: AppConfig) -> None:
        self.num_workers = cfg.num_workers
        self.node_slots = np.zeros(self.num_workers, dtype=np.int8)
        self.node_usage_counter = np.zeros(self.num_workers, dtype=np.int_)
        self.tasks_in_flight = {}

    def get_node(self) -> int:
        ret = np.argmin(self.node_slots)
        self.node_slots[ret] += 1
        self.node_usage_counter[ret] += 1
        return ret

    def limit_concurrency(self, max_concurrency: int) -> None:
        if len(self.tasks_in_flight) >= max_concurrency * self.num_workers:
            self._wait_node()

    def _wait_node(self, num_returns: int = 1) -> None:
        ready, _ = ray.wait(
            list(self.tasks_in_flight.keys()),
            num_returns=num_returns,
            fetch_local=False,
        )
        for task in ready:
            node = self.tasks_in_flight.pop(task)
            self.node_slots[node] -= 1

    def register_task(self, task: ray.ObjectRef, node: int) -> None:
        self.tasks_in_flight[task] = node


@ray.remote(num_cpus=0, max_concurrency=2)
class ReduceController:
    def __init__(self, cfg: AppConfig, node_id: int, reduce_args: list) -> None:
        self.cfg = cfg
        self.node_id = node_id
        self.reduce_idx = 0
        self.reduce_args = reduce_args
        self.reduce_args_lock = threading.Lock()
        logging_utils.init()
        logging.info(
            "ReduceController %d started with %d tasks", node_id, len(reduce_args)
        )

    def run(self) -> list:
        tasks = []
        with tracing_utils.timeit("reduce_master"):
            while True:
                if self.reduce_idx >= self.cfg.reduce_parallelism:
                    ray_utils.wait(
                        tasks[: self.reduce_idx - self.cfg.reduce_parallelism + 1],
                        wait_all=True,
                    )
                with self.reduce_args_lock:
                    if self.reduce_idx >= len(self.reduce_args):
                        break
                    reduce_args = self.reduce_args[self.reduce_idx]
                tasks.append(
                    final_merge.options(
                        **ray_utils.node_i(self.cfg, self.node_id)
                    ).remote(self.cfg, self.node_id, self.reduce_idx, *reduce_args)
                )
                self.reduce_idx += 1
            logging.info("%d submitted %d reduce tasks", self.node_id, self.reduce_idx)
            return ray.get(tasks) + [None] * (
                self.cfg.num_reducers_per_worker - len(tasks)
            )

    def donate_task(self) -> tuple:
        """Donate a reduce task to another worker."""
        with self.reduce_args_lock:
            reduce_idx = len(self.reduce_args) - 1
            if self.reduce_idx >= reduce_idx:
                return (None, self.node_id, None)
            return self.reduce_args.pop(), self.node_id, reduce_idx


def sort_optimized_2(cfg: AppConfig, parts: list[PartInfo]) -> list[PartInfo]:
    assert cfg.merge_factor == 1, cfg
    # cfg.skip_sorting = True
    map_bounds, merge_bounds = get_boundaries(
        cfg.num_workers, cfg.num_reducers_per_worker
    )

    mapper_opt = {"num_returns": cfg.num_workers + 1}
    merger_opt = {"num_returns": cfg.num_reducers_per_worker + 1}

    num_shards = cfg.num_shards_per_mapper
    wait_batch = 1
    merge_round = 0
    merges_in_flight = {}
    node_to_merges = collections.defaultdict(set)
    all_merge_out = []
    map_scheduler = NodeScheduler(cfg)
    merge_stragglers_count = np.zeros(cfg.num_workers, dtype=np.int_)

    def _submit_map(map_id: int):
        pinfolist = parts[map_id * num_shards : (map_id + 1) * num_shards]
        node_id = map_scheduler.get_node()
        opt = dict(**mapper_opt, **ray_utils.node_i(cfg, node_id))
        refs = mapper_yield.options(**opt).remote(cfg, map_id, map_bounds, pinfolist)
        task = refs[cfg.num_workers]
        map_scheduler.register_task(task, node_id)
        return refs[: cfg.num_workers]

    def _submit_merge(w: int, map_out: np.ndarray):
        refs = merge_blocks_yield.options(
            **merger_opt, **ray_utils.node_i(cfg, w)
        ).remote(
            cfg,
            constants.merge_part_ids(w, merge_round),
            merge_bounds[w],
            map_out,
        )
        task = refs[0]
        merges_in_flight[task] = w
        node_to_merges[w].add(task)
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
                node_to_merges[w].remove(task)
                if tasks[w, 0] is not None:
                    continue
                tasks[w, :] = _submit_merge(w, all_map_out[:, w])
                num_submitted += 1
                if num_submitted >= cfg.shuffle_wait_percentile * cfg.num_workers:
                    # Give up waiting for the stragglers. Just schedule the remaining tasks.
                    not_ready_nodes = [
                        w for w in range(cfg.num_workers) if tasks[w, 0] is None
                    ]
                    for w in not_ready_nodes:
                        merge_stragglers_count[w] += 1
                    logging.info(
                        "Waited %.1f seconds; %d/%d merge tasks completed %s",
                        time.time() - start,
                        num_waited,
                        num_submitted,
                        merge_stragglers_count,
                    )
                    for w in not_ready_nodes:
                        if len(node_to_merges[w]) > (
                            cfg.merge_parallelism / cfg.shuffle_wait_percentile
                        ):
                            hard_wait_start = time.time()
                            ray.wait(
                                list(node_to_merges[w]),
                                fetch_local=False,
                            )
                            logging.info(
                                "Hard-waited %.1f seconds for %d to complete a merge",
                                time.time() - hard_wait_start,
                                w,
                            )
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

    # Main map-merge loop.
    all_map_out = []
    for map_id in range(cfg.num_mappers):
        map_scheduler.limit_concurrency(cfg.map_parallelism)
        all_map_out.append(_submit_map(map_id))
        if len(all_map_out) >= cfg.merge_factor * cfg.num_workers:
            _merge_map_out(all_map_out)
            all_map_out = []
    if all_map_out:
        _merge_map_out(all_map_out)

    # Reduce stage.
    # all_merge_out: (num_merge_rounds, num_workers, num_reducers_per_worker)
    # merge_results: (num_workers, num_reducers_per_worker, num_merge_rounds)
    merge_results = np.transpose(all_merge_out, axes=[1, 2, 0]).tolist()
    del all_merge_out

    reduce_controllers = [
        ReduceController.options(**ray_utils.node_i(cfg, w)).remote(
            cfg, w, merge_results[w]
        )
        for w in range(cfg.num_workers)
    ]
    del merge_results
    tasks_in_flight = {
        controller.run.remote(): (w, controller)
        for w, controller in enumerate(reduce_controllers)
    }
    controller_tasks = list(tasks_in_flight.keys())
    stolen_tasks = []
    stolen_tasks_info = []

    def _steal_task(physical_node_id: int):
        donating_controller = np.random.choice(reduce_controllers)
        reduce_args, node_id, reduce_idx = ray.get(
            donating_controller.donate_task.remote()
        )
        if reduce_args is None:
            return
        task = final_merge.options(**ray_utils.node_i(cfg, physical_node_id)).remote(
            cfg, node_id, reduce_idx, *reduce_args
        )
        # TODO(@lsf) not stealing aggressively enough.
        logging.info("%d steals task %d from %d", physical_node_id, reduce_idx, node_id)
        stolen_tasks.append(task)
        stolen_tasks_info.append((node_id, reduce_idx))
        tasks_in_flight[task] = (physical_node_id, None)

    while tasks_in_flight:
        ready, _ = ray.wait(list(tasks_in_flight.keys()))
        task = ready[0]
        physical_node_id, controller = tasks_in_flight.pop(task)
        if controller:
            reduce_controllers.remove(controller)
            if len(reduce_controllers) == 0:
                break
            for _ in range(cfg.reduce_parallelism):
                _steal_task(physical_node_id)
        else:
            _steal_task(physical_node_id)

    # Gather results.
    ret = ray.get(controller_tasks)
    stolen_task_results = ray.get(stolen_tasks)
    for (node_id, reduce_idx), result in zip(stolen_tasks_info, stolen_task_results):
        assert ret[node_id][reduce_idx] is None, (node_id, reduce_idx)
        ret[node_id][reduce_idx] = result
    logging.info("Mapper nodes usage: %s", map_scheduler.node_usage_counter)
    return flatten(flatten(ret))


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
