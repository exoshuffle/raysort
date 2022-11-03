import collections
import csv
import logging
from typing import Iterable, Optional

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


# TODO(@lsf): retries would require deduplication on the merge side.
@ray.remote(num_cpus=0, max_retries=0)
def mapper(
    cfg: AppConfig,
    _mapper_id: PartId,
    bounds: list[int],
    pinfolist: list[PartInfo],
    merge_controllers: list[ray.actor.ActorHandle],
) -> None:
    logging_utils.init()
    with tracing_utils.timeit("map"):
        part, blocks = mapper_sort_blocks(cfg, bounds, pinfolist)
    with tracing_utils.timeit("shuffle"):
        tasks = []
        for merger, (offset, size) in zip(merge_controllers, blocks):
            block = part[offset : offset + size]
            tasks.append(merger.add_block.remote([ray.put(block)]))
        ray.get(tasks)


def _merge_blocks(
    blocks: list[np.ndarray], bounds: list[int]
) -> Iterable[ray.ObjectRef]:
    total_bytes = sum(b.size for b in blocks)
    num_records = constants.bytes_to_records(total_bytes / len(bounds) * 2)
    get_block = lambda i, d: blocks[i] if d == 0 else None
    merger = sortlib.merge_partitions(
        len(blocks), get_block, num_records, False, bounds
    )
    for datachunk in merger:
        yield datachunk


@ray.remote(num_cpus=0)
class Merger:
    def __init__(self, cfg: AppConfig, bounds: list[int]):
        self.cfg = cfg
        self.bounds = bounds
        self._blocks = []

    def add_block(self, block: np.ndarray) -> None:
        # TODO: try copy vs no copy
        self._blocks.append(np.copy(block))

    def close(self) -> Iterable[ray.ObjectRef]:
        with tracing_utils.timeit("merge"):
            for datachunk in _merge_blocks(self._blocks, self.bounds):
                yield datachunk


@ray.remote(num_cpus=0)
class MergeController:
    def __init__(self, cfg: AppConfig, worker_id: PartId, bounds: list[int]):
        self.cfg = cfg
        self.bounds = bounds
        self.worker_id = worker_id
        self.merge_limit = cfg.merge_factor * cfg.num_workers
        self._merger: ray.actor.ActorHandle = None
        self._merger_num_blocks = 0
        self._merge_tasks_in_flight = []
        self._merge_results = []
        self._init_merger()
        logging_utils.init()

    def _init_merger(self):
        self._merger = Merger.options(**ray_utils.current_node_aff()).remote(
            self.cfg, self.bounds
        )
        self._merger_num_blocks = 0

    def _close_merger(self):
        refs = self._merger.close.options(
            num_returns=self.cfg.num_reducers_per_worker
        ).remote()
        self._merge_tasks_in_flight.append(refs[0])
        self._merge_results.append(refs)
        self._init_merger()

    def add_block(self, block_ref: list[ray.ObjectRef]) -> None:
        self._merger.add_block.remote(block_ref[0])
        self._merger_num_blocks += 1
        if self._merger_num_blocks >= self.merge_limit:
            if len(self._merge_tasks_in_flight) >= self.cfg.merge_parallelism:
                _, self._merge_tasks_in_flight = ray.wait(
                    self._merge_tasks_in_flight, fetch_local=False
                )
            self._close_merger()

    def finish_merge_results(self):
        logging.info("#%d waiting for merge tasks to finish", self.worker_id)
        if self._merger_num_blocks > 0:
            self._close_merger()
        ray_utils.wait(self._merge_tasks_in_flight, wait_all=True)
        return self._merge_results

    def reduce(self) -> list[PartInfo]:
        # transposed: (num_reducers_per_worker, num_merge_tasks)
        merge_out = np.transpose(self.finish_merge_results())
        self._merge_results = []
        results = []
        tasks_in_flight = []
        logging.info("#%d starting reduce tasks", self.worker_id)
        with tracing_utils.timeit("reduce_master"):
            for r in range(self.cfg.num_reducers_per_worker):
                if len(tasks_in_flight) >= self.cfg.reduce_parallelism:
                    _, tasks_in_flight = ray.wait(tasks_in_flight, fetch_local=False)
                ref = final_merge.options(**ray_utils.current_node_aff()).remote(
                    self.cfg, self.worker_id, r, *merge_out[r]
                )
                merge_out[r, :] = None
                tasks_in_flight.append(ref)
                results.append(ref)
        return ray.get(results)


# Memory usage: merge_partitions.batch_num_records * RECORD_SIZE = 100MB
# Plasma usage: input_part_size = 2GB
@ray.remote(num_cpus=0)
def final_merge(
    cfg: AppConfig,
    worker_id: PartId,
    reduce_idx: PartId,
    *parts: list[np.ndarray],
) -> PartInfo:
    with tracing_utils.timeit("reduce"):
        M = len(parts)
        get_block = lambda i, d: parts[i] if d == 0 else None
        part_id = constants.merge_part_ids(worker_id, reduce_idx)
        pinfo = sort_utils.part_info(
            cfg, part_id, kind="output", cloud=cfg.cloud_storage
        )
        merger = sortlib.merge_partitions(M, get_block)
        return sort_utils.save_partition(cfg, pinfo, merger)[0]


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


def sort_optimized(cfg: AppConfig, parts: list[PartInfo]) -> list[PartInfo]:
    assert cfg.merge_factor == 1, cfg
    map_bounds, merge_bounds = get_boundaries(
        cfg.num_workers, cfg.num_reducers_per_worker
    )
    num_shards = cfg.num_shards_per_mapper
    map_scheduler = NodeScheduler(cfg)
    merge_controllers = [
        MergeController.options(**ray_utils.node_i(cfg, w)).remote(
            cfg, w, merge_bounds[w]
        )
        for w in range(cfg.num_workers)
    ]

    def _submit_map(map_id: int) -> ray.ObjectRef:
        pinfolist = parts[map_id * num_shards : (map_id + 1) * num_shards]
        node_id = map_scheduler.get_node()
        opt = dict(**ray_utils.node_i(cfg, node_id))
        task = mapper.options(**opt).remote(
            cfg, map_id, map_bounds, pinfolist, merge_controllers
        )
        map_scheduler.register_task(task, node_id)
        return task

    # Main map-merge loop.
    all_map_out = []
    for map_id in range(cfg.num_mappers):
        map_scheduler.limit_concurrency(cfg.map_parallelism)
        all_map_out.append(_submit_map(map_id))

    # Wait for all map tasks to finish.
    logging.info("Waiting for %d map tasks to finish", len(all_map_out))
    ray_utils.wait(all_map_out, wait_all=True)
    logging.info("All map tasks finished; start reduce stage")

    ret = ray.get([controller.reduce.remote() for controller in merge_controllers])
    logging.info("Mapper nodes usage: %s", map_scheduler.node_usage_counter)
    return flatten(ret)


def sort_main(cfg: AppConfig):
    parts = sort_utils.load_manifest(cfg)
    results = sort_optimized(cfg, parts)

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
