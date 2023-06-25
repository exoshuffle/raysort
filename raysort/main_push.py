import csv
import logging
import time

from collections import namedtuple
from typing import Iterable, Tuple, Union

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


MergeStats = namedtuple("MergeStats", "datachunk_sizes total_bytes")


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
def mapper(
    cfg: AppConfig,
    mapper_id: PartId,
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
            block = None if cfg.skip_sorting else part[offset : offset + size]
            task = merger.add_block.remote(
                mapper_id, size, [ray.put(block, _owner=merger)]
            )
            tasks.append(task)
        ray.get(tasks)


def _merge_blocks(
    blocks: list[np.ndarray], bounds: list[int]
) -> Iterable[Union[np.ndarray, Tuple, int]]:
    total_bytes = sum(b.size for b in blocks)
    num_records = max(1, constants.bytes_to_records(total_bytes))
    get_block = lambda i, d: blocks[i] if d == 0 else None
    merger = sortlib.merge_partitions(
        len(blocks), get_block, num_records, False, bounds
    )

    datachunk_sizes = []
    for datachunk in merger:
        yield datachunk
        datachunk_sizes.append(datachunk.size)

    yield MergeStats(ray.put(tuple(datachunk_sizes)), ray.put(total_bytes))


@ray.remote(num_cpus=0)
class Merger:
    def __init__(self, worker_id: int, bounds: list[int]):
        self.worker_id = worker_id
        self.bounds = bounds
        self._blocks = []
        logging_utils.init()

    def add_block(self, block: np.ndarray) -> None:
        self._blocks.append(block)

    def merge(self) -> Iterable[np.ndarray]:
        with tracing_utils.timeit("merge"):
            for datachunk in _merge_blocks(self._blocks, self.bounds):
                yield datachunk
        self.reset()

    def reset(self) -> None:
        self._blocks = []


@ray.remote(num_cpus=0)
class MergeController:
    def __init__(self, cfg: AppConfig, worker_id: PartId, bounds: list[int]):
        self.cfg = cfg
        self.bounds = bounds
        self.worker_id = worker_id
        self.merge_limit = cfg.merge_factor * cfg.num_workers
        self.merge_threshold_gb = cfg.merge_threshold_gb
        self._idle_mergers = [
            Merger.options(**ray_utils.current_node_aff()).remote(
                self.worker_id, self.bounds
            )
            for _ in range(self.cfg.merge_parallelism)
        ]
        self._merge_tasks = {}
        self._current_merger = self._get_merger()
        self._current_num_blocks = 0
        self._current_blocks_size_gb = 0
        self._merge_results = []
        self._merge_datachunk_sizes = []
        self._mapper_received = np.zeros(self.cfg.num_mappers, dtype=np.int_)
        self._merged_bytes = 0
        logging_utils.init()

    def _get_merger(self) -> ray.actor.ActorHandle:
        if self._idle_mergers:
            return self._idle_mergers.pop()
        start = time.perf_counter()
        ready, _ = ray.wait(list(self._merge_tasks.keys()))
        for task in ready:
            self._merged_bytes += ray.get(task)
            merger = self._merge_tasks.pop(task)
            duration = time.perf_counter() - start
            if duration >= 1:
                logging.info(
                    "#%d waited %.1f seconds for a merger to finish",
                    self.worker_id,
                    duration,
                )
            return merger

    def add_block(
        self, mapper_id: int, size: int, block_ref: list[ray.ObjectRef]
    ) -> None:
        if self._mapper_received[mapper_id] > 0:
            logging.info(
                "#%d discarded duplicate block from mapper %d",
                self.worker_id,
                mapper_id,
            )
            return
        self._mapper_received[mapper_id] = size
        self._current_merger.add_block.remote(block_ref[0])
        self._current_num_blocks += 1
        self._current_blocks_size_gb += size / 1e9
        if (
            self._current_num_blocks >= self.merge_limit
            or self._current_blocks_size_gb >= self.merge_threshold_gb
        ):
            self._close_current_merger()

    def _close_current_merger(self):
        refs = self._current_merger.merge.options(
            num_returns=self.cfg.num_reducers_per_worker + 1
        ).remote()
        merge_stats = ray.get(refs[-1])
        self._merge_tasks[merge_stats.total_bytes] = self._current_merger
        self._merge_results.append(refs[:-1])
        self._merge_datachunk_sizes.append(ray.get(merge_stats.datachunk_sizes))
        self._current_merger = self._get_merger()
        self._current_num_blocks = 0
        self._current_blocks_size_gb = 0

    def _finish_merge_results(self) -> list[list[ray.ObjectRef]]:
        if self._current_num_blocks > 0:
            self._close_current_merger()
        start = time.perf_counter()
        self._merged_bytes += sum(ray.get(list(self._merge_tasks.keys())))
        assert self._merged_bytes == self._mapper_received.sum(), (
            self._merged_bytes,
            self._mapper_received.sum(),
        )
        logging.info(
            "#%d waited %.1f seconds for all merges to finish; total bytes merged: %d",
            self.worker_id,
            time.perf_counter() - start,
            self._merged_bytes,
        )
        self._idle_mergers = []
        self._merge_tasks = {}
        self._current_merger = None
        ret = self._merge_results
        ret2 = self._merge_datachunk_sizes
        self._merge_results = []
        return ret, ret2

    def reduce(self) -> list[PartInfo]:
        # merge_results: (num_reducers_per_worker, num_merge_tasks)
        data_mat, size_mat = self._finish_merge_results()
        merge_results = np.transpose(data_mat)
        part_sizes = np.transpose(size_mat)

        with tracing_utils.timeit("reduce_master"):
            results = []
            tasks_in_flight = []
            for r, merge_out in enumerate(merge_results):
                if len(tasks_in_flight) >= self.cfg.reduce_parallelism:
                    _, tasks_in_flight = ray.wait(tasks_in_flight, fetch_local=False)

                ref = final_merge.options(**ray_utils.current_node_aff()).remote(
                    self.cfg, self.worker_id, r, list(merge_out), sum(part_sizes[r])
                )
                merge_results[r, :] = None
                if isinstance(ref, list):
                    tasks_in_flight.extend(ref)
                    results.extend(ref)
                else:
                    tasks_in_flight.append(ref)
                    results.append(ref)
            return [r for r in ray.get(results) if r is not None]


# Memory usage: merge_partitions.batch_num_records * RECORD_SIZE = 100MB
# Plasma usage: input_part_size = 2GB
@ray.remote(num_cpus=0)
def final_merge(
    cfg: AppConfig,
    worker_id: PartId,
    reduce_idx: PartId,
    parts: list[np.ndarray],
    parts_size_bytes: int,
) -> list[PartInfo]:
    logging_utils.init()

    with tracing_utils.timeit("reduce"):
        M = len(parts)
        if M == 0:
            return []
        parts_memory_gb = parts_size_bytes / 1e9

        if M > 1 and parts_memory_gb > cfg.streaming_reducer_threshold_gb:
            part_chunks = ray.get(
                [
                    sort_utils.make_chunks.options(
                        **ray_utils.current_node_aff()
                    ).remote(p)
                    for p in parts
                ]
            )

            def get_block(i, d):
                if i >= len(part_chunks):
                    return None
                if d >= len(part_chunks[i]):
                    return None
                return ray.get(part_chunks[i][d])

            ask_for_refills = True
        else:
            parts_get = [p for p in ray.get(parts) if len(p) > 0]
            get_block = lambda i, d: parts_get[i] if d == 0 else None
            ask_for_refills = False

        part_id = constants.merge_part_ids(worker_id, reduce_idx)
        pinfo = sort_utils.part_info(
            cfg, part_id, kind="output", cloud=cfg.cloud_storage
        )
        merger = sortlib.merge_partitions(M, get_block, ask_for_refills=ask_for_refills)
        output = sort_utils.save_partition(cfg, pinfo, merger)
        return output


class NodeScheduler:
    def __init__(self, cfg: AppConfig) -> None:
        self.num_workers = cfg.num_workers
        self.node_slots = np.zeros(self.num_workers, dtype=np.int_)
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
        ready, _ = ray_utils.wait(self.tasks_in_flight.keys(), num_returns=num_returns)
        for task in ready:
            node = self.tasks_in_flight.pop(task)
            self.node_slots[node] -= 1

    def register_task(self, task: ray.ObjectRef, node: int) -> None:
        self.tasks_in_flight[task] = node


def sort_optimized(cfg: AppConfig, parts: list[PartInfo]) -> list[PartInfo]:
    map_bounds, merge_bounds = sort_utils.get_boundaries_auto(cfg, parts)
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
    logging.info("Mapper nodes usage: %s", map_scheduler.node_usage_counter)
    logging.info("Waiting for %d map tasks to finish", len(all_map_out))
    ray.get(all_map_out)
    logging.info("All map tasks finished; start reduce stage")

    # Reduce stage.
    ret = ray.get([controller.reduce.remote() for controller in merge_controllers])
    return flatten(ret)


def sort_main(cfg: AppConfig):
    parts = sort_utils.load_manifest(cfg)
    np.random.shuffle(parts)
    results = sort_optimized(cfg, parts)

    with open(sort_utils.get_manifest_file(cfg, kind="output"), "w") as fout:
        writer = csv.writer(fout)
        for pinfo_lst in results:
            for pinfo in pinfo_lst:
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
