import time
from typing import List

import numpy as np
import ray

from raysort import config
from raysort import main as sort_main
from raysort import ray_utils, sort_utils, sortlib, tracing_utils
from raysort.config import AppConfig
from raysort.typing import PartId, PartInfo


@ray.remote
@tracing_utils.timeit("map")
def mapper(
    cfg: AppConfig,
    _mapper_id: PartId,
    bounds: List[int],
    pinfo: PartInfo,
) -> List[np.ndarray]:
    start_time = time.time()
    tracing_utils.record_value("map_arrive", start_time)
    part = sort_utils.load_partition(cfg, pinfo)
    load_duration = time.time() - start_time
    tracing_utils.record_value("map_load_time", load_duration)
    sort_fn = sortlib.sort_and_partition
    blocks = sort_fn(part, bounds)
    ret = [part[offset : offset + size] for offset, size in blocks]
    return ret if len(ret) > 1 else ret[0]


@ray.remote(num_cpus=0)
class Reducer:
    def __init__(self):
        self.arrival_times = []

    @tracing_utils.timeit("reduce")
    def consume(self, _map_result):
        # For fine-grained pipelining
        t = time.time()
        self.arrival_times.append(t)
        tracing_utils.record_value("reduce_arrive", t)

    @tracing_utils.timeit("reduce")
    def consume_and_shuffle(self, *map_results):  # pylint: disable=no-self-use
        # Intra-partition shuffle
        catted = np.concatenate(map_results)
        reshaped = catted.reshape((-1, 100))  # 100 is the number of bytes in a record
        np.random.shuffle(reshaped)


@tracing_utils.timeit("sort")
def sort(cfg: AppConfig):
    # Traditional unpipelined sort

    parts = sort_utils.load_manifest(cfg)
    print("Number of partitions", len(parts))
    bounds, _ = sort_main.get_boundaries(cfg.num_reducers)
    mapper_opt = {"num_returns": cfg.num_reducers}

    map_round = 20
    map_scheduled = 0
    all_map_out = np.empty((cfg.num_mappers, cfg.num_reducers), dtype=object)
    while map_scheduled < cfg.num_mappers:
        last_map = min(cfg.num_mappers, map_round + map_scheduled)
        for part_id in range(map_scheduled, last_map):
            pinfo = parts[part_id]
            opt = dict(**mapper_opt, **sort_main.get_node_aff(cfg, pinfo, part_id))
            all_map_out[part_id, :] = mapper.options(**opt).remote(
                cfg, part_id, bounds, pinfo
            )
            if part_id > 0 and part_id % map_round == 0:
                # Wait for at least one map task from this round to finish before
                # scheduling the next round.
                ray_utils.wait(all_map_out[:, 0], num_returns=part_id - map_round + 1)
        map_scheduled += map_round

    reducers = [
        Reducer.options(**ray_utils.node_i(cfg, r % cfg.num_workers)).remote()
        for r in range(cfg.num_reducers)
    ]

    futures = []
    reduce_round = 10
    reduce_scheduled = 0
    while reduce_scheduled < cfg.num_reducers:
        last_reduce = min(cfg.num_reducers, reduce_round + reduce_scheduled)
        for r in range(reduce_scheduled, last_reduce):
            f = reducers[r].consume_and_shuffle.remote(*all_map_out[:, r])
            futures.append(f)
            if r > 0 and r % reduce_round == 0:
                # Let at least one reduce task from this round complete
                # before scheduling next round
                ray.wait(futures, num_returns=r - reduce_round + 1)
        reduce_scheduled += reduce_round
    ray_utils.wait(futures, wait_all=True)
    print("OK")


@tracing_utils.timeit("sort")
def sort_partial_streaming(cfg: AppConfig):
    # Medium-grained pipelining: shuffle within small batches

    parts = sort_utils.load_manifest(cfg)
    print("Number of partitions", len(parts))
    # May have to modify to schedule tasks in rounds for performance later on
    bounds, _ = sort_main.get_boundaries(cfg.num_reducers)
    mapper_opt = {"num_returns": cfg.num_reducers}

    map_round = 80
    map_scheduled = 0
    all_map_out = np.empty((cfg.num_mappers, cfg.num_reducers), dtype=object)
    reducers = [
        Reducer.options(**ray_utils.node_i(cfg, r % cfg.num_workers)).remote()
        for r in range(cfg.num_reducers)
    ]
    while map_scheduled < cfg.num_mappers:
        last_map = min(cfg.num_mappers, map_round + map_scheduled)
        for part_id in range(map_scheduled, last_map):
            pinfo = parts[part_id]
            opt = dict(**mapper_opt, **sort_main.get_node_aff(cfg, pinfo, part_id))
            all_map_out[part_id, :] = mapper.options(**opt).remote(
                cfg, part_id, bounds, pinfo
            )
            if part_id > 0 and part_id % map_round == 0:
                # Wait for at least one map task from this round to finish before
                # scheduling the next round.
                ray_utils.wait(all_map_out[:, 0], num_returns=part_id - map_round + 1)

        futures = []
        reduce_round = 20
        reduce_scheduled = 0
        while reduce_scheduled < cfg.num_reducers:
            last_reduce = min(cfg.num_reducers, reduce_round + reduce_scheduled)
            for r in range(reduce_scheduled, last_reduce):
                f = reducers[r].consume_and_shuffle.remote(
                    *all_map_out[map_scheduled:last_map, r]
                )
                futures.append(f)
                if r > 0 and r % reduce_round == 0:
                    # Let at least one reduce task from this round complete
                    # before scheduling next round
                    ray.wait(futures, num_returns=r - reduce_round + 1)
            reduce_scheduled += reduce_round
        map_scheduled += map_round
    ray_utils.wait(futures, wait_all=True)
    print("OK")


@tracing_utils.timeit("sort")
def sort_streaming(cfg: AppConfig):
    # Fine-grained application pipelining: no intra-partition shuffle

    parts = sort_utils.load_manifest(cfg)
    # May have to modify to schedule tasks in rounds for performance later on
    bounds, _ = sort_main.get_boundaries(cfg.num_reducers)
    mapper_opt = {"num_returns": cfg.num_reducers}

    map_round = 40
    map_scheduled = 0
    reducers = [
        Reducer.options(**ray_utils.node_i(cfg, r % cfg.num_workers)).remote()
        for r in range(cfg.num_reducers)
    ]
    reduce_prev = []
    while map_scheduled < cfg.num_mappers:
        last_map = min(cfg.num_mappers, map_round + map_scheduled)
        all_map_out = np.empty(
            (last_map - map_scheduled, cfg.num_reducers), dtype=object
        )
        for part_id in range(map_scheduled, last_map):
            pinfo = parts[part_id]
            opt = dict(**mapper_opt, **sort_main.get_node_aff(cfg, pinfo, part_id))
            all_map_out[part_id - map_scheduled, :] = mapper.options(**opt).remote(
                cfg, part_id, bounds, pinfo
            )
        if len(reduce_prev) > 0:
            # Wait for at least one reduce task from the last round to finish before
            # scheduling the next round.
            ray.wait(reduce_prev)

        map_out_to_id = {r[0]: i for i, r in enumerate(all_map_out)}
        map_out_remaining = list(map_out_to_id)
        futures = []
        # Process one map block per loop iteration
        while len(map_out_remaining) > 0:
            ready, map_out_remaining = ray.wait(map_out_remaining)
            map_out = all_map_out[map_out_to_id[ready[0]]]
            for result, reducer in zip(map_out, reducers):
                f = reducer.consume.remote(result)
                futures.append(f)
        reduce_prev = futures
        map_scheduled += map_round
    ray_utils.wait(futures, wait_all=True)
    print("OK")


def main():
    job_cfg = config.get()
    tracker = sort_main.init(job_cfg)
    cfg = job_cfg.app
    try:
        sort_utils.generate_input(cfg)
        if cfg.dataloader_mode:
            if cfg.dataloader_mode == "streaming":
                sort_streaming(cfg)
            elif cfg.dataloader_mode == "partial":
                sort_partial_streaming(cfg)
        else:
            sort(cfg)
    finally:
        ray.get(tracker.performance_report.remote())


if __name__ == "__main__":
    main()
