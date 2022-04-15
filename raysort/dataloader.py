import collections
import ray
import time
import numpy as np
from typing import Callable, Dict, Iterable, List, Tuple, Union
from raysort import app_args
from raysort import constants
from raysort import logging_utils
from raysort import ray_utils
from raysort import sortlib
from raysort import sort_utils
from raysort import tracing_utils
from raysort import main as sort_main
from raysort.typing import Args, BlockInfo, PartId, PartInfo, Path

@ray.remote
@tracing_utils.timeit("map")
def mapper(
    args: Args,
    mapper_id: PartId,
    bounds: List[int],
    path: Path,
) -> List[np.ndarray]:
    start_time = time.time()
    tracing_utils.record_value("map_arrive", start_time)
    part = sort_utils.load_partition(args, path)
#    assert part.size == args.input_part_size, (part.shape, path, args)
    load_duration = time.time() - start_time
    tracing_utils.record_value("map_load_time", load_duration)
    sort_fn = sortlib.sort_and_partition; 
    blocks = sort_fn(part, bounds)
    ret = [part[offset : offset + size] for offset, size in blocks]
    print("length of mapper return", len(ret))
    return ret if len(ret) > 1 else ret[0]

@ray.remote(num_cpus=0)
class Reducer:
    def __init__(self):
        self.arrival_times = []

    @tracing_utils.timeit("reduce")
    def consume(self, map_result):
        t = time.time()
        self.arrival_times.append(t)
#        print("consumed reduce partition", len(self.arrival_times), t)
        tracing_utils.record_value("reduce_arrive", t) 

    @tracing_utils.timeit("reduce")
    def consume_and_shuffle(self, *map_results):
#        print("MAP_RESULTS TYPE", type(map_results))
#        print(len(map_results))
        np.random.shuffle(sort_utils.create_partition_records(len(map_results)*100))
#        np.random.shuffle(np.concatenate([arr.astype(sortlib.RecordT) for arr in map_results]))

@tracing_utils.timeit("sort")
def sort(args: Args):
    parts = sort_utils.load_manifest(args)
    print("Number of partitions", len(parts))
    # May have to modify to schedule tasks in rounds for performance later on
    bounds, _ = sort_main.get_boundaries(args.num_reducers)
    mapper_opt = {"num_returns": args.num_reducers}

    map_round = 20
    map_scheduled = 0
    all_map_out = np.empty((args.num_mappers, args.num_reducers), dtype=object)
    while map_scheduled < args.num_mappers:
        last_map = min(args.num_mappers, map_round + map_scheduled)
#        all_map_out = np.empty((last_map - map_scheduled, args.num_reducers), dtype=object)
        for part_id in range(map_scheduled, last_map):
            pinfo = parts[part_id]
            opt = dict(**mapper_opt, **sort_main._get_node_res(args, pinfo, part_id))
            all_map_out[part_id, :] = mapper.options(**opt).remote(
                    args, part_id, bounds, pinfo.path
            )
            if part_id > 0 and part_id % map_round == 0:
                # Wait for at least one map task from this round to finish before
                # scheduling the next round.
                ray_utils.wait(
                    all_map_out[:, 0], num_returns=part_id - map_round + 1
                )
        map_scheduled += map_round
        
    reducers = [Reducer.options(**ray_utils.node_i(args, r % args.num_workers)).remote() for r in range(args.num_reducers)]
    
    print("Ray resources:", ray.available_resources())

    futures = []
    reduce_round = 20
    reduce_scheduled = 0
    while reduce_scheduled < args.num_reducers:
        last_reduce = min(args.num_reducers, reduce_round + reduce_scheduled)
        for r in range(reduce_scheduled, last_reduce):
            f = reducers[r].consume_and_shuffle.remote(*all_map_out[:, r])
            futures.append(f)
            if r > 0 and r % reduce_round == 0:
                # Let at least one reduce task from this round complete
                # before scheduling next round
                print("Waiting for a reduce task to complete")
                ray.wait(futures, num_returns=r - reduce_round + 1)
        reduce_scheduled += reduce_round
    ray_utils.wait(futures, wait_all=True)
    print("OK")

@tracing_utils.timeit("sort")
def sort_partial_streaming(args: Args):
    parts = sort_utils.load_manifest(args)
    print("Number of partitions", len(parts))
    # May have to modify to schedule tasks in rounds for performance later on
    bounds, _ = sort_main.get_boundaries(args.num_reducers)
    mapper_opt = {"num_returns": args.num_reducers}

    map_round = 40
    map_scheduled = 0
    all_map_out = np.empty((args.num_mappers, args.num_reducers), dtype=object)
    reducers = [Reducer.options(**ray_utils.node_i(args, r % args.num_workers)).remote() for r in range(args.num_reducers)]
    while map_scheduled < args.num_mappers:
        last_map = min(args.num_mappers, map_round + map_scheduled)
        for part_id in range(map_scheduled, last_map):
            pinfo = parts[part_id]
            opt = dict(**mapper_opt, **sort_main._get_node_res(args, pinfo, part_id))
            all_map_out[part_id, :] = mapper.options(**opt).remote(
                    args, part_id, bounds, pinfo.path
            )
            if part_id > 0 and part_id % map_round == 0:
                # Wait for at least one map task from this round to finish before
                # scheduling the next round.
                ray_utils.wait(
                    all_map_out[:, 0], num_returns=part_id - map_round + 1
                )
        
        print("Ray resources:", ray.available_resources())

        futures = []
        reduce_round = 20
        reduce_scheduled = 0
        while reduce_scheduled < args.num_reducers:
            last_reduce = min(args.num_reducers, reduce_round + reduce_scheduled)
            for r in range(reduce_scheduled, last_reduce):
                f = reducers[r].consume_and_shuffle.remote(*all_map_out[map_scheduled:last_map, r])
                futures.append(f)
                if r > 0 and r % reduce_round == 0:
                    # Let at least one reduce task from this round complete
                    # before scheduling next round
                    print("Waiting for a reduce task to complete")
                    ray.wait(futures, num_returns=r - reduce_round + 1)
            reduce_scheduled += reduce_round
        map_scheduled += map_round
    ray_utils.wait(futures, wait_all=True)
    print("OK")

@tracing_utils.timeit("sort")
def sort_streaming(args: Args):
    parts = sort_utils.load_manifest(args)
    print("Number of partitions", len(parts))
    # May have to modify to schedule tasks in rounds for performance later on
    bounds, _ = sort_main.get_boundaries(args.num_reducers)
    mapper_opt = {"num_returns": args.num_reducers}

    map_round = 40
    map_scheduled = 0
    reducers = [Reducer.options(**ray_utils.node_i(args, r % args.num_workers)).remote() for r in range(args.num_reducers)]
    print("Reduce options:", ray_utils.node_i(args, 0))
    print("Ray resources:", ray.available_resources())
    reduce_prev = []
    while map_scheduled < args.num_mappers:
        last_map = min(args.num_mappers, map_round + map_scheduled)
        all_map_out = np.empty((last_map - map_scheduled, args.num_reducers), dtype=object)
        for part_id in range(map_scheduled, last_map):
            pinfo = parts[part_id]
            opt = dict(**mapper_opt, **sort_main._get_node_res(args, pinfo, part_id))
            all_map_out[part_id - map_scheduled, :] = mapper.options(**opt).remote(
                    args, part_id, bounds, pinfo.path
            )
        if (len(reduce_prev) > 0):
            # Wait for at least one reduce task from the last round to finish before
            # scheduling the next round.
            ray.wait(reduce_prev)

        map_out_to_id = {r[0]: i for i, r in enumerate(all_map_out)}

        print("Reducers:", len(reducers))
        print("Mappers:", len(map_out_to_id))
        print("Ray resources:", ray.available_resources())

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


def main(args: Args):
    tracker = sort_main.init(args)
    try:
        sort_utils.generate_input(args)
        if (args.dataloader_mode):
            if args.dataloader_mode == "streaming":
                sort_streaming(args)
            elif args.dataloader_mode == "partial":
                sort_partial_streaming(args)
        else:
            sort(args)
    finally:
        ray.get(tracker.performance_report.remote())
   
if __name__ == "__main__":
    main(app_args.get_args())    
