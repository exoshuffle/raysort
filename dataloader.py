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
    part = sort_utils.load_partition(args, path)
#    assert part.size == args.input_part_size, (part.shape, path, args)
    load_duration = time.time() - start_time
    tracing_utils.record_value("map_load_time", load_duration)
    sort_fn = sortlib.sort_and_partition; 
    blocks = sort_fn(part, bounds)
    ret = [part[offset : offset + size] for offset, size in blocks]
    print("length of mapper return", len(ret))
    return ret if len(ret) > 1 else ret[0]

@ray.remote
class Reducer:
    def __init__(self):
        self.arrival_times = []

    @tracing_utils.timeit("reduce")
    def consume(self, map_result):
        t = time.time()
        self.arrival_times.append(t)
        print("consumed reduce partition", len(self.arrival_times), t)
        tracing_utils.record_value("reduce_timestamp", t) 

@ray.remote
def final_merge(args: Args,
    worker_id: PartId,
    reducer_id: PartId,
    *parts: List,
) -> PartInfo:
    M = len(parts)

    def get_block(i: int, d: int) -> np.ndarray:
        if i >= M or d > 0:
            return None
        part = parts[i]
        if part is None:
            return None
        if isinstance(part, np.ndarray):
            return None if part.size == 0 else part
        if isinstance(part, ray.ObjectRef):
            ret = ray.get(part)
            assert ret is None or isinstance(ret, np.ndarray), type(ret)
            return ret
        assert isinstance(part, PartInfo), part
        with open(part.path, "rb", buffering=args.io_size) as fin:
            ret = np.fromfile(fin, dtype=np.uint8)
        os.remove(part.path)
        return None if ret.size == 0 else ret

    part_id = constants.merge_part_ids(worker_id, reducer_id)
    pinfo = sort_utils.part_info(args, part_id, kind="output", s3=args.s3_bucket)
    merge_fn = _dummy_merge if args.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, get_block)
    sort_utils.save_partition(args, pinfo.path, merger)
    return pinfo

def sort(args: Args, streaming=True):
    parts = sort_utils.load_manifest(args)
    print("Number of partitions", len(parts))
    # May have to modify to schedule tasks in rounds for performance later on
    bounds, _ = sort_main.get_boundaries(args.num_reducers)
    mapper_opt = {"num_returns": args.num_reducers}
    all_map_out = np.empty((args.num_mappers, args.num_reducers), dtype=object)

    for part_id in range(args.num_mappers):
        pinfo = parts[part_id]
        opt = dict(**mapper_opt, **sort_main._get_node_res(args, pinfo, part_id))
        all_map_out[part_id, :] = mapper.options(**opt).remote(
                args, part_id, bounds, pinfo.path
        )

    reducers = [Reducer.remote() for _ in range(args.num_reducers)]
    map_out_to_id = {r[0]: i for i, r in enumerate(all_map_out)}

    print("Reducers:", len(reducers))
    print("Mappers:", len(all_map_out))
    
    map_out_remaining = list(map_out_to_id)
    futures = []
    if streaming:
        # Process one map block per loop iteration
        while len(map_out_remaining) > 0:
            ready, map_out_remaining = ray.wait(map_out_remaining)
            map_out = all_map_out[map_out_to_id[ready[0]]]
            for result, reducer in zip(map_out, reducers):
                f = reducer.consume.remote(result)
                futures.append(f)
    else:
        # For non-streaming case
        ray_utils.wait(all_map_out)
        for result_list in all_map_out:
            for result in result_list:
                reducer.consume.remote(result)

    # Finish the final merge (we're not timing this operation, it's just
    # to check for correctness at the end with sort_utils.validate_output)
    print("WAITING FOR MAP OUTPUT")
    ray.wait(futures)

    reduce_results = np.empty(
        (args.num_workers, args.num_reducers_per_worker), dtype=object
    )

    for r in range(args.num_reducers_per_worker):
        reduce_results[:, r] = [
                final_merge.options(
                **ray_utils.node_i(args, w, args.reduce_parallelism)
            ).remote(args, w, r, *all_map_out[:, w * args.num_reducers_per_worker + r])
            for w in range(args.num_workers)
        ]
    return ray.get(reduce_results.flatten().tolist())

    print("OK")


def main(args: Args):
    tracker = sort_main.init(args)
    try:
        sort_utils.generate_input(args)

        sort(args)
        print("VALIDATING OUTPUT")
        sort_utils.validate_output(args)
    finally:
        ray.get(tracker.performance_report.remote())
   
if __name__ == "__main__":
    main(app_args.get_args())    
