import argparse
import csv
import os
import random
import time
from typing import Callable, Dict, Iterable, List, Tuple, Union

import numpy as np
import ray

from raysort import constants
from raysort import logging_utils
from raysort import ray_utils
from raysort import sortlib
from raysort import sort_utils
from raysort import tracing_utils
from raysort.types import BlockInfo, ByteCount, PartId, PartInfo, Path

Args = argparse.Namespace

# ------------------------------------------------------------
#     Parse Arguments
# ------------------------------------------------------------

STEPS = ["generate_input", "sort", "validate_output"]

cluster = None


def get_args(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ray_address",
        default="auto",
        type=str,
        help="if set to None, will launch a local Ray cluster",
    )
    parser.add_argument(
        "--total_gb",
        default=1,
        type=float,
        help="total data size in GB (10^9 bytes)",
    )
    parser.add_argument(
        "--input_part_size",
        default=2000 * 1000 * 1000,
        type=ByteCount,
        help="size in bytes of each map partition",
    )
    parser.add_argument(
        "--num_concurrent_rounds",
        default=2,
        type=int,
        help="how many rounds of tasks to run concurrently (1 or 2)",
    )
    parser.add_argument(
        "--map_parallelism",
        default=2,
        type=int,
        help="each round has `map_parallelism` map tasks per node",
    )
    parser.add_argument(
        "--merge_factor",
        default=2,
        type=int,
        help="each round has `map_parallelism / merge_factor` per node",
    )
    parser.add_argument(
        "--reduce_parallelism",
        default=4,
        type=int,
        help="number of reduce tasks to run in parallel per node",
    )
    parser.add_argument(
        "--io_size",
        default=256 * 1024,
        type=ByteCount,
        help="disk I/O buffer size",
    )
    parser.add_argument(
        "--skip_sorting",
        default=False,
        action="store_true",
        help="if set, no sorting is actually performed",
    )
    parser.add_argument(
        "--skip_input",
        default=False,
        action="store_true",
        help="if set, mappers will not read data from disk",
    )
    parser.add_argument(
        "--skip_output",
        default=False,
        action="store_true",
        help="if set, reducers will not write out results to disk",
    )
    parser.add_argument(
        "--skip_final_merge",
        default=False,
        action="store_true",
        help="if set, will skip the second stage reduce tasks",
    )
    parser.add_argument(
        "--output_consume_time",
        default=0,
        type=float,
        help="output will be consumed instead of being written to disk",
    )
    parser.add_argument(
        "--use_object_store",
        default=False,
        action="store_true",
        help="if set, will use object store for 2nd-stage reduce",
    )
    parser.add_argument(
        "--use_put",
        default=False,
        action="store_true",
        help="if set, will return ray.put() references instead of objects directly",
    )
    parser.add_argument(
        "--simple_shuffle",
        default=False,
        action="store_true",
        help="if set, will use the simple map-reduce version",
    )
    parser.add_argument(
        "--repeat_sort",
        default=1,
        type=int,
        help="how many times to run the sort for benchmarking",
    )
    parser.add_argument(
        "--test_failure",
        default=False,
        action="store_true",
        help="if set, will simulate node failure mid-execution",
    )
    # Which steps to run?
    steps_grp = parser.add_argument_group(
        "steps to run", "if none is specified, will run all steps"
    )
    for step in STEPS:
        steps_grp.add_argument(f"--{step}", action="store_true")
    return parser.parse_args(*args, **kwargs)


def kill_and_restart_node():
    if cluster is not None:
        worker_node = list(cluster.worker_nodes)[0]
        resource_spec = worker_node.get_resource_spec()
        print("Killing worker node", worker_node, resource_spec)
        cluster.remove_node(worker_node)
        cluster.add_node(
            resources=resource_spec.resources,
            object_store_memory=resource_spec.object_store_memory,
            num_cpus=resource_spec.num_cpus,
        )


# ------------------------------------------------------------
#     Sort
# ------------------------------------------------------------


def _load_partition(args: Args, path: Path) -> np.ndarray:
    if args.skip_input:
        return sort_utils.generate_partition(args.input_part_size)
    return np.fromfile(path, dtype=np.uint8)


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
@ray.remote
@tracing_utils.timeit("map")
def mapper(
    args: Args,
    mapper_id: PartId,
    bounds: List[int],
    path: Path,
) -> List[np.ndarray]:
    start_time = time.time()
    part = _load_partition(args, path)
    load_duration = time.time() - start_time
    tracing_utils.record_value("map_disk_time", load_duration)
    sort_fn = (
        _dummy_sort_and_partition if args.skip_sorting else sortlib.sort_and_partition
    )
    blocks = sort_fn(part, bounds)
    if args.use_put:
        ret = [ray.put(part[offset : offset + size]) for offset, size in blocks]
    else:
        ret = [part[offset : offset + size] for offset, size in blocks]
    return ret if len(ret) > 1 else ret[0]


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


# Memory usage: input_part_size * merge_factor / (N/W) * 2 = 320MB
# Plasma usage: input_part_size * merge_factor * 2 = 8GB
@ray.remote
@tracing_utils.timeit("merge")
def merge_mapper_blocks(
    args: Args,
    worker_id: PartId,
    merge_id: PartId,
    bounds: List[int],
    *blocks: List[np.ndarray],
) -> Union[List[PartInfo], List[np.ndarray]]:
    if isinstance(blocks[0], ray.ObjectRef):
        blocks = ray.get(list(blocks))

    M = len(blocks)
    total_bytes = sum(b.size for b in blocks)
    num_records = int(total_bytes / len(bounds) * 2 // constants.RECORD_SIZE)

    def get_block(i, d):
        if i >= M or d > 0:
            return None
        return blocks[i]

    merge_fn = _dummy_merge if args.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, get_block, num_records, False, bounds)

    ret = []
    for i, datachunk in enumerate(merger):
        if args.use_object_store:
            if args.use_put:
                ret.append(ray.put(datachunk))
            else:
                ret.append(np.copy(datachunk))
            # TODO(@lsf): this function is using more memory in this branch
            # than in the else branch. `del datachunk` helped a little but
            # memory usage is still high. This does not make sense since
            # ray.put() should only use shared memory. Need to investigate.
            del datachunk
        else:
            part_id = constants.merge_part_ids(worker_id, merge_id, i)
            pinfo = sort_utils.part_info(args, part_id, kind="temp")
            with open(pinfo.path, "wb", buffering=args.io_size) as fout:
                datachunk.tofile(fout)
            ret.append(pinfo)
    assert len(ret) == len(bounds), (ret, bounds)
    del merger
    del blocks
    return ret


# Memory usage: merge_partitions.batch_num_records * RECORD_SIZE = 1GB
# Plasma usage: input_part_size = 2GB
@ray.remote
@tracing_utils.timeit("reduce")
def final_merge(
    args: Args,
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
    pinfo = sort_utils.part_info(args, part_id, kind="output")
    merge_fn = _dummy_merge if args.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, get_block)
    if args.skip_output or args.output_consume_time > 0:
        first = True
        for datachunk in merger:
            del datachunk
            if first:
                first = False
                if args.output_consume_time > 0:
                    tracing_utils.record_value("output_time", time.time())
        time.sleep(args.output_consume_time)
        return pinfo

    with open(pinfo.path, "wb", buffering=args.io_size) as fout:
        for datachunk in merger:
            fout.write(datachunk)
    return pinfo


def _node_res(node_ip: str, parallelism: int = 1000) -> Dict[str, float]:
    return {"resources": {f"node:{node_ip}": 1 / parallelism}}


def _node_i(args: Args, node_i: int, parallelism: int = 1000) -> Dict[str, float]:
    return _node_res(args.worker_ips[node_i], parallelism)


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


def sort_simple(args: Args, parts: List[PartInfo]) -> List[PartInfo]:
    bounds, _ = get_boundaries(args.num_reducers)

    mapper_opt = {"num_returns": args.num_reducers}
    map_results = np.empty((args.num_mappers, args.num_reducers), dtype=object)
    num_map_tasks_per_round = args.num_workers * args.map_parallelism

    for part_id in range(args.num_mappers):
        _, node, path = parts[part_id]
        opt = dict(**mapper_opt, **_node_res(node))
        map_results[part_id, :] = mapper.options(**opt).remote(
            args, part_id, bounds, path
        )
        # TODO: try memory-aware scheduling
        if part_id > 0 and part_id % num_map_tasks_per_round == 0:
            # Wait for at least one map task from this round to finish before
            # scheduling the next round.
            ray_utils.wait(
                map_results[:, 0], num_returns=part_id - num_map_tasks_per_round + 1
            )

    if args.test_failure:
        kill_and_restart_node()

    if args.skip_final_merge:
        ray_utils.wait(map_results[:, 0], wait_all=True)
        return []

    reducer_results = []
    for w in range(args.num_workers):
        for r in range(args.num_reducers_per_worker):
            reducer_results.append(
                final_merge.options(**_node_i(args, w, args.reduce_parallelism)).remote(
                    args,
                    w,
                    r,
                    *map_results[:, w * args.num_reducers_per_worker + r],
                )
            )

    return ray.get(reducer_results)


def sort_two_stage(args: Args, parts: List[PartInfo]) -> List[PartInfo]:
    map_bounds, merge_bounds = get_boundaries(
        args.num_workers, args.num_reducers_per_worker
    )

    mapper_opt = {"num_returns": args.num_workers}
    merger_opt = {"num_returns": args.num_reducers_per_worker}
    merge_results = np.empty(
        (args.num_workers, args.num_mergers_per_worker, args.num_reducers_per_worker),
        dtype=object,
    )
    num_map_tasks_per_round = args.num_workers * args.map_parallelism

    part_id = 0
    for round in range(args.num_rounds):
        # Submit map tasks.
        num_map_tasks = min(num_map_tasks_per_round, args.num_mappers - part_id)
        map_results = np.empty((num_map_tasks, args.num_workers), dtype=object)
        for _ in range(num_map_tasks):
            _, node, path = parts[part_id]
            opt = dict(**mapper_opt, **_node_res(node))
            m = part_id % num_map_tasks_per_round
            map_results[m, :] = mapper.options(**opt).remote(
                args, part_id, map_bounds, path
            )
            part_id += 1

        # Make sure all merge tasks previous rounds finish.
        num_extra_rounds = round - args.num_concurrent_rounds + 1
        if num_extra_rounds > 0:
            ray_utils.wait(
                merge_results[:, :, 0].flatten(),
                num_returns=num_extra_rounds
                * args.merge_parallelism
                * args.num_workers,
            )

        # Submit merge tasks.
        f = int(np.ceil(num_map_tasks / args.merge_parallelism))
        for j in range(args.merge_parallelism):
            m = round * args.merge_parallelism + j
            for w in range(args.num_workers):
                map_blocks = map_results[j * f : (j + 1) * f, w]
                merge_results[w, m, :] = merge_mapper_blocks.options(
                    **merger_opt, **_node_i(args, w)
                ).remote(args, w, m, merge_bounds[w], *map_blocks)

        # Wait for at least one map task from this round to finish before
        # scheduling the next round.
        ray_utils.wait(map_results[:, 0])
        del map_results

    if args.test_failure:
        kill_and_restart_node()

    if args.skip_final_merge:
        ray_utils.wait(merge_results.flatten(), wait_all=True)
        return []

    # Submit second-stage reduce tasks.
    reduce_results = np.empty(
        (args.num_workers, args.num_reducers_per_worker), dtype=object
    )
    for r in range(args.num_reducers_per_worker):
        # This guarantees at most ((args.reduce_parallelism + 1) * args.num_workers)
        # tasks are queued.
        if r > args.reduce_parallelism:
            ray_utils.wait(
                reduce_results.flatten(),
                num_returns=(r - args.reduce_parallelism) * args.num_workers,
            )
        reduce_results[:, r] = [
            final_merge.options(**_node_i(args, w, args.reduce_parallelism)).remote(
                args, w, r, *merge_results[w, :, r]
            )
            for w in range(args.num_workers)
        ]
        merge_results[:, :, r] = None
    del merge_results

    return ray.get(reduce_results.flatten().tolist())


@tracing_utils.timeit("sort", log_to_wandb=True)
def sort_main(args: Args):
    parts = sort_utils.load_manifest(args, constants.INPUT_MANIFEST_FILE)

    if args.simple_shuffle:
        results = sort_simple(args, parts)
    else:
        results = sort_two_stage(args, parts)

    if not args.skip_output:
        with open(constants.OUTPUT_MANIFEST_FILE, "w") as fout:
            writer = csv.writer(fout)
            writer.writerows(results)


# ------------------------------------------------------------
#     Main
# ------------------------------------------------------------


def _get_app_args(args: Args):
    # If no steps are specified, run all steps.
    args_dict = vars(args)
    if not any(args_dict[step] for step in STEPS):
        for step in STEPS:
            args_dict[step] = True

    args.total_data_size = args.total_gb * 10 ** 9
    args.num_mappers = int(np.ceil(args.total_data_size / args.input_part_size))
    assert args.num_mappers % args.num_workers == 0, args
    assert args.map_parallelism % args.merge_factor == 0, args
    args.merge_parallelism = args.map_parallelism // args.merge_factor
    args.num_rounds = int(
        np.ceil(args.num_mappers / args.num_workers / args.map_parallelism)
    )
    args.num_mergers_per_worker = args.num_rounds * args.merge_parallelism
    args.num_reducers = args.num_mappers
    assert args.num_reducers % args.num_workers == 0, args
    args.num_reducers_per_worker = args.num_reducers // args.num_workers


def init(args: Args) -> ray.actor.ActorHandle:
    global cluster
    logging_utils.init()
    cluster = ray_utils.init(args)
    os.makedirs(constants.WORK_DIR, exist_ok=True)
    _get_app_args(args)
    return tracing_utils.create_progress_tracker(args)


def main(args: Args):
    tracker = init(args)

    if args.generate_input:
        sort_utils.generate_input(args)

    if args.sort:
        for _ in range(args.repeat_sort):
            tracker.reset_gauges.remote()
            sort_main(args)

    if args.validate_output:
        sort_utils.validate_output(args)

    ray.get(tracker.performance_report.remote())


if __name__ == "__main__":
    main(get_args())
