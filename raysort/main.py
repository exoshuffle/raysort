import argparse
import csv
import datetime
import logging
import os
import random
import time
from typing import Callable, Dict, Iterable, List, Tuple, Union

import numpy as np
import ray
from ray.cluster_utils import Cluster

from raysort import constants
from raysort import logging_utils
from raysort import sortlib
from raysort import sort_utils
from raysort import tracing_utils
from raysort.types import BlockInfo, ByteCount, PartId, PartInfo, Path

Args = argparse.Namespace

# ------------------------------------------------------------
#     Parse Arguments
# ------------------------------------------------------------

STEPS = ["generate_input", "sort", "validate_output"]


def get_args(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ray_address",
        default="auto",
        type=str,
        help="if set to None, will launch a local Ray cluster",
    )
    parser.add_argument(
        "--total_tb",
        default=1,
        type=float,
        help="total data size in TiB",
    )
    parser.add_argument(
        "--input_part_size",
        default=2500 * 1000 * 1000,
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
        "--use_object_store",
        default=False,
        action="store_true",
        help="if set, will use object store for 2nd-stage reduce",
    )
    parser.add_argument(
        "--simple_shuffle",
        default=False,
        action="store_true",
        help="if set, will use the simple map-reduce version",
    )
    parser.add_argument(
        "--sim_cluster",
        default=False,
        action="store_true",
        help="if set, will simulate a cluster rather than one node when run locally",
    )
    # Which steps to run?
    steps_grp = parser.add_argument_group(
        "steps to run", "if no  is specified, will run all steps"
    )
    for step in STEPS:
        steps_grp.add_argument(f"--{step}", action="store_true")
    return parser.parse_args(*args, **kwargs)


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
    ret = [part[offset : offset + size] for offset, size in blocks]
    # ret = [ray.put(part[offset:offset + size]) for offset, size in blocks]
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


def _merge_impl(
    args: Args,
    M: int,
    pinfo: PartInfo,
    get_block: Callable[[int, int], np.ndarray],
    skip_output=False,
) -> PartInfo:
    merge_fn = _dummy_merge if args.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, get_block)
    if skip_output:
        for datachunk in merger:
            del datachunk
        return pinfo

    with open(pinfo.path, "wb", buffering=args.io_size) as fout:
        for datachunk in merger:
            fout.write(datachunk)
    return pinfo


@ray.remote
@tracing_utils.timeit("merge")
def merge_mapper_blocks(
    args: Args,
    worker_id: PartId,
    merge_id: PartId,
    bounds: List[int],
    *blocks: List[np.ndarray],
) -> Union[List[PartInfo], List[np.ndarray]]:
    M = len(blocks)
    # blocks = ray.get(list(blocks))

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
            ret.append(datachunk)
            # ret.append(ray.put(datachunk))
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


@ray.remote
@tracing_utils.timeit("reduce")
def final_merge(
    args: Args,
    worker_id: PartId,
    reducer_id: PartId,
    *parts: Union[List[PartInfo], List[np.ndarray]],
) -> PartInfo:
    M = len(parts)

    def get_block(i: int, d: int) -> np.ndarray:
        if i >= M or d > 0:
            return None
        part = parts[i]
        if isinstance(part, np.ndarray):
            return part
            # return ray.get(part)
        if part is None:
            return None
        with open(part.path, "rb", buffering=args.io_size) as fin:
            ret = np.fromfile(fin, dtype=np.uint8)
            if ret.size == 0:
                return None
            return ret

    part_id = constants.merge_part_ids(worker_id, reducer_id)
    pinfo = sort_utils.part_info(args, part_id, kind="output")
    return _merge_impl(args, M, pinfo, get_block, args.skip_output)


def _node_res(node_ip: str, parallelism: int = 1000) -> Dict[str, float]:
    return {"resources": {f"node:{node_ip}": 1 / parallelism}}


def _node_i(args: Args, node_i: int, parallelism: int = 1000) -> Dict[str, float]:
    return _node_res(args.node_ips[node_i], parallelism)


def _ray_wait(
    futures, wait_all: bool = False, **kwargs
) -> Tuple[List[ray.ObjectRef], List[ray.ObjectRef]]:
    to_wait = [f for f in futures if f is not None]
    default_kwargs = dict(fetch_local=False)
    if wait_all:
        default_kwargs.update(num_returns=len(to_wait))
    kwargs = dict(**default_kwargs, **kwargs)
    return ray.wait(to_wait, **kwargs)


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

    for part_id in range(args.num_mappers):
        _, node, path = parts[part_id]
        opt = dict(**mapper_opt, **_node_res(node))
        map_results[part_id, :] = mapper.options(**opt).remote(
            args, part_id, bounds, path
        )

    if args.skip_final_merge:
        _ray_wait(map_results[:, 0], wait_all=True)
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

        # Make sure previous rounds finish before scheduling merge tasks.
        num_extra_rounds = round - args.num_concurrent_rounds + 1
        if num_extra_rounds > 0:
            _ray_wait(
                merge_results[0, :, 0],
                num_returns=num_extra_rounds * args.merge_parallelism,
            )

        # Submit merge tasks.
        f = int(np.ceil(num_map_tasks / args.merge_parallelism))
        for j in range(args.merge_parallelism):
            m = round * args.merge_parallelism + j
            for w in range(args.num_workers):
                merge_results[w, m, :] = merge_mapper_blocks.options(
                    **merger_opt, **_node_i(args, w)
                ).remote(
                    args,
                    w,
                    m,
                    merge_bounds[w],
                    *map_results[j * f : (j + 1) * f, w],
                )

        # Wait for at least one map task from this round to finish before
        # scheduling the next round.
        _ray_wait(map_results[:, 0])
        del map_results

    if args.skip_final_merge:
        _ray_wait(merge_results.flatten(), wait_all=True)
        return []

    # Submit second-stage reduce tasks.
    reducer_results = np.empty(
        (args.num_workers, args.num_reducers_per_worker), dtype=object
    )
    for r in range(args.num_reducers_per_worker):
        reducer_results[:, r] = [
            final_merge.options(**_node_i(args, w, args.reduce_parallelism)).remote(
                args, w, r, *merge_results[w, :, r]
            )
            for w in range(args.num_workers)
        ]
    del merge_results

    reducer_results = reducer_results.flatten().tolist()
    return ray.get(reducer_results)


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


def _get_resources_args(args: Args):
    resources = ray.cluster_resources()
    logging.info(f"Cluster resources: {resources}")
    args.num_workers = int(resources["worker"])
    head_addr = ray.util.get_node_ip_address()
    if not args.ray_address:
        args.node_ips = [head_addr] * args.num_workers
        args.num_nodes = 1
    else:
        args.node_ips = [
            r.split(":")[1]
            for r in resources
            if r.startswith("node:") and r != f"node:{head_addr}"
        ]
        args.num_nodes = args.num_workers + 1
        assert args.num_workers == len(args.node_ips), args
    args.mount_points = sort_utils.get_mount_points()
    args.node_workmem = resources["memory"] / args.num_nodes
    args.node_objmem = resources["object_store_memory"] / args.num_nodes


def _get_app_args(args: Args):
    args.run_id = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    # If no steps are specified, run all steps.
    args_dict = vars(args)
    if not any(args_dict[step] for step in STEPS):
        for step in STEPS:
            args_dict[step] = True

    args.total_data_size = args.total_tb * 10 ** 12
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


def _build_cluster(system_config, num_nodes=2, num_cpus=4, object_store_memory=1 * 1024 * 1024 * 1024):
    cluster = Cluster()
    cluster.add_node(
         resources={"head": 1},
         object_store_memory=2 * 1024 * 1024 * 1024,
         _system_config=system_config,
    )
    cluster.add_node(
        resources={"node_1": 1, "worker": 1},
        object_store_memory=object_store_memory,
        num_cpus=num_cpus//2,
    )
    cluster.add_node(
        resources={"node_2": 1, "worker": 1},
        object_store_memory=object_store_memory,
        num_cpus=num_cpus//2,
    )
    cluster.wait_for_nodes()
    return cluster


def _init_ray(addr: str, sim_cluster: bool):
    if addr:
        ray.init(address=addr)
        return
    system_config = {
        "max_io_workers": 8,
        "object_spilling_threshold": 1,
    }
    if os.path.exists("/mnt/nvme0/tmp"):
        system_config.update(
            object_spilling_config='{"type":"filesystem","params":{"directory_path":["/mnt/nvme0/tmp/ray"]}}'
        )
    if sim_cluster:
        cluster = _build_cluster(system_config)
        cluster.connect()
    else:
        ray.init(
            resources={"head": 1, "worker": os.cpu_count() // 2},
            object_store_memory=2 * 1024 * 1024 * 1024,
            _system_config=system_config,
        )


def init(args: Args) -> ray.actor.ActorHandle:
    _init_ray(args.ray_address, args.sim_cluster)
    logging_utils.init()
    os.makedirs(constants.WORK_DIR, exist_ok=True)
    _get_resources_args(args)
    _get_app_args(args)
    return tracing_utils.create_progress_tracker(args)


def main(args: Args):
    tracker = init(args)

    if args.generate_input:
        sort_utils.generate_input(args)

    if args.sort:
        sort_main(args)

    if args.validate_output:
        sort_utils.validate_output(args)

    ray.get(tracker.performance_report.remote())


if __name__ == "__main__":
    main(get_args())
