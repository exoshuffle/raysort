import argparse
import csv
import datetime
import logging
import os
import random
import subprocess
import tempfile
import time
from typing import Callable, Dict, Iterable, List, Tuple

import numpy as np
import ray

from raysort import constants
from raysort import logging_utils
from raysort import sortlib
from raysort import tracing_utils
from raysort.types import \
    BlockInfo, ByteCount, RecordCount, PartId, PartInfo, Path

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
        "--total_data_size",
        default=1 * 1000 * 1024 * 1024 * 1024,
        type=ByteCount,
        help="total data size in bytes",
    )
    parser.add_argument(
        "--input_part_size",
        default=2500 * 1024 * 1024,
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
    # Which steps to run?
    steps_grp = parser.add_argument_group(
        "steps to run", "if no  is specified, will run all steps")
    for step in STEPS:
        steps_grp.add_argument(f"--{step}", action="store_true")
    return parser.parse_args(*args, **kwargs)


# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------


def _part_info(args: Args, part_id: PartId, kind="input") -> PartInfo:
    node = ray.util.get_node_ip_address()
    mnt = random.choice(args.mount_points)
    filepath = _get_part_path(mnt, part_id, kind)
    return PartInfo(part_id, node, filepath)


def _get_part_path(mnt: Path, part_id: PartId, kind="input") -> Path:
    assert kind in {"input", "output", "temp"}
    dir_fmt = constants.DATA_DIR_FMT[kind]
    dirpath = dir_fmt.format(mnt=mnt)
    os.makedirs(dirpath, exist_ok=True)
    filename_fmt = constants.FILENAME_FMT[kind]
    filename = filename_fmt.format(part_id=part_id)
    filepath = os.path.join(dirpath, filename)
    return filepath


@ray.remote
def generate_part(args: Args, part_id: PartId, size: RecordCount,
                  offset: RecordCount) -> PartInfo:
    logging_utils.init()
    pinfo = _part_info(args, part_id)
    subprocess.run(
        [constants.GENSORT_PATH, f"-b{offset}", f"{size}", pinfo.path],
        check=True)
    logging.info(f"Generated input {pinfo}")
    return pinfo, size


def generate_input(args: Args):
    if args.skip_input:
        return
    total_size = constants.bytes_to_records(args.total_data_size)
    size = constants.bytes_to_records(args.input_part_size)
    offset = 0
    tasks = []
    for part_id in range(args.num_mappers):
        node = args.node_ips[part_id % args.num_workers]
        tasks.append(
            generate_part.options(**_node_res(node)).remote(
                args, part_id, min(size, total_size - offset), offset))
        offset += size
    logging.info(f"Generating {len(tasks)} partitions")
    parts = ray.get(tasks)
    assert sum([s for _, s in parts]) == total_size, (parts, args)
    with open(constants.INPUT_MANIFEST_FILE, "w") as fout:
        writer = csv.writer(fout)
        writer.writerows([p for p, _ in parts])


# ------------------------------------------------------------
#     Sort
# ------------------------------------------------------------


def _load_manifest(args: Args, path: Path) -> List[PartInfo]:
    if args.skip_input:
        return [
            PartInfo(i, args.node_ips[i % args.num_workers], None)
            for i in range(args.num_mappers)
        ]
    with open(path) as fin:
        reader = csv.reader(fin)
        return [
            PartInfo(int(part_id), node, path)
            for part_id, node, path in reader
        ]


def _generate_partition(part_size: int) -> np.ndarray:
    num_records = part_size // 100
    mat = np.empty((num_records, 100), dtype=np.uint8)
    mat[:, :10] = np.frombuffer(np.random.default_rng().bytes(num_records *
                                                              10),
                                dtype=np.uint8).reshape((num_records, -1))
    return mat.flatten()


def _load_partition(args: Args, path: Path) -> np.ndarray:
    if args.skip_input:
        return _generate_partition(args.input_part_size)
    return np.fromfile(path, dtype=np.uint8)


def _dummy_sort_and_partition(part: np.ndarray,
                              bounds: List[int]) -> List[BlockInfo]:
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
    sort_fn = _dummy_sort_and_partition \
        if args.skip_sorting else sortlib.sort_and_partition
    blocks = sort_fn(part, bounds)
    return [part[offset:offset + size] for offset, size in blocks]
    # return [ray.put(part[offset:offset + size]) for offset, size in blocks]


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
) -> PartInfo:
    M = len(blocks)
    total_bytes = sum(b.size for b in blocks)
    num_records = int(total_bytes / len(bounds) * 2 // constants.RECORD_SIZE)

    # blocks = ray.get(list(blocks))

    def get_block(i, d):
        if i >= M or d > 0:
            return None
        return blocks[i]

    merge_fn = _dummy_merge if args.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, get_block, num_records, False, bounds)

    ret = []
    for i, datachunk in enumerate(merger):
        if args.use_object_store:
            ret.append(ray.put(datachunk))
        else:
            part_id = constants.merge_part_ids(worker_id, merge_id, i)
            pinfo = _part_info(args, part_id, kind="temp")
            with open(pinfo.path, "wb", buffering=args.io_size) as fout:
                datachunk.tofile(fout)
            ret.append(pinfo)
    assert len(ret) == len(bounds), (ret, bounds)
    return ret


# TODO: Find out optimal reduce concurrency
@ray.remote(num_cpus=4)
@tracing_utils.timeit("reduce")
def final_merge(
    args: Args,
    worker_id: PartId,
    reducer_id: PartId,
    *parts: List[PartInfo],
) -> PartInfo:
    M = len(parts)

    def get_block(i: int, d: int) -> np.ndarray:
        if i >= M or d > 0:
            return None
        part = parts[i]
        if args.use_object_store:
            return ray.get(part)
        if part is None:
            return None
        with open(part.path, "rb", buffering=args.io_size) as fin:
            ret = np.fromfile(fin, dtype=np.uint8)
            if ret.size == 0:
                return None
            return ret

    part_id = constants.merge_part_ids(worker_id, reducer_id)
    pinfo = _part_info(args, part_id, "output")
    return _merge_impl(args, M, pinfo, get_block, args.skip_output)


def _node_res(node: str) -> Dict[str, float]:
    return {"resources": {f"node:{node}": 1e-3}}


def get_boundaries(args: Args) -> Tuple[List[int], List[List[int]]]:
    merge_bounds_flat = sortlib.get_boundaries(args.num_workers *
                                               args.num_reducers_per_worker)
    merge_bounds = np.array(merge_bounds_flat, dtype=np.uint64).reshape(
        args.num_workers, args.num_reducers_per_worker).tolist()
    map_bounds = [b[0] for b in merge_bounds]
    return map_bounds, merge_bounds


@tracing_utils.timeit("sort")
def sort_main(args: Args):
    parts = _load_manifest(args, constants.INPUT_MANIFEST_FILE)
    assert len(parts) == args.num_mappers
    map_bounds, merge_bounds = get_boundaries(args)

    mapper_opt = {"num_returns": args.num_workers}
    merger_opt = {"num_returns": args.num_reducers_per_worker}
    merge_results = np.empty((args.num_workers, args.num_mergers_per_worker,
                              args.num_reducers_per_worker),
                             dtype=object)
    num_map_tasks_per_round = args.num_workers * args.map_parallelism
    worker_res = [_node_res(node) for node in args.node_ips]

    part_id = 0
    for round in range(args.num_rounds):
        # Submit map tasks.
        num_map_tasks = min(num_map_tasks_per_round,
                            args.num_mappers - part_id)
        map_results = np.empty((num_map_tasks, args.num_workers), dtype=object)
        for _ in range(num_map_tasks):
            _, node, path = parts[part_id]
            opt = dict(**mapper_opt, **_node_res(node))
            m = part_id % num_map_tasks_per_round
            map_results[m, :] = mapper.options(**opt).remote(
                args, part_id, map_bounds, path)
            part_id += 1

        # Make sure previous rounds finish before scheduling merge tasks.
        num_extra_rounds = round - args.num_concurrent_rounds + 1
        if num_extra_rounds > 0:
            ray.wait([t for t in merge_results[0, :, 0] if t is not None],
                     num_returns=num_extra_rounds * args.merge_parallelism,
                     fetch_local=False)

        # Submit merge tasks.
        for j in range(args.merge_parallelism):
            m = round * args.merge_parallelism + j
            f = int(np.ceil(num_map_tasks / args.merge_parallelism))
            for w in range(args.num_workers):
                merge_results[w, m, :] = merge_mapper_blocks.options(
                    **merger_opt, **worker_res[w]).remote(
                        args, w, m, merge_bounds[w],
                        *map_results[j * f:(j + 1) * f, w].flatten().tolist())

        # Wait for at least one map task from this round to finish before
        # scheduling the next round.
        ray.wait(map_results[:, 0].tolist(), fetch_local=False)
        map_results = None

    if args.skip_final_merge:
        tasks = [t for t in merge_results.flatten() if t is not None]
        ray.wait(tasks, num_returns=len(tasks), fetch_local=False)
        return

    # print(ray.internal.internal_api.memory_summary())

    # Submit second-stage reduce tasks.
    reducer_results = np.empty(
        (args.num_workers, args.num_reducers_per_worker), dtype=object)
    for r in range(args.num_reducers_per_worker):
        reducer_results[:, r] = [
            final_merge.options(**worker_res[w]).remote(
                args, w, r, *merge_results[w, :, r].tolist())
            for w in range(args.num_workers)
        ]

    reducer_results = reducer_results.flatten().tolist()
    reducer_results = ray.get(reducer_results)

    if not args.skip_output:
        with open(constants.OUTPUT_MANIFEST_FILE, "w") as fout:
            writer = csv.writer(fout)
            writer.writerows(reducer_results)


# ------------------------------------------------------------
#     Validate Output
# ------------------------------------------------------------


def _run_valsort(args: List[str]):
    proc = subprocess.run([constants.VALSORT_PATH] + args, capture_output=True)
    if proc.returncode != 0:
        logging.critical("\n" + proc.stderr.decode("ascii"))
        raise RuntimeError(f"Validation failed: {args}")


@ray.remote
def validate_part(path: Path):
    logging_utils.init()
    sum_path = path + ".sum"
    _run_valsort(["-o", sum_path, path])
    logging.info(f"Validated output {path}")
    with open(sum_path, "rb") as fin:
        return os.path.getsize(path), fin.read()


def validate_output(args: Args):
    if args.skip_sorting or args.skip_output:
        return
    partitions = _load_manifest(args, constants.OUTPUT_MANIFEST_FILE)
    results = []
    for _, node, path in partitions:
        results.append(validate_part.options(**_node_res(node)).remote(path))
    logging.info(f"Validating {len(results)} partitions")
    results = ray.get(results)
    total = sum(s for s, _ in results)
    assert total == args.total_data_size, total - args.total_data_size
    all_checksum = b"".join(c for _, c in results)
    with tempfile.NamedTemporaryFile() as fout:
        fout.write(all_checksum)
        fout.flush()
        _run_valsort(["-s", fout.name])
    logging.info("All OK!")


# ------------------------------------------------------------
#     Main
# ------------------------------------------------------------


def _get_mount_points():
    mnt = "/mnt"
    if os.path.exists(mnt):
        ret = [
            os.path.join(mnt, d) for d in os.listdir(mnt)
            if d.startswith("nvme")
        ]
        if len(ret) > 0:
            return ret
    return [tempfile.gettempdir()]


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
            r.split(":")[1] for r in resources
            if r.startswith("node:") and r != f"node:{head_addr}"
        ]
        args.num_nodes = args.num_workers + 1
        assert args.num_workers == len(args.node_ips), args
    args.mount_points = _get_mount_points()
    args.node_workmem = resources["memory"] / args.num_nodes
    args.node_objmem = resources["object_store_memory"] / args.num_nodes


def _round_up_power_of_two(x: int) -> int:
    ret = 1
    while ret <= x:
        ret <<= 1
    return ret


def _get_app_args(args: Args):
    args.run_id = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    # If no steps are specified, run all steps.
    args_dict = vars(args)
    if not any(args_dict[step] for step in STEPS):
        for step in STEPS:
            args_dict[step] = True

    args.num_mappers = int(np.ceil(args.total_data_size /
                                   args.input_part_size))
    assert args.map_parallelism % args.merge_factor == 0, args
    args.merge_parallelism = args.map_parallelism // args.merge_factor
    args.num_rounds = int(
        np.ceil(args.num_mappers / args.num_workers / args.map_parallelism))
    args.num_mergers_per_worker = args.num_rounds * args.merge_parallelism
    args.num_reducers_per_worker = _round_up_power_of_two(
        args.num_mergers_per_worker)


def init(args: Args):
    if not args.ray_address:
        total_mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        ray.init(
            resources={"worker": os.cpu_count() // 2},
            object_store_memory=total_mem * 0.4,
        )
    else:
        ray.init(address=args.ray_address)
    logging_utils.init()
    os.makedirs(constants.WORK_DIR, exist_ok=True)
    _get_resources_args(args)
    _get_app_args(args)
    logging.info(args)
    progress_tracker = tracing_utils.create_progress_tracker(args)
    return progress_tracker


def main(args: Args):
    agent = init(args)

    if args.generate_input:
        generate_input(args)

    if args.sort:
        sort_main(args)

    if args.validate_output:
        validate_output(args)

    tracing_utils.performance_report(args.run_id, agent)


if __name__ == "__main__":
    main(get_args())
