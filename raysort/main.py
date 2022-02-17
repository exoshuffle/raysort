import csv
import os
import random
import time
from typing import Callable, Dict, Iterable, List, Tuple, Union

import numpy as np
import ray

from raysort import app_args
from raysort import constants
from raysort import logging_utils
from raysort import ray_utils
from raysort import sortlib
from raysort import sort_utils
from raysort import tracing_utils
from raysort.typing import Args, BlockInfo, PartId, PartInfo, Path


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
    part = sort_utils.load_partition(args, path)
    assert part.size == args.input_part_size, (part.shape, args)
    load_duration = time.time() - start_time
    tracing_utils.record_value("map_load_time", load_duration)
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
        if not args.manual_spilling:
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
            # TODO(@lsf): support S3 for manual_spilling
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
    if args.skip_output:
        first_chunk = True
        for datachunk in merger:
            if first_chunk:
                first_chunk = False
                tracing_utils.record_value(
                    "output_time",
                    time.time(),
                    relative_to_start=True,
                    echo=True,
                    log_to_wandb=True,
                )
            del datachunk
    else:
        sort_utils.save_partition(args, pinfo.path, merger)
    return pinfo


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


def _get_node_res(args: Args, part: PartInfo, part_id: PartId) -> Dict:
    if part.node:
        return ray_utils.node_res(part.node)
    return ray_utils.node_i(args, part_id)


def sort_simple(args: Args, parts: List[PartInfo]) -> List[PartInfo]:
    bounds, _ = get_boundaries(args.num_reducers)

    mapper_opt = {"num_returns": args.num_reducers}
    map_results = np.empty((args.num_mappers, args.num_reducers), dtype=object)
    num_map_tasks_per_round = args.num_workers * args.map_parallelism

    for part_id in range(args.num_mappers):
        part = parts[part_id]
        opt = dict(**mapper_opt, **_get_node_res(args, part, part_id))
        map_results[part_id, :] = mapper.options(**opt).remote(
            args, part_id, bounds, part.path
        )
        # TODO(@lsf): try memory-aware scheduling
        if part_id > 0 and part_id % num_map_tasks_per_round == 0:
            # Wait for at least one map task from this round to finish before
            # scheduling the next round.
            ray_utils.wait(
                map_results[:, 0], num_returns=part_id - num_map_tasks_per_round + 1
            )

    ray_utils.fail_and_restart_node(args)

    if args.skip_final_merge:
        ray_utils.wait(map_results[:, 0], wait_all=True)
        return []

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
            final_merge.options(
                **ray_utils.node_i(args, w, args.reduce_parallelism)
            ).remote(args, w, r, *map_results[:, w * args.num_reducers_per_worker + r])
            for w in range(args.num_workers)
        ]
    return ray.get(reduce_results.flatten().tolist())


def sort_riffle(args: Args, parts: List[PartInfo]) -> List[PartInfo]:
    round_merge_factor = args.merge_factor // args.map_parallelism

    start_time = time.time()
    map_bounds, _ = get_boundaries(1)
    merge_bounds, _ = get_boundaries(args.num_reducers)

    mapper_opt = {"num_returns": 1}
    merger_opt = {"num_returns": args.num_reducers}
    merge_results = np.empty(
        (args.num_workers * args.num_mergers_per_worker, args.num_reducers),
        dtype=object,
    )
    num_map_tasks_per_round = args.num_workers * args.map_parallelism

    all_map_results = []
    part_id = 0
    for round in range(args.num_rounds):
        # Submit map tasks.
        num_map_tasks = min(num_map_tasks_per_round, args.num_mappers - part_id)
        map_results = np.empty((args.num_workers, args.map_parallelism), dtype=object)
        for i in range(num_map_tasks):
            part = parts[part_id]
            opt = dict(**mapper_opt, **_get_node_res(args, part, part_id))
            m = part_id % num_map_tasks_per_round
            map_results[i % args.num_workers, i // args.num_workers] = mapper.options(
                **opt
            ).remote(args, part_id, map_bounds, part.path)
            part_id += 1
        all_map_results.append(map_results)

        if (round + 1) % round_merge_factor == 0:
            # Make sure all merge tasks previous rounds finish.
            num_extra_rounds = (
                (round + 1) // round_merge_factor - args.num_concurrent_rounds + 1
            )
            if num_extra_rounds > 0:
                ray_utils.wait(
                    merge_results[:, 0].flatten(),
                    num_returns=num_extra_rounds * args.num_workers,
                )

            # Submit merge tasks.
            merge_start = round - round_merge_factor + 1
            merge_end = round + 1
            map_results_to_merge = np.concatenate(
                all_map_results[merge_start:merge_end], axis=1
            )
            m = round
            for w in range(args.num_workers):
                map_blocks = map_results_to_merge[w, :]
                merge_results[
                    w + m * args.num_workers, :
                ] = merge_mapper_blocks.options(
                    **merger_opt, **ray_utils.node_i(args, w)
                ).remote(
                    args, w, m, merge_bounds, *map_blocks
                )

        if start_time > 0 and (time.time() - start_time) > args.fail_time:
            ray_utils.fail_and_restart_node(args)
            start_time = -1

        # Wait for at least one map task from this round to finish before
        # scheduling the next round.
        ray_utils.wait(map_results.flatten())

    ray_utils.fail_and_restart_node(args)

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
            final_merge.options(
                **ray_utils.node_i(args, w, args.reduce_parallelism)
            ).remote(
                args, w, r, *merge_results[:, w * args.num_reducers_per_worker + r]
            )
            for w in range(args.num_workers)
        ]

    return ray.get(reduce_results.flatten().tolist())


def sort_two_stage(args: Args, parts: List[PartInfo]) -> List[PartInfo]:
    start_time = time.time()
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

    all_map_results = []  # For Magnet.
    part_id = 0
    for round in range(args.num_rounds):
        # Submit map tasks.
        num_map_tasks = min(num_map_tasks_per_round, args.num_mappers - part_id)
        map_results = np.empty((num_map_tasks, args.num_workers), dtype=object)
        for _ in range(num_map_tasks):
            part = parts[part_id]
            opt = dict(**mapper_opt, **_get_node_res(args, part, part_id))
            m = part_id % num_map_tasks_per_round
            map_results[m, :] = mapper.options(**opt).remote(
                args, part_id, map_bounds, part.path
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
                    **merger_opt, **ray_utils.node_i(args, w)
                ).remote(args, w, m, merge_bounds[w], *map_blocks)

        if start_time > 0 and (time.time() - start_time) > args.fail_time:
            ray_utils.fail_and_restart_node(args)
            start_time = -1

        # Wait for at least one map task from this round to finish before
        # scheduling the next round.
        ray_utils.wait(map_results[:, 0])
        if args.magnet:
            all_map_results.append(map_results)
        else:
            del map_results

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
            final_merge.options(
                **ray_utils.node_i(args, w, args.reduce_parallelism)
            ).remote(args, w, r, *merge_results[w, :, r])
            for w in range(args.num_workers)
        ]
        merge_results[:, :, r] = None

    return ray.get(reduce_results.flatten().tolist())


@tracing_utils.timeit("sort", log_to_wandb=True)
def sort_main(args: Args):
    parts = sort_utils.load_manifest(args)

    if args.simple_shuffle:
        results = sort_simple(args, parts)
    elif args.riffle:
        results = sort_riffle(args, parts)
    else:
        results = sort_two_stage(args, parts)

    if not args.skip_output:
        with open(sort_utils.get_manifest_file(args, kind="output"), "w") as fout:
            writer = csv.writer(fout)
            writer.writerows(results)


def init(args: Args) -> ray.actor.ActorHandle:
    logging_utils.init()
    ray_utils.init(args)
    sort_utils.init(args)
    app_args.derive_app_args(args)
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
    main(app_args.get_args())
