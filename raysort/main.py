import argparse
import collections
import datetime
import logging
import os
import subprocess
import time

import numpy as np
import ray
import wandb

from raysort import constants
from raysort import file_utils
from raysort import logging_utils
from raysort import ray_utils
from raysort import sortlib
from raysort.types import *

GB_RECORDS = 1000 * 1000 * 10  # 1 GiB worth of records.


def get_args():
    parser = argparse.ArgumentParser()
    # Benchmark config

    parser.add_argument(
        "-m",
        "--num_mappers",
        default=4,
        type=int,
        help="number of mapper workers",
    )
    parser.add_argument(
        "-r",
        "--num_reducers",
        type=int,
        help="number of reducer workers; default to num_mappers",
    )
    parser.add_argument(
        "--records_per_mapper",
        default=int(4 * GB_RECORDS),
        type=int,
        help="total number of records = this * num_mappers",
    )
    parser.add_argument(
        "--reducer_batch_size",
        default=int(0.1 * GB_RECORDS),
        type=int,
        help="size of multipart upload chunks for reducer",
    )
    # Cluster config
    cluster_config_group = parser.add_argument_group()
    cluster_config_group.add_argument(
        "--cluster",
        action="store_true",
        help="try connecting to an existing Ray cluster",
    )
    # Which tasks to run?
    tasks_group = parser.add_argument_group(
        "tasks to run", "if no task is specified, will run all tasks"
    )
    tasks = ["generate_input", "sort", "validate_output", "cleanup"]
    for task in tasks:
        tasks_group.add_argument(
            f"--{task}", action="store_true", help=f"run task {task}"
        )

    args = parser.parse_args()
    # Derive additional arguments.
    if args.num_reducers is None:
        args.num_reducers = args.num_mappers
    args.num_records = args.records_per_mapper * args.num_mappers
    # If no tasks are specified, run all tasks.
    args_dict = vars(args)
    if not any(args_dict[task] for task in tasks):
        for task in tasks:
            args_dict[task] = True
    return args


@ray.remote
def mapper(args, mapper_id, boundaries):
    with ray.profiling.profile(f"M-{mapper_id}"):
        logging_utils.init()
        logging.info(f"M-{mapper_id} starting")
        part = file_utils.load_partition(mapper_id)
        logging.info(f"M-{mapper_id} downloaded partition")
        chunks = sortlib.sort_and_partition(part, boundaries)
        logging.info(f"M-{mapper_id} sorted partition")
        file_utils.save_partition(mapper_id, part)
        ret = [ChunkInfo(mapper_id, offset, size) for offset, size in chunks]
        logging.info(
            f"M-{mapper_id} uploaded sorted partition; output chunks (first {constants.LOGGING_ITEMS_LIMIT}): %s",
            ret[: constants.LOGGING_ITEMS_LIMIT],
        )
        return ret


# By using varargs, Ray will schedule the reducer when its arguments are ready.
@ray.remote
def reducer(args, reducer_id, *chunks):
    if len(chunks) == 1:
        chunks = chunks[0]
    with ray.profiling.profile(f"R-{reducer_id}"):
        logging_utils.init()
        logging.info(f"R-{reducer_id} starting")
        logging.info(
            f"R-{reducer_id} input chunks (first {constants.LOGGING_ITEMS_LIMIT}): %s",
            chunks[: constants.LOGGING_ITEMS_LIMIT],
        )
        chunks = file_utils.load_chunks(chunks)
        logging.info(
            f"R-{reducer_id} downloaded chunks (first {constants.LOGGING_ITEMS_LIMIT}): %s",
            [c.getbuffer().nbytes for c in chunks[: constants.LOGGING_ITEMS_LIMIT]],
        )
        merger = sortlib.merge_partitions(chunks, args.reducer_batch_size)
        file_utils.save_partition_mpu(
            reducer_id,
            merger,
        )
        logging.info(f"R-{reducer_id} uploaded partition")


def progress_tracker(mapper_results, reducer_futs):
    logging.info("Progress tracker started")
    future_to_id = {}
    mapper_futs = mapper_results[:, 0].tolist()
    for i, fut in enumerate(mapper_futs):
        future_to_id[fut] = ("mapper", f"M-{i}")
    for i, fut in enumerate(reducer_futs):
        future_to_id[fut] = ("reducer", f"R-{i}")

    for kind in ["mapper", "reducer"]:
        logging_utils.wandb_log({f"{kind}_completed": 0})

    done_count_dict = collections.defaultdict(int)
    rest = mapper_futs + reducer_futs
    while len(rest) > 0:
        done, rest = ray.wait(rest)
        assert len(done) == 1, (done, rest)
        kind, task_id = future_to_id[done[0]]
        done_count_dict[kind] += 1
        logging_utils.wandb_log({f"{kind}_completed": done_count_dict[kind]})


def sort_main(args):
    M = args.num_mappers
    R = args.num_reducers

    mapper_mem = args.num_records * constants.RECORD_SIZE / M * 1.25
    reducer_mem = args.num_records * constants.RECORD_SIZE / R * 1.25

    boundaries = sortlib.get_boundaries(R)
    mapper_results = np.empty((M, R), dtype=object)
    for m in range(M):
        mapper_results[m, :] = mapper.options(num_returns=R, memory=mapper_mem).remote(
            args, m, boundaries
        )

    logging.info(f"Futures: {mapper_results.shape}\n{mapper_results}")

    reducer_results = []
    for r in range(R):
        parts = mapper_results[:, r].tolist()
        ret = reducer.options(memory=reducer_mem).remote(args, r, *parts)
        reducer_results.append(ret)

    progress_tracker(mapper_results, reducer_results)
    ray.get(reducer_results)


def export_timeline():
    timestr = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"/tmp/timeline-{timestr}.json"
    ray.timeline(filename=filename)
    logging.info(f"Exported timeline to {filename}")
    try:
        wandb.save(filename)
    except wandb.Error:
        pass


def print_memory():
    logging.info(
        subprocess.run("ray status", shell=True, capture_output=True).stdout.decode(
            "ascii"
        )
    )
    logging.info(
        subprocess.run(
            f"cat /proc/{os.getpid()}/status | grep Vm", shell=True, capture_output=True
        ).stdout.decode("ascii")
    )
    logging.info(
        "\n%s",
        subprocess.run("free -h", shell=True, capture_output=True).stdout.decode(
            "ascii"
        ),
    )


def main():
    logging_utils.init()
    args = get_args()

    if args.cluster:
        ray.init(address="auto")
        # ray_utils.request_resources(args)
    else:
        ray.init()

    file_utils.touch_prefixes(args)
    if args.generate_input:
        file_utils.generate_input(args)

    if args.sort:
        logging_utils.wandb_init(args)
        start_time = time.time()
        sort_main(args)
        end_time = time.time()
        logging_utils.log_benchmark_result(args, end_time - start_time)

    if args.validate_output:
        file_utils.validate_output(args)

    if args.cleanup:
        file_utils.cleanup(args)

    export_timeline()


if __name__ == "__main__":
    main()
