import argparse
import datetime
import logging
import os
import subprocess
import time

import numpy as np
import ray

from raysort import constants
from raysort import file_utils
from raysort import logging_utils
from raysort import ray_utils
from raysort import sortlib

GB_RECORDS = 1000 * 1000 * 10  # 1 GiB worth of records.
RECORDS_PER_WORKER = 4 * GB_RECORDS  # How many records should a worker process.


def get_args():
    parser = argparse.ArgumentParser()
    # Benchmark config
    n_workers = 4

    parser.add_argument(
        "--num_records",
        "-n",
        default=RECORDS_PER_WORKER * n_workers,
        type=int,
        help="Each record is 100B. Official requirement is 10^12 records (100 TiB).",
    )
    # Cluster config
    cluster_config_group = parser.add_argument_group()
    cluster_config_group.add_argument(
        "--cluster",
        action="store_true",
        help="try connecting to an existing Ray cluster",
    )
    cluster_config_group.add_argument(
        "--no_s3",
        action="store_true",
        help="use local storage only; workers must live on the same node",
    )
    parser.add_argument(
        "--num_mappers", default=n_workers, type=int, help="number of mapper workers"
    )
    parser.add_argument(
        "--num_reducers", default=n_workers, type=int, help="number of reducer workers"
    )
    # Which tasks to run?
    tasks_group = parser.add_argument_group(
        "tasks to run", "if no task is specified, will run all tasks"
    )
    tasks = ["generate_input", "sort", "validate_output"]
    for task in tasks:
        tasks_group.add_argument(
            f"--{task}", action="store_true", help=f"run task {task}"
        )
    # Ray config
    parser.add_argument(
        "--export_timeline", action="store_true", help="export a Ray timeline trace"
    )

    args = parser.parse_args()
    # If no tasks are specified, run all tasks.
    args_dict = vars(args)
    if not any(args_dict[task] for task in tasks):
        for task in tasks:
            args_dict[task] = True
    return args


@ray.remote(resources={"mapper": 1})
def mapper(args, mapper_id, boundaries):
    with ray.profiling.profile(f"Mapper M-{mapper_id}"):
        logging_utils.init()
        logging.info(f"Starting Mapper M-{mapper_id}")
        part = file_utils.load_partition(mapper_id, use_s3=not args.no_s3)
        chunks = sortlib.sort_and_partition(part, boundaries)
        logging.info("Output sizes: %s", [chunk.shape for chunk in chunks])
        # Ray expects a list of objects when num_returns > 1, and an object when
        # num_returns == 1. Hence the special case here.
        if len(chunks) == 1:
            return chunks[0]
        return chunks


# By using varargs, Ray will schedule the reducer when its arguments are ready.
@ray.remote(resources={"reducer": 1})
def reducer(args, reducer_id, *parts):
    with ray.profiling.profile(f"Reducer R-{reducer_id}"):
        logging_utils.init()
        logging.info(f"Starting Reducer R-{reducer_id}")
        # Filter out the empty partitions.
        parts = [part for part in parts if part.size > 0]
        logging.info("Input sizes: %s", [part.shape for part in parts])
        merged = sortlib.merge_partitions(parts)
        file_utils.save_partition(reducer_id, merged, use_s3=not args.no_s3)


def sort_main(args):
    M = args.num_mappers
    R = args.num_reducers

    boundaries = sortlib.get_boundaries(R)
    mapper_results = np.empty((M, R), dtype=object)
    for m in range(M):
        mapper_results[m, :] = mapper.options(num_returns=R).remote(args, m, boundaries)

    logging.info("Future IDs:\n%s", mapper_results)

    reducer_results = []
    for r in range(R):
        parts = mapper_results[:, r].tolist()
        ret = reducer.remote(args, r, *parts)
        reducer_results.append(ret)

    ray.get(reducer_results)


def export_timeline():
    timestr = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"timeline-{timestr}.json"
    ray.timeline(filename=filename)
    logging.info(f"Exported timeline to {filename}")


@ray.remote
def trace_memory():
    while True:
        subprocess.run("ray memory", shell=True)
        print("available_resources", ray.available_resources())
        print("cluster_resources", ray.cluster_resources())
        time.sleep(10)


def print_memory():
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
    args = get_args()
    if args.cluster:
        ray.init(address="auto")
    else:
        ray.init()

    logging_utils.init()
    if args.cluster:
        ray_utils.check_ray_resources(args)

    if args.generate_input:
        file_utils.generate_input(args)

    if args.sort:
        start_time = time.time()
        sort_main(args)
        end_time = time.time()
        duration = end_time - start_time
        total_size = args.num_records * 100 / 10 ** 9
        logging.info(
            f"Sorting {args.num_records:,} records ({total_size} GiB) took {duration:.3f} seconds."
        )

    if args.validate_output:
        file_utils.validate_output(args)

    if args.export_timeline:
        export_timeline()


if __name__ == "__main__":
    main()
