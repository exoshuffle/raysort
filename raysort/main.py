import argparse
import datetime
import logging
import time

import numpy as np
import ray

from raysort import constants
from raysort import file_utils
from raysort import logging_utils
from raysort import sortlib


def get_args():
    parser = argparse.ArgumentParser()
    # Benchmark config
    GB_RECORDS = 1000 * 1000 * 10  # 1 GiB worth of records.
    parser.add_argument(
        "--num_records",
        "-n",
        default=GB_RECORDS * 1,
        type=int,
        help="Each record is 100B. Official requirement is 10^12 records (100 TiB).",
    )
    # Cluster config
    cluster_config_group = parser.add_mutually_exclusive_group()
    cluster_config_group.add_argument(
        "--cluster",
        action="store_true",
        help="try connecting to an existing Ray cluster",
    )
    cluster_config_group.add_argument(
        "--no_s3",
        action="store_true",
        help="use local storage only, conflicts with --cluster",
    )
    parser.add_argument(
        "--num_mappers", default=4, type=int, help="number of mapper workers"
    )
    parser.add_argument(
        "--num_reducers", default=4, type=int, help="number of reducer workers"
    )
    # What stages to run?
    parser.add_argument("--all", action="store_true", help="run the entire benchmark")
    parser.add_argument(
        "--generate_input", action="store_true", help="run the generate_input step"
    )
    parser.add_argument("--sort", action="store_true", help="run the sort step")
    parser.add_argument(
        "--validate_output", action="store_true", help="run the validate_output step"
    )
    # Ray config
    parser.add_argument(
        "--export_timeline", action="store_true", help="export a Ray timeline trace"
    )

    args = parser.parse_args()
    # Setup tasks
    if args.all:
        args.generate_input = True
        args.sort = True
        args.validate_output = True
    return args


@ray.remote(num_cpus=constants.NODE_CPUS)
def mapper(args, mapper_id, boundaries):
    with ray.profiling.profile(f"Mapper M-{mapper_id}"):
        logging_utils.init()
        logging.info(f"Starting Mapper M-{mapper_id}")
        part = file_utils.load_partition(mapper_id, use_s3=not args.no_s3)
        chunks = sortlib.sort_and_partition(part, boundaries)
        logging.info(f"Output sizes: %s", [chunk.shape for chunk in chunks])
        # Ray expects a list of objects when num_returns > 1, and an object when
        # num_returns == 1. Hence the special case here.
        if len(chunks) == 1:
            return chunks[0]
        return chunks


# By using varargs, Ray will schedule the reducer when its arguments are ready.
@ray.remote(num_cpus=constants.NODE_CPUS)
def reducer(args, reducer_id, *parts):
    with ray.profiling.profile(f"Reducer R-{reducer_id}"):
        logging_utils.init()
        logging.info(f"Starting Reducer R-{reducer_id}")
        # Filter out the empty partitions.
        parts = [part for part in parts if part.size > 0]
        # https://github.com/ray-project/ray/blob/master/python/ray/cloudpickle/cloudpickle_fast.py#L448
        # Pickled numpy arrays are by default not writable, which creates problem for sortlib.
        # Workaround until CloudPickle has a fix.
        for part in parts:
            part.setflags(write=True)
        logging.info(f"Input sizes: %s", [part.shape for part in parts])
        merged = sortlib.merge_partitions(parts)
        file_utils.save_partition(reducer_id, merged, use_s3=not args.no_s3)


def sort_main(args):
    M = args.num_mappers
    R = args.num_reducers
    boundaries = sortlib.get_boundaries(R)
    mapper_results = np.empty((M, R), dtype=object)
    for m in range(M):
        mapper_results[m, :] = mapper.options(num_returns=R).remote(args, m, boundaries)

    reducer_results = []
    for r in range(R):
        parts = mapper_results[:, r].tolist()
        ret = reducer.remote(args, r, *parts)
        reducer_results.append(ret)

    ray.get(reducer_results)


def export_timeline():
    timestr = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    filename = f"timeline-{timestr}.json"
    ray.timeline(filename=filename)
    logging.info(f"Exported timeline to {filename}")


def main():
    args = get_args()
    if args.cluster:
        ray.init(address="auto", _redis_password="5241590000000000")
    else:
        ray.init()
    logging_utils.init()
    logging.info("Ray initialized")

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
