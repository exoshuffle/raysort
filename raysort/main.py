import argparse
import logging
import time

import numpy as np
import ray

from raysort import logging_utils
from raysort import file_utils
from raysort import params
from raysort import sortlib


def get_args():
    parser = argparse.ArgumentParser()
    # Benchmark config
    parser.add_argument(
        "--num_records",
        "-n",
        default=1000 * 1000 * 10,  # 1 GiB
        type=int,
        help="Each record is 100B. Official requirement is 10^12 records (100 TiB).",
    )
    # Cluster config
    parser.add_argument(
        "--cluster",
        action="store_true",
        help="try connecting to an existing Ray cluster",
    )
    # Tasks
    parser.add_argument("--all", action="store_true", help="run the entire benchmark")
    parser.add_argument(
        "--generate_input", action="store_true", help="run the generate_input step"
    )
    parser.add_argument("--sort", action="store_true", help="run the sort step")
    parser.add_argument(
        "--validate_output", action="store_true", help="run the validate_output step"
    )
    args = parser.parse_args()
    if args.all:
        args.generate_input = True
        args.sort = True
        args.validate_output = True
    return args


@ray.remote(num_returns=params.NUM_REDUCERS)
def mapper(mapper_id, boundaries):
    logging_utils.init()
    logging.info(f"Starting Mapper M-{mapper_id}")
    part = file_utils.load_partition(mapper_id)
    chunks = sortlib.sort_and_partition(part, boundaries)
    logging.info(f"Output sizes: %s", [chunk.shape for chunk in chunks])
    if params.NUM_REDUCERS == 1:
        return chunks[0]
    return chunks


# By using varargs, Ray will schedule the reducer when its arguments are ready.
@ray.remote
def reducer(reducer_id, *parts):
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
    file_utils.save_partition(reducer_id, merged)
    return True


def sort_main():
    boundaries = sortlib.get_boundaries(params.NUM_REDUCERS)
    mapper_results = np.empty((params.NUM_MAPPERS, params.NUM_REDUCERS), dtype=object)
    for m in range(params.NUM_MAPPERS):
        mapper_results[m, :] = mapper.remote(m, boundaries)

    reducer_results = []
    for r in range(params.NUM_REDUCERS):
        parts = mapper_results[:, r].tolist()
        ret = reducer.remote(r, *parts)
        reducer_results.append(ret)

    reducer_results = ray.get(reducer_results)
    assert all(reducer_results), reducer_results


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
        sort_main()
        end_time = time.time()
        duration = end_time - start_time
        total_size = args.num_records * 100 / 10 ** 9
        logging.info(
            f"Sorting {args.num_records:,} records ({total_size} GiB) took {duration:.3f} seconds."
        )

    if args.validate_output:
        file_utils.validate_output()


if __name__ == "__main__":
    main()
