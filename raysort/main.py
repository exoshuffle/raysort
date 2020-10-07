import time

from absl import app
import numpy as np
import ray

import logging_utils
import object_store_utils
import params
import sortlib.sortlib as sortlib


@ray.remote(num_returns=params.NUM_REDUCERS)
def mapper(mapper_id, boundaries):
    log = logging_utils.logger()
    log.info(f"Starting Mapper M-{mapper_id}")
    part = object_store_utils.load_partition(mapper_id)
    chunks = sortlib.partition_and_sort(part, boundaries)
    # TODO: Workaround: must create a copy, otherwise buffer gets
    # corrupted during CloudPickle serialization.
    chunks = [np.array(chunk) for chunk in chunks]
    log.info(f"Output sizes: %s", [chunk.shape for chunk in chunks])
    if params.NUM_REDUCERS == 1:
        return chunks[0]
    return chunks


# By using varargs, Ray will schedule the reducer when its arguments are ready.
@ray.remote
def reducer(reducer_id, *parts):
    log = logging_utils.logger()
    log.info(f"Starting Reducer R-{reducer_id}")
    # Filter out the empty partitions.
    parts = [part for part in parts if part.size > 0]
    # https://github.com/ray-project/ray/blob/master/python/ray/cloudpickle/cloudpickle_fast.py#L448
    # Pickled numpy arrays are by default not writable, which creates problem for sortlib.
    # Workaround until CloudPickle has a fix.
    for part in parts:
        part.setflags(write=True)
    log.info(f"Input sizes: %s", [part.shape for part in parts])
    merged = sortlib.merge_partitions(parts)
    object_store_utils.save_partition(reducer_id, merged)
    return True


def main(argv):
    ray.init()
    log = logging_utils.logger()
    log.info("Ray initialized")

    object_store_utils.prepare_input()

    start_time = time.time()

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
    assert all(reducer_results), "Some task failed :("

    end_time = time.time()
    duration = end_time - start_time
    total_size = params.TOTAL_NUM_RECORDS * params.RECORD_SIZE / 10 ** 9
    print(
        f"Sorting {params.TOTAL_NUM_RECORDS:,} records ({total_size} GiB) took {duration:.3f} seconds."
    )

    object_store_utils.validate_output()


if __name__ == "__main__":
    app.run(main)
