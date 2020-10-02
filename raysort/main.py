import subprocess

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
    log.info(f"Starting Mapper M-{mapper_id:02}")
    part = object_store_utils.load_partition(mapper_id)
    chunks = sortlib.partition_and_sort(part, boundaries)
    if params.NUM_REDUCERS == 1:
        return chunks[0]
    return chunks


@ray.remote
def reducer(reducer_id, parts):
    log = logging_utils.logger()
    log.info(f"Starting Reducer R-{reducer_id:02}")
    parts = ray.get(parts)
    # https://github.com/ray-project/ray/blob/master/python/ray/cloudpickle/cloudpickle_fast.py#L448
    # Pickled numpy arrays are by default not writable, which creates problem for sortlib.
    # Workaround until CloudPickle has a fix.
    for part in parts:
        part.setflags(write=True)
    merged = sortlib.merge_partitions(parts)
    object_store_utils.save_partition(reducer_id, merged)
    return True


def main(argv):
    ray.init()
    log = logging_utils.logger()
    log.info("Ray initialized.")

    object_store_utils.prepare_input()

    boundaries = sortlib.get_boundaries(params.NUM_REDUCERS)

    mapper_results = np.empty((params.NUM_MAPPERS, params.NUM_REDUCERS), dtype=object)
    for m in range(params.NUM_MAPPERS):
        mapper_results[m, :] = mapper.remote(m, boundaries)

    reducer_results = []
    for r in range(params.NUM_REDUCERS):
        parts = mapper_results[:, r].tolist()
        ret = reducer.remote(r, parts)
        reducer_results.append(ret)

    reducer_results = ray.get(reducer_results)
    assert all(reducer_results), "Some task failed :("

    object_store_utils.validate_output()


if __name__ == "__main__":
    app.run(main)
