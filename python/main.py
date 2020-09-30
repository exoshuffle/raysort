import logging

import numpy as np
import ray

import logging_utils
import params


@ray.remote(num_returns=params.NUM_REDUCERS)
def mapper(mapper_id):
    log = logging_utils.logger()
    log.info(f"Starting Mapper M#{mapper_id}")
    # load partition mapper_id
    # partition_and_sort into R chunks
    return [mapper_id * 100 + i for i in range(params.NUM_REDUCERS)]


@ray.remote
def reducer(reducer_id, chunks):
    log = logging_utils.logger()
    log.info(f"Starting Reducer R#{reducer_id}")
    chunks = ray.get(chunks)
    # merge R chunks
    # save to S3 or disk
    print(chunks)
    return 0


def main():
    ray.init()
    log = logging_utils.logger()
    log.info("Ray initialized.")

    chunks = np.empty((params.NUM_MAPPERS, params.NUM_REDUCERS), dtype=object)
    for m in range(params.NUM_MAPPERS):
        chunks[m, :] = mapper.remote(m)
    print(chunks)

    reducer_tasks = []
    for r in range(params.NUM_REDUCERS):
        reducer_chunks = chunks[:, r].tolist()
        ret = reducer.remote(r, reducer_chunks)
        reducer_tasks.append(ret)


if __name__ == "__main__":
    main()
