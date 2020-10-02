from absl import app
import numpy as np
import ray

import logging_utils
import object_store_utils
import params
import sortlib.sortlib as sortlib


def get_num_records(data):
    size = len(data)
    assert size % params.RECORD_SIZE == 0, (
        size,
        "input data size must be multiples of RECORD_SIZE",
    )
    num_records = int(size / params.RECORD_SIZE)
    return num_records


# @ray.remote(num_returns=params.NUM_REDUCERS)
def mapper(mapper_id, boundaries):
    log = logging_utils.logger()
    log.info(f"Starting Mapper M#{mapper_id}")
    part = object_store_utils.load_partition(mapper_id)
    part_size = get_num_records(part)
    log.info(f"partition_and_sort {part_size}")
    chunks = sortlib.partition_and_sort(part, part_size, boundaries)
    print(chunks)

    x = chunks[0]
    sz = get_num_records(x)
    print("new sz", sz, x)
    sortlib.partition_and_sort(x, sz, boundaries)

    return chunks


@ray.remote
def reducer(reducer_id, chunks):
    log = logging_utils.logger()
    log.info(f"Starting Reducer R#{reducer_id}")
    chunks = ray.get(chunks)
    # merge R chunks
    # save to S3 or disk
    print(chunks)
    return 0


def main(argv):
    # ray.init()
    log = logging_utils.logger()
    # log.info("Ray initialized.")

    object_store_utils.prepare_input()

    boundaries = sortlib.get_boundaries(params.NUM_REDUCERS)

    chunks = np.empty((params.NUM_MAPPERS, params.NUM_REDUCERS), dtype=object)
    for m in range(params.NUM_MAPPERS):
        # chunks[m, :] = mapper.remote(m, boundaries)
        ret = mapper(m, boundaries)
        print(ret)
        import IPython

        IPython.embed()
        exit(0)

    print(chunks)

    reducer_tasks = []
    for r in range(params.NUM_REDUCERS):
        reducer_chunks = chunks[:, r].tolist()
        ret = reducer.remote(r, reducer_chunks)
        reducer_tasks.append(ret)


if __name__ == "__main__":
    app.run(main)
