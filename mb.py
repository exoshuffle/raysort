import argparse
import subprocess
import os

import numpy as np
import ray
import tqdm

from raysort import logging_utils
from raysort import tracing_utils


def get_args(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ray_address",
        default="",
        type=str,
    )
    parser.add_argument(
        "--total_data_size",
        default=16_000_000_000,
        type=int,
    )
    parser.add_argument(
        "--num_objects",
        default=16000, # 1MB
        # default=16000 * 10,  # 100KB
        # default=16000 * 20,  # 50KB
        type=int,
    )
    parser.add_argument(
        # This must make it such that each task requires <1GB data.
        "--num_tasks",
        default=80,
        type=int,
    )
    args = parser.parse_args(args, **kwargs)
    args.object_size = args.total_data_size // args.num_objects
    args.num_objects_per_task = args.num_objects // args.num_tasks
    return args


@ray.remote(resources={"worker": 1})
def consume(*xs):
    return sum(x.size for x in xs)


@tracing_utils.timeit("consume_all")
def consume_all(args, refs):
    task = None
    for t in tqdm.tqdm(range(args.num_tasks)):
        if task is not None:
            ray.get(task)
        begin = t * args.num_objects_per_task
        end = (t + 1) * args.num_objects_per_task
        task = consume.remote(*refs[begin:end])

    # Wait for tasks.
    # with tqdm.tqdm(total=len(tasks)) as pbar:
    #     not_ready = tasks
    #     while not_ready:
    #         _, not_ready = ray.wait(not_ready, fetch_local=False)
    #         pbar.update(1)
    # print(ray.get(tasks))


@tracing_utils.timeit("produce_all")
def produce_all(args):
    refs = []
    for i in tqdm.tqdm(range(args.num_objects)):
        obj = np.full(args.object_size, i % 256, dtype=np.uint8)
        refs.append(ray.put(obj))
    return refs


@tracing_utils.timeit("e2e")
def work(args):
    # Produce.
    refs = produce_all(args)

    # Nuke the cache to test reading from disk.
    subprocess.run("sudo bash -c 'echo 3 > /proc/sys/vm/drop_caches'", shell=True)
    
    # Consume.
    consume_all(args, refs)


def main(args):
    logging_utils.init()

    system_config = {}
    if os.path.exists("/mnt/ebs0/tmp"):
        system_config.update(
            object_spilling_config='{"type":"filesystem","params":{"directory_path":["/mnt/ebs0/tmp/ray"]}}'
        )   
    ray.init(
        num_cpus=2,
        resources={"head": 1, "worker": 1},
        object_store_memory=1_000_000_000,
        _system_config=system_config
    )

    # Produce/consume and log statistics.
    tracker = tracing_utils.create_progress_tracker(args)  
    work(args)
    ray.get(tracker.performance_report.remote())


if __name__ == "__main__":
    main(get_args())

