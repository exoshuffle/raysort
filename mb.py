import argparse
import logging
import os
import subprocess
import time

import numpy as np
import ray
import tqdm

from raysort import logging_utils
from raysort import tracing_utils


def get_args(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--total_data_size",
        default=16_000_000_000,
        type=int,
    )
    parser.add_argument(
        "--num_objects",
        default=16000,  # 1MB
        # default=16000 * 10,  # 100KB
        # default=16000 * 20,  # 50KB
        type=int,
    )
    parser.add_argument(
        "--num_objects_per_task",
        default=200,
        type=int,
    )
    parser.add_argument(
        "--object_store_memory",
        default=1 * 1024 * 1024 * 1024,
        type=int,
    )
    parser.add_argument(
        "--task_parallelism",
        default=1,
        type=int,
    )
    parser.add_argument(
        "--no_fusing",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--no_prefetching",
        default=False,
        action="store_true",
    )
    args = parser.parse_args(args, **kwargs)
    args.object_size = args.total_data_size // args.num_objects
    args.num_tasks = args.num_objects // args.num_objects_per_task
    assert args.object_size * args.num_objects_per_task < args.object_store_memory, args
    return args


@ray.remote
def consume(*xs):
    time.sleep(1)
    return sum(x.size for x in xs)


@tracing_utils.timeit("consume_all")
def consume_one_by_one(args, refs):
    task = None
    for t in tqdm.tqdm(range(args.num_tasks)):
        if task is not None:
            print(ray.get(task))
        task = consume.remote(
            *refs[t * args.num_objects_per_task : (t + 1) * args.num_objects_per_task]
        )
    print(ray.get(task))


@tracing_utils.timeit("consume_all")
def consume_all(args, refs):
    tasks = [
        consume.remote(
            *refs[t * args.num_objects_per_task : (t + 1) * args.num_objects_per_task]
        )
        for t in range(args.num_tasks)
    ]
    with tqdm.tqdm(total=len(tasks)) as pbar:
        not_ready = tasks
        while not_ready:
            _, not_ready = ray.wait(not_ready, fetch_local=False)
            pbar.update(1)
    print(ray.get(tasks))


@tracing_utils.timeit("produce_all")
def produce_all(args):
    refs = []
    for i in tqdm.tqdm(range(args.num_objects)):
        obj = np.full(args.object_size, i % 256, dtype=np.uint8)
        refs.append(ray.put(obj))
    return refs


@tracing_utils.timeit("e2e")
def microbenchmark(args):
    logging.info("Produce")
    refs = produce_all(args)

    logging.info("Dropping filesystem cache")
    subprocess.run("sudo bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'", shell=True)

    logging.info("Consume")
    consume_one_by_one(args, refs)


def init_ray(args):
    system_config = {
        "max_io_workers": 1,
        "object_spilling_threshold": 1,
    }
    if os.path.exists("/mnt/ebs0/tmp"):
        system_config.update(
            object_spilling_config='{"type":"filesystem","params":{"directory_path":["/mnt/ebs0/tmp/ray"]}}'
        )
    if args.no_fusing:
        system_config.update(
            min_spilling_size=0,
        )
    if args.no_prefetching:
        system_config.update(
            max_object_pull_fraction=0,
        )
    logging.info(system_config)
    ray.init(
        # 1 extra CPU for the ProgressTracker actor.
        num_cpus=args.task_parallelism + 1,
        object_store_memory=args.object_store_memory,
        resources={"head": 1, "worker": 1},
        _system_config=system_config,
    )


def main(args):
    logging_utils.init()
    logging.info(args)
    init_ray(args)
    tracker = tracing_utils.create_progress_tracker(args, project="raysort-mb")
    microbenchmark(args)
    ray.get(tracker.performance_report.remote())
    logging.info(args)


if __name__ == "__main__":
    main(get_args())
