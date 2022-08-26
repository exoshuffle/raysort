import csv
import functools
import logging
import os
import random
import time
from typing import Callable, Dict, Iterable, List, Tuple, Union

import numpy as np
import ray

from raysort import (
    config,
    constants,
    logging_utils,
    ray_utils,
    s3_utils,
    sort_utils,
    sortlib,
    tracing_utils,
)
from raysort.config import AppConfig, JobConfig
from raysort.typing import BlockInfo, PartId, PartInfo, SpillingMode

@ray.remote
def debug_task(count):
    print("Task " + str(count) + " is running")
    with tracing_utils.timeit("task"):

        time.sleep(20 + random.randint(-1, 1))

        return 0

def sort_main(cfg: AppConfig):
    results = [debug_task.options(scheduling_strategy="DEFAULT").remote(i) for i in range(0, 200)]

def init(job_cfg: JobConfig) -> ray.actor.ActorHandle:
    logging_utils.init()
    logging.info("Job config: %s", job_cfg.name)
    ray_utils.init(job_cfg)
    sort_utils.init(job_cfg.app)
    return tracing_utils.create_progress_tracker(job_cfg)

def main():
    job_cfg = config.get()
    tracker = init(job_cfg)
    cfg = job_cfg.app

    sort_main(cfg)

    ray.get(tracker.performance_report.remote())
    ray.shutdown()

if __name__ == "__main__":
    main()