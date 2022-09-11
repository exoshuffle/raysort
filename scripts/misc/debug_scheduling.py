# Script for running low cost / straightfoward tasks using default or spread ray scheduler
# in order to analyze how the ray scheduler is scheduling tasks

import logging
import random
import time

import numpy as np
import ray

from raysort import (
    config,
    logging_utils,
    ray_utils,
    sort_utils,
    tracing_utils,
)
from raysort.config import AppConfig, JobConfig
from raysort.typing import BlockInfo, PartId, PartInfo, SpillingMode


@ray.remote(resources={"worker": 0.001})
def debug_task(count):
    with tracing_utils.timeit("task"):
        print("Task " + str(count) + " is running")
        time.sleep(20 + random.randint(-1, 1))

        return 0


def sort_main(cfg: AppConfig):
    for round in range(10):
        results = [
            debug_task.options(scheduling_strategy="SPREAD").remote(i)
            for i in range(0, 40)
        ]
        ray.get(results)


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
