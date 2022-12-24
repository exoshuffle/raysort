'''
Fills the s3 buckets with the input gensort input
'''

import logging

import ray

from raysort import (
    config,
    logging_utils,
    ray_utils,
    sort_utils,
    tracing_utils,
)
from raysort.config import JobConfig

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
    try:
        if cfg.generate_input:
            sort_utils.generate_input(cfg)

    finally:
        ray.get(tracker.performance_report.remote())
        ray.shutdown()


if __name__ == "__main__":
    main()
