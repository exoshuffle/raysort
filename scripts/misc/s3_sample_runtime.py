"""
Scripts for runtime analysis of s3 sampling
"""

import concurrent.futures as cf
import logging
import time

import numpy as np
import ray

from raysort import (
    config,
    constants,
    logging_utils,
    ray_utils,
    s3_utils,
    sort_utils,
    tracing_utils,
)
from raysort.config import AppConfig, JobConfig
from raysort.typing import PartInfo


def init(job_cfg: JobConfig) -> ray.actor.ActorHandle:
    logging_utils.init()
    logging.info("Job config: %s", job_cfg.name)
    ray_utils.init(job_cfg)
    sort_utils.init(job_cfg.app)
    return tracing_utils.create_progress_tracker(job_cfg)


# def _get_single_sample(cfg: AppConfig, pinfo: PartInfo, idx: int) -> np.ndarray:
#     offset = idx * constants.RECORD_SIZE
#     if cfg.s3_buckets:
#         sample_bytes = s3_utils.get_object_range(pinfo, (offset, constants.KEY_SIZE))
#         return np.frombuffer(sample_bytes, dtype=">u8")
#     return np.fromfile(
#         pinfo.path, dtype=np.uint8, offset=offset, count=constants.KEY_SIZE
#     ).view(">u8")


def _get_single_sample_shared(
    client, cfg: AppConfig, pinfo: PartInfo, idx: int
) -> np.ndarray:
    offset = idx * constants.RECORD_SIZE
    sample_bytes = s3_utils.get_object_range(
        client, pinfo, (offset, constants.KEY_SIZE)
    )
    return np.frombuffer(sample_bytes, dtype=">u8")


# sample one point from 20 files
@ray.remote
def get_partition_sample_from_mmany(
    cfg: AppConfig, parts: list[PartInfo]
) -> np.ndarray:
    with tracing_utils.timeit("sample_20_from_one_part"):
        total_num_partitions = len(parts)
        partition_indices = np.random.randint(
            total_num_partitions, size=cfg.num_samples_per_partition
        )

        client = s3_utils.client()

        with cf.ThreadPoolExecutor() as executor:
            futures = []
            for part_index in partition_indices:
                total_num_records = constants.bytes_to_records(cfg.input_part_size)
                idx = np.random.randint(total_num_records)
                futures.append(
                    executor.submit(
                        _get_single_sample_shared, client, cfg, parts[part_index], idx
                    )
                )
            results = [f.result() for f in futures]
            return np.concatenate(results)


# sample 20 points from 1 file
@ray.remote
def get_partition_sample(cfg: AppConfig, pinfo: PartInfo) -> np.ndarray:
    with tracing_utils.timeit("sample_partition"):
        total_num_records = constants.bytes_to_records(cfg.input_part_size)
        indices = np.random.randint(
            total_num_records, size=cfg.num_samples_per_partition
        )

        client = s3_utils.client()

        with cf.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(_get_single_sample_shared, client, cfg, pinfo, idx)
                for idx in indices
            ]
            results = [f.result() for f in futures]
            return np.concatenate(results)


def _get_key_sample(cfg: AppConfig, parts: list[PartInfo]) -> np.ndarray:
    logging.info(
        "Sampling %d data points each from %d partitions",
        cfg.num_samples_per_partition,
        1,
    )
    # pick a random partition to sample from
    p_idx = np.random.randint(len(parts))
    samples = ray.get(get_partition_sample.remote(cfg, parts[p_idx]))
    return samples


def main():
    job_cfg = config.get()
    tracker = init(job_cfg)
    cfg = job_cfg.app

    parts = sort_utils.load_manifest(cfg)

    time.sleep(5)  # give enough time for wandb to login

    try:
        with tracing_utils.timeit("sample_all"):
            for _ in range(10):
                sample = _get_key_sample(cfg, parts)
    finally:
        ray.get(tracker.performance_report.remote())
        ray.shutdown()


if __name__ == "__main__":
    main()
