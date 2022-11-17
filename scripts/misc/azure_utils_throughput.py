import time

import io
import ray
import numpy as np

from raysort import azure_utils
from typing import List

from raysort import (
    config,
    sort_utils,
)

from raysort.config import AppConfig
from raysort.typing import PartInfo

PART_SIZE = 2_000_000_000
DOWNLOAD_PART_SIZE = 1_997_200_000
NUM_TASKS = 500
CONCURRENCY: int = 10
OFFSET = 64


@ray.remote
def upload(i):
    new_i = i % OFFSET
    blob_client = azure_utils.get_blob_client("raysort-lsf", f"{new_i:03}")
    data = b"0" * PART_SIZE
    blob_client.upload_blob(data, overwrite=True, max_concurrency=CONCURRENCY)


@ray.remote
def download(i):
    new_i = i % OFFSET
    blob_client = azure_utils.get_blob_client("raysort-lsf", f"{new_i:03}")
    stream = blob_client.download_blob(max_concurrency=CONCURRENCY)
    buf = io.BytesIO()
    stream.readinto(buf)
    return np.frombuffer(buf.getbuffer(), dtype=np.uint8)


def analyze_uploads():
    print("Analyzing Azure throughput on uploads")
    begin = time.time()
    tasks = [upload.remote(i) for i in range(NUM_TASKS)]
    not_ready = tasks
    num_completed = 0
    while not_ready:
        ready, not_ready = ray.wait(not_ready)
        num_completed += len(ready)
        duration = time.time() - begin
        print(
            duration,
            "s",
            PART_SIZE * num_completed / duration / 1024 / 1024 * 8,
            "Mb/s",
        )
    return


def analyze_downloads():
    print("Analyzing Azure throughput on downloads")
    begin = time.time()
    tasks = [download.remote(i) for i in range(NUM_TASKS)]
    not_ready = tasks
    num_completed = 0
    while not_ready:
        ready, not_ready = ray.wait(not_ready)
        num_completed += len(ready)
        duration = time.time() - begin
        print(
            duration,
            "s",
            DOWNLOAD_PART_SIZE * num_completed / duration / 1024 / 1024 * 8,
            "Mb/s",
        )
    return


def main():
    analyze_uploads()
    analyze_downloads()


if __name__ == "__main__":
    main()
