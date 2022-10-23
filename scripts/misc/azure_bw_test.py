import time

import ray

from raysort import azure_utils

ray.init()

PART_SIZE = 2_000_000_000


@ray.remote
def upload(i):
    blob_client = azure_utils.get_blob_client("raysort-lsf", f"{i:03}")
    data = b"0" * PART_SIZE
    blob_client.upload_blob(data)


NUM_TASKS = 64
BATCH = 8

begin = time.time()
tasks = [upload.remote(i) for i in range(NUM_TASKS)]
not_ready = tasks
num_completed = 0
while not_ready:
    ready, not_ready = ray.wait(not_ready, num_returns=BATCH)
    num_completed += len(ready)
    duration = time.time() - begin
    print(duration, "s", PART_SIZE * num_completed / duration / 1024 / 1024 * 8, "Mb/s")
