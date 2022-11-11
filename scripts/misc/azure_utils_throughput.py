import time

import ray

from raysort import azure_utils
from typing import List

from raysort import (
    config,
    sort_utils,
)

from raysort.config import AppConfig
from raysort.typing import PartInfo

@ray.remote
def upload():
    azure_utils.upload()

@ray.remote
def download(pinfo: PartInfo):
    azure_utils.download(pinfo)

def analyze_uploads(cfg: AppConfig, parts: List[PartInfo]):
    # TODO: reference generate input?
    return

def analyze_downloads(cfg: AppConfig, parts: List[PartInfo]):
    begin = time.time()
    tasks = [download.remote(parts[part_id]) for part_id in range(cfg.num_mappers)]
    not_ready = tasks
    num_completed = 0
    while not_ready:
        ready, not_ready = ray.wait(not_ready)
        num_completed += len(ready)
        duration = time.time() - begin
        print(duration, "s", PART_SIZE * num_completed / duration / 1024 / 1024 * 8, "Mb/s")


def main():
    job_cfg = config.get()
    cfg = job_cfg.app
    parts = sort_utils.load_manifest(cfg)
    analyze_uploads(cfg, parts)
    analyze_downloads(cfg, parts)



if __name__ == "__main__":
    main()