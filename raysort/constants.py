import os
from typing import List

from raysort.typing import ByteCount, PartId, RecordCount

__DIR__ = os.path.dirname(os.path.abspath(__file__))

# Basics
RECORD_SIZE = 100  # bytes

# Progress Tracker Actor
PROGRESS_TRACKER_ACTOR = "ProgressTrackerActor"

# Executable locations
GENSORT_PATH = os.path.join(__DIR__, "bin/gensort/64/gensort")
VALSORT_PATH = os.path.join(__DIR__, "bin/gensort/64/valsort")

# Filenames
WORK_DIR = "/tmp/raysort"
RAY_SYSTEM_CONFIG_FILE = "_ray_config.yml"
# TODO: INPUT_MANIFEST_FILE should have a s3/no-s3 suffix
INPUT_MANIFEST_FILE = os.path.join(WORK_DIR, "input-manifest.csv")
OUTPUT_MANIFEST_FILE = os.path.join(WORK_DIR, "output-manifest.csv")
FILENAME_FMT = {
    "input": "input-{part_id:010x}",
    "output": "output-{part_id:010x}",
    "temp": "temp-{part_id:010x}",
}
FILEPATH_FMT = "{prefix}/{kind}/{filename}"

# S3
S3_BUCKET = "raysort-tmp"
S3_SHARD_FACTOR = 1024  # How many objects in a prefix in S3

# Prometheus config
PROM_RAY_EXPORTER_PORT = 8090
PROM_NODE_EXPORTER_PORT = 8091


# Convenience functions
def bytes_to_records(n_bytes: ByteCount) -> RecordCount:
    assert n_bytes % RECORD_SIZE == 0
    return int(n_bytes / RECORD_SIZE)


def merge_part_ids(*part_ids: List[PartId], skip_places: int = 4) -> PartId:
    ret = 0
    mul = 1
    for p in reversed(part_ids):
        ret += p * mul
        mul *= 16**skip_places
    return ret
