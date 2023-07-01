import os

from raysort.typing import PartId, RecordCount

__DIR__ = os.path.dirname(os.path.abspath(__file__))

# Basics
KEY_SIZE = 8  # bytes
RECORD_SIZE = 100  # bytes

# Ray constants
WORKER_RESOURCE = "worker"
ACTOR_NAMESPACE = "raysort"
PROGRESS_TRACKER_ACTOR = "ProgressTrackerActor"

# Executable locations
GENSORT_PATH = os.path.join(__DIR__, "bin/gensort/64/gensort")
VALSORT_PATH = os.path.join(__DIR__, "bin/gensort/64/valsort")

# Filenames
RAY_SYSTEM_CONFIG_FILE = "_ray_config.yml"
MANIFEST_FMT = "{kind}-manifest-{suffix}.csv"
FILENAME_FMT = {
    "input": "input-{part_id:010x}",
    "output": "output-{part_id:010x}",
    "temp": "temp-{part_id:010x}",
}
SHARD_FMT = "{shard:04x}"
TMPFS_PATH = "/mnt/data0/tmp"

# S3
S3_MIN_CHUNK_SIZE = 5 * 1024 * 1024
S3_SHARD_NUMBER = 2**16  # 65536, must be a power of 2
S3_SHARD_MASK = S3_SHARD_NUMBER - 1

# Prometheus config
PROM_RAY_EXPORTER_PORT = 8090
PROM_NODE_EXPORTER_PORT = 8091


# Convenience functions
def bytes_to_records(n_bytes: float) -> RecordCount:
    return int(n_bytes / RECORD_SIZE)


def merge_part_ids(*part_ids: list[PartId], skip_places: int = 4) -> PartId:
    ret = 0
    mul = 1
    for p in reversed(part_ids):
        ret += p * mul
        mul <<= skip_places * 4
    return ret
