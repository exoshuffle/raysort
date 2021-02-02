import os

__DIR__ = os.path.dirname(os.path.abspath(__file__))

# Basics
RECORD_SIZE = 100  # bytes

# Executable locations
GENSORT_PATH = os.path.join(__DIR__, "../gensort/64/gensort")
VALSORT_PATH = os.path.join(__DIR__, "../gensort/64/valsort")
DATA_DIR = {"input": "/var/tmp/raysort/input/", "output": "/var/tmp/raysort/output/"}
FILENAME_FMT = {"input": "input-{part_id:06}", "output": "output-{part_id:06}"}

# AWS S3 config
S3_REGION = "us-west-2"
S3_BUCKET = "raysort-debug"
S3_NUM_SHARDS = 100
S3_NUM_UPLOAD_THREADS = 1
OBJECT_KEY_FMT = {
    "input": "input/input-{part_id:06}",
    "output": "output/shard-{shard_id:05}/output-{part_id:06}",
    "temp": "temp/shard-{shard_id:05}/temp-{part_id:06}",
    "temp_prefix": "temp/shard-{shard_id:05}",
}

# Logging config
# Maximum number of items to print in an array
LOGGING_ITEMS_LIMIT = 10

# W&B logging config
WANDB_PROJECT = "raysort"
