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
OBJECT_KEY_FMT = {
    "input": "input/input-{part_id:06}",
    "output": "output/output-{part_id:06}",
    "temp": "temp/temp-{part_id:06}",
}

# Ray reports memory in 50MB units
RAY_MEMORY_UNIT = 50 * 1024 * 1024
