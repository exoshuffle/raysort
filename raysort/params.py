import os

__DIR__ = os.path.dirname(os.path.abspath(__file__))

# Cluster config
NUM_MAPPERS = 8
NUM_REDUCERS = 8

# Executable locations
GENSORT_PATH = os.path.join(__DIR__, "../gensort/64/gensort")
VALSORT_PATH = os.path.join(__DIR__, "../gensort/64/valsort")
DATA_DIR = {"input": "/var/tmp/raysort/input/", "output": "/var/tmp/raysort/output/"}
FILENAME_FMT = {"input": "input-{part_id:06}", "output": "output-{part_id:06}"}

# AWS S3 config
USE_S3 = True
S3_REGION = "us-west-1"
S3_BUCKET = "raysort-dev"
OBJECT_KEY_FMT = {
    "input": "input/input-{part_id:06}",
    "output": "output/output-{part_id:06}",
}
