import os

__DIR__ = os.path.dirname(os.path.abspath(__file__))

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

# Ray magic
# Set num_cpus to this to make sure the task will exclusively get a node.
# By default we use m5.large which has 2 CPUs.
NODE_CPUS = 2
