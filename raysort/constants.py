import os

__DIR__ = os.path.dirname(os.path.abspath(__file__))

# Basics
RECORD_SIZE = 100  # bytes

# Executable locations
GENSORT_PATH = os.path.join(__DIR__, "../bin/gensort/64/gensort")
VALSORT_PATH = os.path.join(__DIR__, "../bin/gensort/64/valsort")
DATA_DIR = {"input": "/var/tmp/raysort/input/", "output": "/var/tmp/raysort/output/"}
FILENAME_FMT = {"input": "input-{part_id:08}", "output": "output-{part_id:08}"}

# Prometheus config
PROM_HTTP_ENDPOINT = "http://localhost:9090/api/v1/"
PROM_NODE_EXPORTER_SD_FILE_PATH = "/tmp/ray/prom_node_exporters.json"
PROM_NODE_EXPORTER_PORT = 8091
PROM_RAY_EXPORTER_PORT = 8090

# AWS S3 config
S3_REGION = "us-west-2"
S3_BUCKET = "raysort-debug"
S3_NUM_SHARDS = 1000
S3_UPLOAD_MAX_CONCURRENCY = 1
S3_MAX_POOL_CONNECTIONS = 64
OBJECT_KEY_FMT = {
    "input": "input/{shard_id:04}/input-{part_id:08}",
    "output": "output/{shard_id:04}/output-{part_id:08}",
    "temp": "temp/{shard_id:04}/temp-{part_id:08}",
}

# AWS EC2 constants
# Retrieved on 2/2/2021 from https://aws.amazon.com/ec2/pricing/on-demand/
EC2_HOURLY_PRICING = {
    "m5.xlarge": 0.192,
    "m5.2xlarge": 0.384,
    "m5a.xlarge": 0.172,
    "m5a.2xlarge": 0.344,
    "r5.large": 0.126,
    "r5.xlarge": 0.252,
    "r5a.large": 0.113,
    "r5a.xlarge": 0.226,
}

# Logging config
# Maximum number of items to print in an array
LOGGING_ITEMS_LIMIT = 10

# W&B logging config
WANDB_PROJECT = "raysort"
