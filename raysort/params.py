# Basics.
RECORD_SIZE = 100

# Cluster config.
NUM_MAPPERS = 1
NUM_REDUCERS = 1

# Input config.
GENSORT_PATH = "../gensort/64/gensort"
VALSORT_PATH = "../gensort/64/valsort"
DATA_DIR = {"input": "/var/tmp/raysort/input/", "output": "/var/tmp/raysort/output/"}
FILENAME_FMT = {"input": "input-{part_id}", "output": "output-{part_id}"}

# Each record is 100 bytes. Official requirement is 10^12 records (100 TiB).
TOTAL_NUM_RECORDS = 1000 * 10  # 1 MiB
