# Basics.
RECORD_SIZE = 100

# Cluster config.
NUM_MAPPERS = 3
NUM_REDUCERS = 2

# Input config.
GENSORT_PATH = "../gensort/64/gensort"
INPUT_FILE_FMT = "input_{part_id}"
LOCAL_INPUT_DIR = "/var/tmp/raysort/input/"

# Each record is 100 bytes. Official requirement is 10^12 records (100 TiB).
TOTAL_NUM_RECORDS = 1000 * 10  # 1 MiB
