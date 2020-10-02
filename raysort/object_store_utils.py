import math
import os
import subprocess

import logging_utils
import params

log = logging_utils.logger()


def _get_part_path(part_id):
    filename = params.INPUT_FILE_FMT.format(part_id=part_id)
    filepath = os.path.join(params.LOCAL_INPUT_DIR, filename)
    return filepath


def generate_part(part_id, size, offset):
    filepath = _get_part_path(part_id)
    cpu_count = os.cpu_count()
    subprocess.run(
        [params.GENSORT_PATH, f"-b{offset}", f"-t{cpu_count}", f"{size}", filepath],
        check=True,
    )
    log.info(f"Generated input {filepath} containing {size} records.")


def generate_input():
    os.makedirs(params.LOCAL_INPUT_DIR, exist_ok=True)
    M = params.NUM_MAPPERS
    size = math.ceil(params.TOTAL_NUM_RECORDS / M)
    offset = 0
    for part_id in range(M - 1):
        generate_part(part_id, size, offset)
        offset += size
    generate_part(M - 1, params.TOTAL_NUM_RECORDS - offset, offset)


def prepare_input():
    generate_input()


def load_partition(part_id):
    """
    Load the input data partition from object store.
    """
    # Load from disk.
    filepath = _get_part_path(part_id)
    with open(filepath, "rb") as fin:
        data = fin.read()
    ret = bytearray(data)  # TODO: how to avoid copying?
    return ret
