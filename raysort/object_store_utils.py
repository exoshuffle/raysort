import math
import os
import subprocess

import numpy as np

import logging_utils
import params
import sortlib.sortlib as sortlib

log = logging_utils.logger()


def _get_part_path(part_id, kind="input"):
    assert kind in {"input", "output"}
    dirpath = params.DATA_DIR[kind]
    filename_fmt = params.FILENAME_FMT[kind]
    os.makedirs(dirpath, exist_ok=True)
    filename = filename_fmt.format(part_id=part_id)
    filepath = os.path.join(dirpath, filename)
    return filepath


def generate_part(part_id, size, offset):
    filepath = _get_part_path(part_id)
    cpu_count = os.cpu_count()
    subprocess.run(
        [params.GENSORT_PATH, f"-b{offset}", f"-t{cpu_count}", f"{size}", filepath],
        check=True,
    )
    log.info(f"Generated input {filepath} containing {size} records")


def generate_input():
    M = params.NUM_MAPPERS
    size = math.ceil(params.TOTAL_NUM_RECORDS / M)
    offset = 0
    for part_id in range(M - 1):
        generate_part(part_id, size, offset)
        offset += size
    generate_part(M - 1, params.TOTAL_NUM_RECORDS - offset, offset)


def validate_output():
    cpu_count = os.cpu_count()
    for part_id in range(params.NUM_REDUCERS):
        filepath = _get_part_path(part_id, kind="output")
        if os.path.getsize(filepath) == 0:
            log.info(f"Validated output {filepath} (empty)")
            continue
        proc = subprocess.run(
            [params.VALSORT_PATH, f"-t{cpu_count}", filepath], capture_output=True
        )
        if proc.returncode != 0:
            log.critical("\n" + proc.stderr.decode("ascii"))
            raise RuntimeError(f"VALIDATION FAILED for partition {part_id}")
        log.info(f"Validated output {filepath}")


def prepare_input():
    generate_input()


def load_partition(part_id):
    filepath = _get_part_path(part_id)
    data = np.fromfile(filepath, dtype=sortlib.RecordT)
    return data


def save_partition(part_id, data):
    filepath = _get_part_path(part_id, kind="output")
    data.tofile(filepath)
