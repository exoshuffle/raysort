import logging
import math
import os
import ray
import subprocess

import numpy as np

from raysort import logging_utils
from raysort import params
from raysort import s3_utils
from raysort.sortlib import sortlib


def _get_part_path(part_id, kind="input"):
    assert kind in {"input", "output"}
    dirpath = params.DATA_DIR[kind]
    filename_fmt = params.FILENAME_FMT[kind]
    os.makedirs(dirpath, exist_ok=True)
    filename = filename_fmt.format(part_id=part_id)
    filepath = os.path.join(dirpath, filename)
    return filepath


def _get_part_key(part_id, kind="input"):
    assert kind in {"input", "output"}
    key_fmt = params.OBJECT_KEY_FMT[kind]
    key = key_fmt.format(part_id=part_id)
    return key


@ray.remote
def generate_part(part_id, size, offset):
    logging_utils.init()
    cpu_count = os.cpu_count()
    filepath = _get_part_path(part_id)
    subprocess.run(
        [params.GENSORT_PATH, f"-b{offset}", f"-t{cpu_count}", f"{size}", filepath],
        check=True,
    )
    logging.info(f"Generated input {filepath} containing {size} records")
    if params.USE_S3:
        key = _get_part_key(part_id)
        s3_utils.put_object(filepath, key)


def generate_input():
    M = params.NUM_MAPPERS
    size = math.ceil(params.TOTAL_NUM_RECORDS / M)
    offset = 0
    tasks = []
    for part_id in range(M - 1):
        tasks.append(generate_part.remote(part_id, size, offset))
        offset += size
    tasks.append(generate_part.remote(M - 1, params.TOTAL_NUM_RECORDS - offset, offset))
    ray.get(tasks)


@ray.remote
def validate_part(part_id):
    logging_utils.init()
    cpu_count = os.cpu_count()
    filepath = _get_part_path(part_id, kind="output")
    if params.USE_S3:
        key = _get_part_key(part_id, kind="output")
        data = s3_utils.get_object(key)
        with open(filepath, "wb") as fout:
            fout.write(data)
    if os.path.getsize(filepath) == 0:
        logging.info(f"Validated output {filepath} (empty)")
        return
    proc = subprocess.run(
        [params.VALSORT_PATH, f"-t{cpu_count}", filepath], capture_output=True
    )
    if proc.returncode != 0:
        logging.critical("\n" + proc.stderr.decode("ascii"))
        raise RuntimeError(f"VALIDATION FAILED for partition {part_id}")
    logging.info(f"Validated output {filepath}")


def validate_output():
    tasks = []
    for part_id in range(params.NUM_REDUCERS):
        tasks.append(validate_part.remote(part_id))
    ray.get(tasks)


def load_partition(part_id):
    if params.USE_S3:
        return _load_partition_s3(part_id)
    else:
        return _load_partition_local(part_id)


def _load_partition_local(part_id):
    filepath = _get_part_path(part_id)
    ret = np.fromfile(filepath, dtype=sortlib.RecordT)
    return ret


def _load_partition_s3(part_id):
    key = _get_part_key(part_id)
    data = s3_utils.get_object(key)
    ret = np.frombuffer(data, dtype=sortlib.RecordT)
    ret = np.array(ret)
    return ret


def save_partition(part_id, data):
    if params.USE_S3:
        return _save_partition_s3(part_id, data)
    else:
        return _save_partition_local(part_id, data)


def _save_partition_local(part_id, data):
    filepath = _get_part_path(part_id, kind="output")
    data.tofile(filepath)


def _save_partition_s3(part_id, data):
    key = _get_part_key(part_id, kind="output")
    data = data.tobytes()
    s3_utils.put_object(data, key)
