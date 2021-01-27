import logging
import math
import os
import ray
import subprocess

import numpy as np

from raysort import constants
from raysort import logging_utils
from raysort import s3_utils
from raysort import sortlib


def _get_part_path(part_id, kind="input"):
    assert kind in {"input", "output"}
    dirpath = constants.DATA_DIR[kind]
    filename_fmt = constants.FILENAME_FMT[kind]
    os.makedirs(dirpath, exist_ok=True)
    filename = filename_fmt.format(part_id=part_id)
    filepath = os.path.join(dirpath, filename)
    return filepath


def _get_part_key(part_id, kind="input"):
    assert kind in {"input", "output", "temp"}
    key_fmt = constants.OBJECT_KEY_FMT[kind]
    key = key_fmt.format(part_id=part_id)
    return key


@ray.remote(resources={"worker": 1})
def generate_part(part_id, size, offset):
    logging_utils.init()
    cpu_count = os.cpu_count()
    filepath = _get_part_path(part_id)
    subprocess.run(
        [constants.GENSORT_PATH, f"-b{offset}", f"-t{cpu_count}", f"{size}", filepath],
        check=True,
    )
    logging.info(f"Generated input {filepath} containing {size:,} records")
    key = _get_part_key(part_id)
    s3_utils.upload(filepath, key)
    logging.info(f"Uploaded {filepath}")


def generate_input(args):
    M = args.num_mappers
    size = math.ceil(args.num_records / M)
    offset = 0
    tasks = []
    for part_id in range(M - 1):
        tasks.append(generate_part.remote(part_id, size, offset))
        offset += size
    tasks.append(generate_part.remote(M - 1, args.num_records - offset, offset))
    ray.get(tasks)


@ray.remote(resources={"worker": 1})
def validate_part(part_id):
    logging_utils.init()
    cpu_count = os.cpu_count()
    filepath = _get_part_path(part_id, kind="output")
    key = _get_part_key(part_id, kind="output")
    data = s3_utils.download(key)
    with open(filepath, "wb") as fout:
        fout.write(data)
    if os.path.getsize(filepath) == 0:
        logging.info(f"Validated output {filepath} (empty)")
        return
    proc = subprocess.run(
        [constants.VALSORT_PATH, f"-t{cpu_count}", filepath], capture_output=True
    )
    if proc.returncode != 0:
        logging.critical("\n" + proc.stderr.decode("ascii"))
        raise RuntimeError(f"Validation failed for partition {part_id}")
    logging.info(f"Validated output {filepath}")


def validate_output(args):
    tasks = []
    for part_id in range(args.num_reducers):
        tasks.append(validate_part.remote(part_id))
    ray.get(tasks)


def load_partition(part_id, kind="input"):
    key = _get_part_key(part_id, kind=kind)
    ret = s3_utils.download(key)
    return ret


def save_partition(part_id, data, kind="output"):
    key = _get_part_key(part_id, kind=kind)
    s3_utils.upload(data, key)
    return key


def load_chunk(part_id, offset, size, kind="temp"):
    key = _get_part_key(part_id, kind=kind)
    end = offset + size
    range_str = f"bytes={offset}-{end}"
    return s3_utils.download_range(key, range_str)


# TODO: make this async and parallel
def load_chunks(chunks, kind="temp"):
    return [load_chunk(*chunk) for chunk in chunks]
