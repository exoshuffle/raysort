import asyncio
import logging
import math
import os
import ray
import subprocess

from raysort import constants
from raysort import logging_utils
from raysort import s3_utils
from raysort.types import *


def _get_shard_id(part_id):
    return part_id % constants.S3_NUM_SHARDS


def _get_part_path(part_id, kind="input"):
    assert kind in {"input", "output"}
    dirpath = constants.DATA_DIR[kind]
    filename_fmt = constants.FILENAME_FMT[kind]
    os.makedirs(dirpath, exist_ok=True)
    filename = filename_fmt.format(part_id=part_id)
    filepath = os.path.join(dirpath, filename)
    return filepath


def _get_part_key(part_id, kind="input"):
    assert kind in {"input", "output", "temp", "temp_prefix"}
    shard_id = _get_shard_id(part_id)
    key_fmt = constants.OBJECT_KEY_FMT[kind]
    key = key_fmt.format(part_id=part_id, shard_id=shard_id)
    return key


@ray.remote
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
    os.remove(filepath)
    logging.info(f"Uploaded {filepath}")


def generate_input(args):
    M = args.num_mappers
    size = math.ceil(args.num_records / M)
    offset = 0
    tasks = []
    memory = size * constants.RECORD_SIZE
    for part_id in range(M - 1):
        tasks.append(generate_part.options(memory=memory).remote(part_id, size, offset))
        offset += size
    tasks.append(
        generate_part.options(memory=memory).remote(
            M - 1, args.num_records - offset, offset
        )
    )
    logging.info(f"Generating {len(tasks)} partitions")
    ray.get(tasks)


@ray.remote
def validate_part(part_id):
    logging_utils.init()
    cpu_count = os.cpu_count()
    filepath = _get_part_path(part_id, kind="output")
    key = _get_part_key(part_id, kind="output")
    s3_utils.download_file(key, filepath)
    if os.path.getsize(filepath) == 0:
        logging.info(f"Validated output {filepath} (empty)")
        return
    proc = subprocess.run(
        [constants.VALSORT_PATH, f"-t{cpu_count}", filepath], capture_output=True
    )
    if proc.returncode != 0:
        logging.critical("\n" + proc.stderr.decode("ascii"))
        raise RuntimeError(f"Validation failed for partition {part_id}")
    os.remove(filepath)
    logging.info(f"Validated output {filepath}")


def validate_output(args):
    tasks = []
    for part_id in range(args.num_reducers):
        tasks.append(validate_part.remote(part_id))
    logging.info(f"Validating {len(tasks)} partitions")
    ray.get(tasks)


def load_partition(part_id, kind="input"):
    key = _get_part_key(part_id, kind=kind)
    ret = s3_utils.download(key)
    return ret


def save_partition(part_id, data, kind="output"):
    key = _get_part_key(part_id, kind=kind)
    s3_utils.upload(data, key)
    return key


def create_empty_prefixes(args):
    # Create empty prefixes to warm up S3 for higher concurrent throughput.
    prefixes = list(set([_get_part_key(i) for i in range(args.num_mappers)]))
    logging.info(f"Creating {len(prefixes)} prefixes")
    return asyncio.run(s3_utils.touch_prefixes(prefixes))


def load_chunks(chunks, kind="temp"):
    chunks = [
        ChunkInfo(_get_part_key(part_id, kind=kind), offset, size)
        for part_id, offset, size in chunks
    ]
    return asyncio.run(s3_utils.download_chunks(chunks))


def cleanup(args):
    logging.info(f"Cleaning up S3 files")
    return s3_utils.delete_objects_with_prefix(["temp/", "output/"])
