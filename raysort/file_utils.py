import asyncio
import io
import logging
import math
import os
import ray
import subprocess

from raysort import constants
from raysort import logging_utils
from raysort import monitoring_utils
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


def _get_part_key(part_id, kind="input", prefix_only=False):
    assert kind in {"input", "output", "temp"}
    shard_id = _get_shard_id(part_id)
    key_fmt = constants.OBJECT_KEY_FMT[kind]
    key = key_fmt.format(part_id=part_id, shard_id=shard_id)
    if prefix_only:
        return key.rsplit("/", 1)[0]
    return key


@ray.remote(resources={"worker": 1})
def generate_part(part_id, size, offset, use_s3=True):
    logging_utils.init()
    cpu_count = os.cpu_count()
    filepath = _get_part_path(part_id)
    subprocess.run(
        [constants.GENSORT_PATH, f"-b{offset}", f"-t{cpu_count}", f"{size}", filepath],
        check=True,
    )
    logging.info(f"Generated input {filepath} containing {size:,} records")
    key = _get_part_key(part_id)
    if use_s3:
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
        tasks.append(
            generate_part.options(memory=memory).remote(
                part_id,
                size,
                offset,
                use_s3=args.use_s3_input,
            )
        )
        offset += size
    tasks.append(
        generate_part.options(memory=memory).remote(
            M - 1,
            args.num_records - offset,
            offset,
            use_s3=args.use_s3_input,
        )
    )
    logging.info(f"Generating {len(tasks)} partitions")
    ray.get(tasks)


@ray.remote(resources={"worker": 1})
def validate_part(part_id, use_s3=True):
    logging_utils.init()
    cpu_count = os.cpu_count()
    filepath = _get_part_path(part_id, kind="output")
    if use_s3:
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
        tasks.append(
            validate_part.remote(
                part_id,
                use_s3=args.use_s3_output,
            )
        )
    logging.info(f"Validating {len(tasks)} partitions")
    ray.get(tasks)


def load_partition(part_id, kind="input", use_s3=True):
    if use_s3:
        key = _get_part_key(part_id, kind=kind)
        with monitoring_utils.timeit("mapper_download"):
            return s3_utils.download(key)
    filepath = _get_part_path(part_id, kind)
    with open(filepath, "rb") as fin:
        return io.BytesIO(fin.read())


def save_partition(part_id, data, kind="temp"):
    key = _get_part_key(part_id, kind=kind)
    with monitoring_utils.timeit("shuffle_upload", len(data.getbuffer())):
        s3_utils.upload(data, key)
    return key


def save_partition_mpu(part_id, dataloader, kind="output", use_s3=True):
    if use_s3:
        key = _get_part_key(part_id, kind=kind)
        s3_utils.multipart_upload(dataloader, key, part_id)
        return key
    filepath = _get_part_path(part_id, kind="output")
    s3_utils.multipart_write(dataloader, filepath, part_id)


def touch_prefixes(args):
    # Touch directory prefixes to warm up S3 for higher concurrent throughput.
    prefixes = set()
    for i in range(args.num_mappers):
        prefixes.add(_get_part_key(i, kind="input", prefix_only=True))
        prefixes.add(_get_part_key(i, kind="temp", prefix_only=True))
    for i in range(args.num_reducers):
        prefixes.add(_get_part_key(i, kind="output", prefix_only=True))
    prefixes = list(prefixes)
    logging.info(f"Touching {len(prefixes)} prefixes")
    return asyncio.run(s3_utils.touch_prefixes(prefixes))


def get_chunk_info(part_id, offset, size, kind="temp"):
    return ChunkInfo(_get_part_key(part_id, kind=kind), offset, size)


def load_chunks(reducer_id, chunks):
    return asyncio.run(s3_utils.download_chunks(reducer_id, chunks, get_chunk_info))


def cleanup(args):
    logging.info(f"Cleaning up S3 files")
    return s3_utils.delete_objects_with_prefix(["temp/", "output/"])
