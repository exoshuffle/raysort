import argparse
import collections
import io
import itertools
import logging
import random

import botocore
import boto3
import numpy as np
import ray

from raysort import logging_utils
from raysort import monitoring_utils
from raysort import ray_utils
from raysort.typing import *

BYTES_PER_MB = 1024 * 1024
BYTES_PER_GB = BYTES_PER_MB * 1024

S3_REGION = "us-west-2"
S3_BUCKET_FMT = "raysort-benchmark-{id:04}"
S3_BUCKET_ID_LENGTH = 8
S3_NUM_BUCKETS = 32

BenchmarkConfig = collections.namedtuple(
    "BenchmarkConfig",
    [
        "num_buckets",
        "num_objects",
        "num_connections",
    ],
)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--total_data_size",
        default=16 * BYTES_PER_GB,
        type=int,
        help="total data size in bytes",
    )
    args = parser.parse_args()
    return args


def create_or_empty_bucket(s3, bucket_id):
    bucket_name = S3_BUCKET_FMT.format(id=bucket_id)
    try:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": S3_REGION},
        )
    except Exception as e:
        # logging.warning(repr(e))
        pass
    return bucket_name


def create_buckets(args, num_buckets):
    logging.info(f"Creating {num_buckets} buckets")
    s3 = boto3.client("s3", region_name=S3_REGION)
    return [
        create_or_empty_bucket(s3, random.randint(0, S3_NUM_BUCKETS))
        for _ in range(num_buckets)
    ]


@ray.remote
def upload_object(obj_size, object_key, bucket, concurrency):
    s3 = boto3.client(
        "s3",
        config=botocore.config.Config(
            region_name=S3_REGION,
            max_pool_connections=concurrency,
        ),
    )
    data = io.BytesIO(b"0" * obj_size)
    config = boto3.s3.transfer.TransferConfig(
        max_concurrency=concurrency,
    )
    s3.upload_fileobj(data, bucket, object_key, Config=config)
    return bucket, object_key


def upload_objects(args, config, buckets):
    obj_size = int(args.total_data_size / config.num_objects)
    concurrency_per_obj = int(config.num_connections / config.num_objects)
    object_keys = [f"{i}/obj-{i}-{obj_size}" for i in range(config.num_objects)]
    resources_dict = {ray_utils.get_current_node_resource(): 1 / len(object_keys)}

    with monitoring_utils.timeit(
        "upload",
        int(args.total_data_size / BYTES_PER_MB),
        {
            "num_buckets": config.num_buckets,
            "num_objects": config.num_objects,
            "num_connections": config.num_connections,
        },
    ):
        tasks = [
            upload_object.options(resources_dict).remote(
                obj_size, key, bkt, concurrency_per_obj
            )
            for key, bkt in zip(object_keys, itertools.cycle(buckets))
        ]
        return ray.get(tasks)


@ray.remote
def download_object(object_key, bucket, concurrency):
    s3 = boto3.client(
        "s3",
        config=botocore.config.Config(
            region_name=S3_REGION,
            max_pool_connections=concurrency,
        ),
    )
    config = boto3.s3.transfer.TransferConfig(
        max_concurrency=concurrency,
    )
    ret = io.BytesIO()
    s3.download_fileobj(bucket, object_key, ret, Config=config)
    ret.seek(0)
    ret = np.frombuffer(ret.getbuffer(), dtype=np.uint8)
    obj_id = ray.put(ret)
    return obj_id


def download_objects(args, config, object_paths):
    concurrency_per_obj = int(config.num_connections / config.num_objects)
    resources_dict = {ray_utils.get_current_node_resource(): 1 / len(object_paths)}

    with monitoring_utils.timeit(
        "download",
        int(args.total_data_size / BYTES_PER_MB),
        {
            "num_buckets": config.num_buckets,
            "num_objects": config.num_objects,
            "num_connections": config.num_connections,
        },
    ):
        tasks = [
            download_object.options(resources_dict).remote(
                key, bkt, concurrency_per_obj
            )
            for bkt, key in object_paths
        ]
        return ray.get(tasks)


@ray.remote(resources={"worker": 1})
def benchmark(args, config):
    logging_utils.init()
    logging.info(f"Benchmarking {config}")

    if config.num_connections % config.num_objects != 0:
        logging.info(f"Skipping: num_connections must be a multiple of num_objects")
        return None

    if config.num_objects % config.num_buckets != 0:
        logging.info(f"Skipping: num_objects must be a multiple of num_buckets")
        return None

    buckets = create_buckets(args, config.num_buckets)
    object_paths = upload_objects(args, config, buckets)
    with monitoring_utils.timeit(
        "download_and_copy",
        int(args.total_data_size / BYTES_PER_MB),
        {
            "num_buckets": config.num_buckets,
            "num_objects": config.num_objects,
            "num_connections": config.num_connections,
        },
    ):
        obj_ids = download_objects(args, config, object_paths)
        ret = ray.get(obj_ids)
        # print([len(b.getbuffer()) for b in ret])
        print([b.shape for b in ret])


def main():
    logging_utils.init()
    ray.init(address="auto")
    monitoring_utils.redis_init()
    args = get_args()

    tasks = []
    repeat = 10
    for b in range(2):
        num_buckets = 2 ** b
        for n in range(6, 11):
            num_objects = 2 ** n
            for c in range(8, 13):
                num_conns = 2 ** c
                for _ in range(repeat):
                    if num_conns % num_objects == 0 and num_objects % num_buckets == 0:
                        tasks.append(
                            benchmark.remote(
                                args,
                                BenchmarkConfig(num_buckets, num_objects, num_conns),
                            )
                        )
    ray.get(tasks)

    r = logging_utils.get_redis()
    print("upload =", monitoring_utils.get_all_datapoints(r, "upload"))
    print("download =", monitoring_utils.get_all_datapoints(r, "download"))
    print(
        "download_and_copy =",
        monitoring_utils.get_all_datapoints(r, "download_and_copy"),
    )


if __name__ == "__main__":
    main()
