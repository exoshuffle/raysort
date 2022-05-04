import io
import os
from typing import Iterable, Optional

import botocore
import boto3
from boto3.s3 import transfer
import numpy as np
import ray

from raysort import constants
from raysort import ray_utils
from raysort.typing import Args, Path

KiB = 1024
MiB = KiB * 1024

TRANSFER_CONFIG = transfer.TransferConfig(
    io_chunksize=4 * MiB,
    multipart_chunksize=32 * MiB,
)


def s3() -> botocore.client.BaseClient:
    return boto3.client(
        "s3",
        config=botocore.config.Config(
            retries={
                "max_attempts": 10,
                "mode": "adaptive",
            }
        ),
    )


def upload_s3(bucket: str, src: Path, dst: Path, *, delete_src: bool = True) -> None:
    try:
        s3().upload_file(src, bucket, dst, Config=TRANSFER_CONFIG)
    finally:
        if delete_src:
            os.remove(src)


def download_s3(
    bucket: str, src: Path, dst: Optional[Path] = None
) -> Optional[np.ndarray]:
    if dst:
        s3().download_file(bucket, src, dst)
        return
    ret = io.BytesIO()
    s3().download_fileobj(bucket, src, ret)
    return np.frombuffer(ret.getbuffer(), dtype=np.uint8)


def upload_s3_buffer(args: Args, data: np.ndarray, path: Path) -> None:
    # TODO: avoid copying
    s3().upload_fileobj(io.BytesIO(data), args.s3_bucket, path, Config=TRANSFER_CONFIG)


@ray.remote(num_cpus=0)
def upload_s3_buffer_remote(*args, **kwargs):
    return upload_s3_buffer(*args, **kwargs)


@ray.remote(num_cpus=0)
def upload_part_remote(**kwargs):
    return s3().upload_part(**kwargs)


def multipart_upload(args: Args, path: Path, merger: Iterable[np.ndarray]) -> None:
    s3 = boto3.client("s3")
    mpu = s3.create_multipart_upload(Bucket=args.s3_bucket, Key=path)
    tasks = []
    mpu_part_id = 1

    def upload_part(data):
        nonlocal mpu_part_id
        # Limit concurrency.
        max_concurrency = 2
        if len(tasks) > max_concurrency:
            ray_utils.wait(
                [t for t, _ in tasks], num_returns=len(tasks) - max_concurrency
            )
        # Upload in a different worker process.
        task = upload_part_remote.options(**ray_utils.current_node_res()).remote(
            Body=data,
            Bucket=args.s3_bucket,
            Key=path,
            PartNumber=mpu_part_id,
            UploadId=mpu["UploadId"],
        )
        tasks.append((task, mpu_part_id))
        mpu_part_id += 1

    # The merger produces a bunch of small chunks which we need to fuse into
    # one chunk before uploading to S3.
    tail = io.BytesIO()
    for datachunk in merger:
        if datachunk.size >= constants.S3_MIN_CHUNK_SIZE:
            # There should never be large chunks once we start seeing
            # small chunks towards the end.
            assert tail.getbuffer().nbytes == 0
            upload_part(datachunk.tobytes())
        else:
            tail.write(datachunk)

    if tail.getbuffer().nbytes > 0:
        tail.seek(0)
        upload_part(tail)

    # Wait for all upload tasks to complete.
    mpu_parts = [
        {
            "ETag": ray.get(t)["ETag"],
            "PartNumber": n,
        }
        for t, n in tasks
    ]

    s3.complete_multipart_upload(
        Bucket=args.s3_bucket,
        Key=path,
        MultipartUpload={"Parts": mpu_parts},
        UploadId=mpu["UploadId"],
    )
