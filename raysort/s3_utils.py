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
from raysort.config import AppConfig
from raysort.typing import Path

KiB = 1024
MiB = KiB * 1024


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
        s3().upload_file(src, bucket, dst)
    finally:
        if delete_src:
            os.remove(src)


def download_s3(
    bucket: str, src: Path, dst: Optional[Path] = None, **kwargs
) -> Optional[np.ndarray]:
    config = transfer.TransferConfig(**kwargs) if kwargs else None
    if dst:
        s3().download_file(bucket, src, dst, Config=config)
        return
    ret = io.BytesIO()
    s3().download_fileobj(bucket, src, ret, Config=config)
    return np.frombuffer(ret.getbuffer(), dtype=np.uint8)


def upload_s3_buffer(cfg: AppConfig, data: np.ndarray, path: Path, **kwargs) -> None:
    config = transfer.TransferConfig(**kwargs) if kwargs else None
    # TODO: avoid copying
    s3().upload_fileobj(io.BytesIO(data), cfg.s3_bucket, path, Config=config)


def multipart_upload(cfg: AppConfig, path: Path, merger: Iterable[np.ndarray]) -> None:
    parallelism = cfg.reduce_io_parallelism
    s3_client = boto3.client("s3")
    mpu = s3_client.create_multipart_upload(Bucket=cfg.s3_bucket, Key=path)
    tasks = []
    mpu_part_id = 1

    def upload(**kwargs):
        # Cannot use s3_client because Ray cannot pickle SSLContext.
        return s3().upload_part(**kwargs)

    upload_remote = ray_utils.remote(upload)

    def upload_part(data):
        nonlocal mpu_part_id
        if parallelism > 0 and len(tasks) > parallelism:
            ray_utils.wait([t for t, _ in tasks], num_returns=len(tasks) - parallelism)
        kwargs = dict(
            Body=data,
            Bucket=cfg.s3_bucket,
            Key=path,
            PartNumber=mpu_part_id,
            UploadId=mpu["UploadId"],
        )
        if parallelism > 0:
            task = upload_remote.remote(**kwargs)
        else:
            task = upload(**kwargs)
        tasks.append((task, mpu_part_id))
        mpu_part_id += 1

    # The merger produces a bunch of small chunks towards the end, which
    # we need to fuse into one chunk before uploading to S3.
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
            "ETag": ray.get(t)["ETag"] if isinstance(t, ray.ObjectRef) else t["ETag"],
            "PartNumber": mpid,
        }
        for t, mpid in tasks
    ]

    s3_client.complete_multipart_upload(
        Bucket=cfg.s3_bucket,
        Key=path,
        MultipartUpload={"Parts": mpu_parts},
        UploadId=mpu["UploadId"],
    )
