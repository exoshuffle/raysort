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
from raysort.typing import PartInfo, Path

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


def upload_s3(src: Path, pinfo: PartInfo, *, delete_src: bool = True, **kwargs) -> None:
    config = transfer.TransferConfig(**kwargs) if kwargs else None
    try:
        s3().upload_file(src, pinfo.bucket, pinfo.path, Config=config)
    finally:
        if delete_src:
            os.remove(src)


def download_s3(
    pinfo: PartInfo, dst: Optional[Path] = None, **kwargs
) -> Optional[np.ndarray]:
    config = transfer.TransferConfig(**kwargs) if kwargs else None
    if dst:
        s3().download_file(pinfo.bucket, pinfo.path, dst, Config=config)
        return
    ret = io.BytesIO()
    s3().download_fileobj(pinfo.bucket, pinfo.path, ret, Config=config)
    return np.frombuffer(ret.getbuffer(), dtype=np.uint8)


def upload_s3_buffer(
    cfg: AppConfig, data: np.ndarray, pinfo: PartInfo, **kwargs
) -> None:
    config = transfer.TransferConfig(**kwargs) if kwargs else None
    # TODO: avoid copying
    s3().upload_fileobj(io.BytesIO(data), pinfo.bucket, pinfo.path, Config=config)


def multipart_upload(
    cfg: AppConfig, pinfo: PartInfo, merger: Iterable[np.ndarray]
) -> None:
    parallelism = cfg.reduce_io_parallelism
    s3_client = boto3.client("s3")
    mpu = s3_client.create_multipart_upload(Bucket=pinfo.bucket, Key=pinfo.path)
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
            Bucket=pinfo.bucket,
            Key=pinfo.path,
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
        Bucket=pinfo.bucket,
        Key=pinfo.path,
        MultipartUpload={"Parts": mpu_parts},
        UploadId=mpu["UploadId"],
    )
