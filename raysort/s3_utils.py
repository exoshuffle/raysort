import io
import os
import threading
from typing import Iterable, List, Optional

import boto3
import botocore
import numpy as np
import ray
from boto3.s3 import transfer

from raysort import constants, ray_utils, sort_utils
from raysort.config import AppConfig
from raysort.typing import PartInfo, Path

KiB = 1024
MiB = KiB * 1024


def s3() -> botocore.client.BaseClient:
    return boto3.session.Session().client(
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


def download_s3_parallel(pinfolist: List[PartInfo]) -> np.ndarray:
    io_buffers = [io.BytesIO() for _ in pinfolist]
    threads = [
        threading.Thread(
            target=download_s3,
            args=(pinfo,),
            kwargs={"buf": buf, "max_concurrency": 4},
        )
        for pinfo, buf in zip(pinfolist, io_buffers)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return np.frombuffer(
        bytearray(b"".join([buf.getbuffer() for buf in io_buffers])),
        dtype=np.uint8,
    )


def download_s3(
    pinfo: PartInfo,
    filename: Optional[Path] = None,
    buf: io.BytesIO = io.BytesIO(),
    **kwargs
) -> Optional[np.ndarray]:
    config = transfer.TransferConfig(**kwargs) if kwargs else None
    if filename:
        s3().download_file(pinfo.bucket, pinfo.path, filename, Config=config)
        return None
    s3().download_fileobj(pinfo.bucket, pinfo.path, buf, Config=config)
    return np.frombuffer(buf.getbuffer(), dtype=np.uint8)


def upload_s3_buffer(
    _cfg: AppConfig, data: np.ndarray, pinfo: PartInfo, **kwargs
) -> None:
    config = transfer.TransferConfig(**kwargs) if kwargs else None
    # TODO: avoid copying
    s3().upload_fileobj(io.BytesIO(data), pinfo.bucket, pinfo.path, Config=config)


def single_upload(
    cfg: AppConfig, pinfo: PartInfo, merger: Iterable[np.ndarray]
) -> List[PartInfo]:
    buf = io.BytesIO()
    for datachunk in merger:
        buf.write(datachunk)
    upload_s3_buffer(cfg, buf.getbuffer(), pinfo)
    return [pinfo]


def multi_upload(
    cfg: AppConfig, pinfo: PartInfo, merger: Iterable[np.ndarray]
) -> List[PartInfo]:
    parallelism = cfg.reduce_io_parallelism
    tasks = []
    chunk_id = 0

    def upload(data, chunk_id):
        sub_part_id = constants.merge_part_ids(pinfo.part_id, chunk_id, skip_places=2)
        sub_pinfo = sort_utils.part_info(cfg, sub_part_id, kind="output", s3=True)
        upload_s3_buffer(cfg, data, sub_pinfo, use_threads=False)
        return sub_pinfo

    upload_remote = ray_utils.remote(upload)

    def upload_part(data):
        nonlocal chunk_id
        if parallelism > 0 and len(tasks) > parallelism:
            ray_utils.wait(tasks, num_returns=len(tasks) - parallelism)
        if parallelism > 0:
            task = upload_remote.remote(data, chunk_id)
        else:
            task = upload(data, chunk_id)
        tasks.append(task)
        chunk_id += 1

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
        upload_part(tail.getbuffer())

    # Wait for all upload tasks to complete.
    return ray.get(tasks) if parallelism > 0 else tasks


def multipart_upload(
    cfg: AppConfig, pinfo: PartInfo, merger: Iterable[np.ndarray]
) -> List[PartInfo]:
    # TODO(@lsf) make this a flag
    # return single_upload(cfg, pinfo, merger)
    # return multi_upload(cfg, pinfo, merger)
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
    return [pinfo]
