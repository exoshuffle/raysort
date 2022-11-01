import io
import os
import queue
import threading
from typing import Iterable, List, Optional

import boto3
import botocore
import numpy as np
from boto3.s3 import transfer

from raysort import constants, s3_custom, sort_utils
from raysort.config import AppConfig
from raysort.typing import PartInfo, Path

CHUNK_SIZE = 10_000_000


def client() -> botocore.client.BaseClient:
    return boto3.session.Session().client(
        "s3",
        config=botocore.config.Config(
            max_pool_connections=100,
            parameter_validation=False,
            retries={
                "max_attempts": 10,
                "mode": "adaptive",
            },
        ),
    )


def get_transfer_config(**kwargs) -> transfer.TransferConfig:
    if "multipart_chunksize" not in kwargs:
        kwargs["multipart_chunksize"] = CHUNK_SIZE
    return transfer.TransferConfig(**kwargs)


def upload(src: Path, pinfo: PartInfo, *, delete_src: bool = True, **kwargs) -> None:
    config = get_transfer_config(**kwargs)
    try:
        client().upload_file(src, pinfo.bucket, pinfo.path, Config=config)
    finally:
        if delete_src:
            os.remove(src)


def download_parallel(pinfolist: List[PartInfo]) -> np.ndarray:
    io_buffers = [io.BytesIO() for _ in pinfolist]
    threads = [
        threading.Thread(
            target=download,
            args=(pinfo,),
            kwargs={"buf": buf, "max_concurrency": 2},
        )
        for pinfo, buf in zip(pinfolist, io_buffers)
    ]
    for thd in threads:
        thd.start()
    for thd in threads:
        thd.join()
    ret = bytearray()
    # TODO(@lsf): preallocate the download buffers
    for buf in io_buffers:
        ret += buf.getbuffer()
    return np.frombuffer(ret, dtype=np.uint8)


def download(
    pinfo: PartInfo,
    filename: Optional[Path] = None,
    buf: Optional[io.BytesIO] = None,
    size: Optional[int] = None,
    **kwargs,
) -> np.ndarray:
    config = get_transfer_config(**kwargs)
    if filename:
        client().download_file(pinfo.bucket, pinfo.path, filename, Config=config)
        return np.empty(0, dtype=np.uint8)
    if buf is None:
        buf = io.BytesIO()
    if size:
        s3_custom.download_fileobj(
            client(), pinfo.bucket, pinfo.path, buf, size, Config=config
        )
    else:
        client().download_fileobj(pinfo.bucket, pinfo.path, buf, Config=config)
    return np.frombuffer(buf.getbuffer(), dtype=np.uint8)


def download_sample(
    cfg: AppConfig,
    pinfo: PartInfo,
) -> np.ndarray:
    obj = client().get_object(
        Bucket=pinfo.bucket, Key=pinfo.path, Range=f"bytes=0-{cfg.sample_size}"
    )
    return np.frombuffer(obj)


def upload_s3_buffer(
    _cfg: AppConfig, data: np.ndarray, pinfo: PartInfo, **kwargs
) -> None:
    config = get_transfer_config(**kwargs)
    # TODO: avoid copying
    client().upload_fileobj(io.BytesIO(data), pinfo.bucket, pinfo.path, Config=config)


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
    upload_threads = []
    mpu_queue = queue.PriorityQueue()
    chunk_id = 0

    def upload(data, chunk_id):
        sub_part_id = constants.merge_part_ids(pinfo.part_id, chunk_id, skip_places=2)
        sub_pinfo = sort_utils.part_info(cfg, sub_part_id, kind="output", cloud=True)
        upload_s3_buffer(cfg, data, sub_pinfo, use_threads=False)
        mpu_queue.put(sub_pinfo)

    def upload_part(data):
        nonlocal chunk_id
        if len(upload_threads) >= parallelism > 0:
            upload_threads.pop(0).join()
        if parallelism > 0:
            thd = threading.Thread(target=upload, args=(data, chunk_id))
            thd.start()
            upload_threads.append(thd)
        else:
            upload(data, chunk_id)
        chunk_id += 1

    # The merger produces a bunch of small chunks towards the end, which
    # we need to fuse into one chunk before uploading.
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
    for thd in upload_threads:
        thd.join()

    return list(mpu_queue.queue)


def multipart_upload(
    cfg: AppConfig, pinfo: PartInfo, merger: Iterable[np.ndarray]
) -> List[PartInfo]:
    # TODO(@lsf) make this a flag
    # return single_upload(cfg, pinfo, merger)
    # return multi_upload(cfg, pinfo, merger)
    parallelism = cfg.reduce_io_parallelism
    s3_client = client()
    mpu = s3_client.create_multipart_upload(Bucket=pinfo.bucket, Key=pinfo.path)
    upload_threads = []
    mpu_queue = queue.PriorityQueue()
    mpu_part_id = 1

    def upload(**kwargs):
        resp = s3_client.upload_part(**kwargs)
        mpu_queue.put((kwargs["PartNumber"], resp))

    def upload_part(data):
        nonlocal mpu_part_id
        if len(upload_threads) >= parallelism > 0:
            upload_threads.pop(0).join()
        kwargs = dict(
            Body=data,
            Bucket=pinfo.bucket,
            Key=pinfo.path,
            PartNumber=mpu_part_id,
            UploadId=mpu["UploadId"],
        )
        if parallelism > 0:
            thd = threading.Thread(target=upload, kwargs=kwargs)
            thd.start()
            upload_threads.append(thd)
        else:
            upload(**kwargs)
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
    for thd in upload_threads:
        thd.join()

    mpu_parts = []
    while not mpu_queue.empty():
        mpu_id, resp = mpu_queue.get()
        mpu_parts.append({"ETag": resp["ETag"], "PartNumber": mpu_id})

    s3_client.complete_multipart_upload(
        Bucket=pinfo.bucket,
        Key=pinfo.path,
        MultipartUpload={"Parts": mpu_parts},
        UploadId=mpu["UploadId"],
    )
    return [pinfo]
