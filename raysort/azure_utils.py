import io
import os
import queue
import threading
from typing import Iterable, List, Optional

import numpy as np
import requests
from azure.storage.blob import BlobClient
from requests.adapters import HTTPAdapter

from raysort import constants
from raysort.config import AppConfig
from raysort.typing import PartInfo, Path


AZURE_STORAGE_URL: str = os.getenv("AZURE_STORAGE_URL", "")
CONCURRENCY: int = 10


class BigBlockSizeHTTPAdapter(HTTPAdapter):
    def get_connection(self, url, proxies=None):
        conn = super(BigBlockSizeHTTPAdapter, self).get_connection(url, proxies)
        if not conn.conn_kw:
            conn.conn_kw = {}
        conn.conn_kw["blocksize"] = 32 * 1024
        return conn


def get_blob_client(container: str, blob: str) -> BlobClient:
    MAX_CONNECTIONS = 100
    session = requests.Session()
    adapter = BigBlockSizeHTTPAdapter(
        pool_connections=MAX_CONNECTIONS,
        pool_maxsize=MAX_CONNECTIONS,
    )
    session.mount("https://", adapter)
    return BlobClient(AZURE_STORAGE_URL, container, blob, session=session)


def upload(src: Path, pinfo: PartInfo, *, delete_src: bool = True) -> None:
    try:
        blob_client = get_blob_client(pinfo.bucket, pinfo.path)
        with open(src, "rb") as fin:
            blob_client.upload_blob(fin, overwrite=True, max_concurrency=CONCURRENCY)
    finally:
        if delete_src:
            os.remove(src)


def download(pinfo: PartInfo, filename: Optional[Path] = None) -> np.ndarray:
    blob_client = get_blob_client(pinfo.bucket, pinfo.path)
    stream = blob_client.download_blob(max_concurrency=CONCURRENCY)
    if filename:
        with open(filename, "wb") as fout:
            stream.readinto(fout)
        return np.empty(0, dtype=np.uint8)
    buf = io.BytesIO()
    stream.readinto(buf)
    return np.frombuffer(buf.getbuffer(), dtype=np.uint8)


def multipart_upload(
    cfg: AppConfig, pinfo: PartInfo, merger: Iterable[np.ndarray]
) -> List[PartInfo]:
    concurrency = CONCURRENCY
    blob_client = get_blob_client(pinfo.bucket, pinfo.path)
    upload_threads = []
    mpu_queue = queue.PriorityQueue()
    mpu_part_id = 1

    def upload(data, part_id):
        block_id = f"{part_id:04d}"
        blob_client.stage_block(
            block_id, data
        )  # this block needs to be much smaller than 100MB
        mpu_queue.put(block_id)

    def upload_part(data):
        nonlocal mpu_part_id
        if len(upload_threads) >= concurrency > 0:
            upload_threads.pop(0).join()
        args = (data, mpu_part_id)
        if concurrency > 0:
            thd = threading.Thread(target=upload, args=args)
            thd.start()
            upload_threads.append(thd)
        else:
            upload(*args)
        mpu_part_id += 1

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
        upload_part(tail)

    # Wait for all upload tasks to complete.
    for thd in upload_threads:
        thd.join()

    mpu_parts = []
    while not mpu_queue.empty():
        block_id = mpu_queue.get()
        mpu_parts.append(block_id)

    blob_client.commit_block_list(mpu_parts)
    return [pinfo]
