import concurrent.futures
import io
import logging
import os
import time

import boto3
import botocore
from botocore.exceptions import ClientError
import numpy as np
import ray

from raysort import constants
from raysort import monitoring_utils
from raysort import ray_utils
from raysort.types import *


def get_s3_client(region=constants.S3_REGION):
    config = botocore.config.Config(
        region_name=region,
        max_pool_connections=constants.S3_MAX_POOL_CONNECTIONS,
    )
    return boto3.client("s3", config=config)


def upload(
    data,
    object_key,
    region=constants.S3_REGION,
    bucket=constants.S3_BUCKET,
    retries=constants.S3_MAX_RETRIES,
):
    if isinstance(data, str):
        try:
            with open(data, "rb") as filedata:
                return upload(filedata, object_key, region, bucket)
        except IOError:
            logging.error(f"Expected filename or binary stream: {data}")

    s3 = get_s3_client(region)
    config = boto3.s3.transfer.TransferConfig(
        max_concurrency=constants.S3_UPLOAD_CONCURRENCY_PER_MAPPER,
    )
    while retries > 0:
        retries -= 1
        try:
            if isinstance(data, io.BytesIO):
                data.seek(0)
            s3.upload_fileobj(data, bucket, object_key, Config=config)
            return
        except ClientError as e:
            if e.response["Error"]["Code"] == "SlowDown" and retries > 0:
                t = constants.S3_SLOWDOWN_TIME
                logging.warning(f"Received AWS SlowDown error. Waiting for {t}s.")
                time.sleep(t)
            else:
                raise e


def touch_prefixes(prefixes, region=constants.S3_REGION, bucket=constants.S3_BUCKET):
    filename = "__init__"
    s3 = get_s3_client(region)
    for prefix in prefixes:
        s3.put_object(
            Bucket=bucket,
            Key=os.path.join(prefix, filename),
            Body=b"",
        )


def download(object_key, region=constants.S3_REGION, bucket=constants.S3_BUCKET):
    """
    Returns: io.BytesIO stream.
    """
    s3 = get_s3_client(region)
    ret = io.BytesIO()
    s3.download_fileobj(bucket, object_key, ret)
    ret.seek(0)
    return ret


def download_file(
    object_key, filepath, region=constants.S3_REGION, bucket=constants.S3_BUCKET
):
    """
    Returns: io.BytesIO stream.
    """
    s3 = get_s3_client(region)
    s3.download_file(bucket, object_key, filepath)
    return filepath


def download_chunks(
    reducer_id,
    chunks,
    get_chunk_info,
    region=constants.S3_REGION,
    bucket=constants.S3_BUCKET,
):
    """
    Asynchronously download all chunks.
    - chunks: a list of Ray futures of ChunkInfo.
    - Returns: a list of chunk data.
    """
    if len(chunks) == 0:
        return []
    wait_for_futures = not isinstance(chunks[0], tuple)
    resources_dict = {
        ray_utils.get_current_node_resource(): 1
        / constants.S3_DOWNLOAD_CONCURRENCY_PER_REDUCER
    }
    if wait_for_futures:
        download_tasks = []
        rest = chunks
        while len(rest) > 0:
            ready, rest = ray.wait(rest)
            chunk = ray.get(ready[0])
            chunk_info = get_chunk_info(*chunk)
            if chunk.size > 0:
                task = download_chunk.remote(chunk_info, bucket, region)
                download_tasks.append(task)
                logging.info(f"R-{reducer_id} downloading {chunk}")
    else:
        download_tasks = [
            download_chunk.options(resources_dict).remote(
                get_chunk_info(*chunk), bucket, region
            )
            for chunk in chunks
        ]
    refs = ray.get(download_tasks)
    arrs = ray.get(refs)
    return [io.BytesIO(arr.tobytes()) for arr in arrs]


@ray.remote
def download_chunk(chunk, bucket, region):
    object_key, offset, size = chunk
    end = offset + size - 1
    range_str = f"bytes={offset}-{end}"
    s3 = get_s3_client(region)
    with monitoring_utils.timeit("shuffle_download", size):
        resp = s3.get_object(
            Bucket=bucket,
            Key=object_key,
            Range=range_str,
        )
        body = resp["Body"]
        buf = body.read()
        ret = np.frombuffer(buf, dtype=np.uint8)
    return ray.put(ret)


def delete_objects_with_prefix(
    prefixes, region=constants.S3_REGION, bucket=constants.S3_BUCKET
):
    s3 = get_s3_client(region)
    bucket = s3.Bucket(bucket)
    for prefix in prefixes:
        logging.info(f"Deleting {os.path.join(prefix, '*')}")
        bucket.objects.filter(Prefix=prefix).delete()


def multipart_upload(
    dataloader,
    object_key,
    reducer_id,
    region=constants.S3_REGION,
    bucket=constants.S3_BUCKET,
):
    s3 = get_s3_client(region)
    mpu = s3.create_multipart_upload(Bucket=bucket, Key=object_key)
    mpuid = mpu["UploadId"]
    parts = []
    logging.info(f"Created multipart upload for {object_key}")

    def wait_and_process(task):
        """Wait for the upload task and add it to the result set."""
        nonlocal parts
        if task is None:
            return
        etag, part_id = task.result()
        parts.append({"ETag": etag, "PartNumber": part_id})

    def upload(datachunk, part_id):
        length = len(datachunk.getbuffer())
        logging.info(f"R-{reducer_id} uploading part {part_id} (size={length})")
        resp = s3.upload_part(
            Body=datachunk,
            Bucket=bucket,
            Key=object_key,
            UploadId=mpuid,
            PartNumber=part_id,
            ContentLength=length,
        )
        # TODO: tolerate SlowDown errors
        return resp["ETag"], part_id

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=constants.S3_MULTIPART_UPLOAD_CONCURRENCY
    ) as executor:
        upload_task = None
        for part_id, datachunk in enumerate(dataloader, start=1):
            wait_and_process(upload_task)
            upload_task = executor.submit(upload, datachunk, part_id)
        wait_and_process(upload_task)
    s3.complete_multipart_upload(
        Bucket=bucket, Key=object_key, UploadId=mpuid, MultipartUpload={"Parts": parts}
    )
    logging.info(f"R-{reducer_id} uploading complete")
    return parts


def multipart_write(dataloader, filepath, reducer_id):
    """Same interface as multipart_upload but write to local filesystem."""

    with open(filepath, "wb") as fout:
        for part_id, datachunk in enumerate(dataloader, start=1):
            buf = datachunk.getbuffer()
            logging.info(f"R-{reducer_id} writing part {part_id} (size={len(buf)})")
            fout.write(buf)
    logging.info(f"R-{reducer_id} writing complete")
