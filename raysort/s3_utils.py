import asyncio
import concurrent.futures
import io
import logging
import os

import aiobotocore
import boto3

from raysort import constants


def upload(data, object_key, region=constants.S3_REGION, bucket=constants.S3_BUCKET):
    # TODO: fault-tolerance of SlowDown errors
    if isinstance(data, str):
        try:
            with open(data, "rb") as filedata:
                return upload(filedata, object_key, region, bucket)
        except IOError:
            logging.error(f"Expected filename or binary stream: {data}")

    s3 = boto3.client("s3", region_name=region)
    config = boto3.s3.transfer.TransferConfig(
        max_concurrency=constants.S3_UPLOAD_MAX_CONCURRENCY
    )
    s3.upload_fileobj(data, bucket, object_key, Config=config)


async def touch_prefixes(
    prefixes, region=constants.S3_REGION, bucket=constants.S3_BUCKET
):
    session = aiobotocore.get_session()
    filename = "__init__"
    async with session.create_client("s3", region_name=region) as s3:
        return await asyncio.gather(
            *[
                s3.put_object(
                    Bucket=bucket,
                    Key=os.path.join(prefix, filename),
                    Body=b"",
                )
                for prefix in prefixes
            ]
        )


def download(object_key, region=constants.S3_REGION, bucket=constants.S3_BUCKET):
    """
    Returns: io.BytesIO stream.
    """
    s3 = boto3.client("s3", region_name=region)
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
    s3 = boto3.client("s3", region_name=region)
    s3.download_file(bucket, object_key, filepath)
    return filepath


async def download_chunks(
    chunks, region=constants.S3_REGION, bucket=constants.S3_BUCKET
):
    session = aiobotocore.get_session()
    async with session.create_client("s3", region_name=region) as s3:
        return await asyncio.gather(
            *[download_chunk(s3, chunk, bucket) for chunk in chunks if chunk.size > 0]
        )


async def download_chunk(s3, chunk, bucket):
    object_key, offset, size = chunk
    end = offset + size - 1
    range_str = f"bytes={offset}-{end}"
    resp = await s3.get_object(
        Bucket=bucket,
        Key=object_key,
        Range=range_str,
    )
    body = resp["Body"]
    ret = io.BytesIO(await body.read())
    ret.seek(0)
    return ret


def delete_objects_with_prefix(
    prefixes, region=constants.S3_REGION, bucket=constants.S3_BUCKET
):
    s3 = boto3.resource("s3", region_name=region)
    bucket = s3.Bucket(bucket)
    for prefix in prefixes:
        logging.info(f"Deleting {prefix}/*")
        bucket.objects.filter(Prefix=prefix).delete()


def multipart_upload(
    dataloader,
    object_key,
    reducer_id,
    region=constants.S3_REGION,
    bucket=constants.S3_BUCKET,
):
    s3 = boto3.client("s3", region_name=region)
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
        nbytes = datachunk.getbuffer().nbytes
        logging.info(f"R-{reducer_id} uploading part {part_id} (size={nbytes})")
        resp = s3.upload_part(
            Body=datachunk,
            Bucket=bucket,
            Key=object_key,
            UploadId=mpuid,
            PartNumber=part_id,
            ContentLength=nbytes,
        )
        # TODO: tolerate SlowDown errors
        return resp["ETag"], part_id

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=constants.S3_UPLOAD_MAX_CONCURRENCY
    ) as executor:
        upload_task = None
        for part_id, datachunk in enumerate(dataloader, start=1):
            wait_and_process(upload_task)
            upload_task = executor.submit(upload, datachunk, part_id)
        wait_and_process(upload_task)
    s3.complete_multipart_upload(
        Bucket=bucket, Key=object_key, UploadId=mpuid, MultipartUpload={"Parts": parts}
    )
    return parts
