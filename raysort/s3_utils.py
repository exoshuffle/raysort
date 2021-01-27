import asyncio
import io
import logging

import aiobotocore
import boto3

from raysort import constants


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


def upload(data, object_key, region=constants.S3_REGION, bucket=constants.S3_BUCKET):
    if isinstance(data, str):
        try:
            with open(data, "rb") as filedata:
                return upload(filedata, object_key, region, bucket)
        except IOError:
            logging.error(f"Expected filename or binary stream: {data}")

    s3 = boto3.client("s3", region_name=region)
    s3.upload_fileobj(data, bucket, object_key)


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
