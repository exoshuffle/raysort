import io
import logging

import boto3

from raysort import constants


def _get_bucket(region, bucket):
    return boto3.resource("s3", region_name=region).Bucket(bucket)


def download(object_key, region=constants.S3_REGION, bucket=constants.S3_BUCKET):
    """
    Returns: io.BytesIO stream.
    """
    s3 = boto3.client("s3", region_name=region)
    ret = io.BytesIO()
    s3.download_fileobj(bucket, object_key, ret)
    return ret.getbuffer()


def upload(
    data, object_key, region=constants.S3_REGION, bucket=constants.S3_BUCKET
):
    if isinstance(data, str):
        try:
            filedata = open(data, "rb")
            return upload(filedata, object_key, region, bucket)
        except IOError:
            logging.error(f"Expected filename or binary stream: {data}")
        finally:
            filedata.close()

    s3 = boto3.client("s3", region_name=region)
    s3.upload_fileobj(data, bucket, object_key)
