import io
import logging

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


def download_range(
    object_key, range_str, region=constants.S3_REGION, bucket=constants.S3_BUCKET
):
    """
    Returns: io.BytesIO stream.
    """
    s3 = boto3.client("s3", region_name=region)
    obj = s3.get_object(Bucket=bucket, Key=object_key, Range=range_str)
    body = obj["Body"]
    ret = io.BytesIO(body.read())
    ret.seek(0)
    return ret


def upload(data, object_key, region=constants.S3_REGION, bucket=constants.S3_BUCKET):
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
