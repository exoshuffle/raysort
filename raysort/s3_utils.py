import boto3

from raysort import logging_utils
from raysort import params

log = logging_utils.logger()


def _get_bucket(region, bucket):
    return boto3.resource("s3", region_name=region).Bucket(bucket)


def get_object(object_key, region=params.S3_REGION, bucket=params.S3_BUCKET):
    bucket = _get_bucket(region, bucket)
    obj = bucket.Object(object_key)
    body = obj.get()["Body"]
    data = body.read()
    return data


def put_object(data, object_key, region=params.S3_REGION, bucket=params.S3_BUCKET):
    if isinstance(data, str):
        try:
            data = open(data, "rb")
        except IOError:
            log.error(f"Expected filename or binary data: {data}")

    bucket = _get_bucket(region, bucket)
    obj = bucket.Object(object_key)
    obj.put(Body=data)
    obj.wait_until_exists()
    log.info(f"Put data in S3 at {object_key}")
