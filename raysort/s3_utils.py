import boto3
import logging

from raysort import constants


def _get_bucket(region, bucket):
    return boto3.resource("s3", region_name=region).Bucket(bucket)


def get_object(object_key, region=constants.S3_REGION, bucket=constants.S3_BUCKET):
    bucket = _get_bucket(region, bucket)
    obj = bucket.Object(object_key)
    body = obj.get()["Body"]
    data = body.read()
    return data


def put_object(
    data, object_key, region=constants.S3_REGION, bucket=constants.S3_BUCKET
):
    if isinstance(data, str):
        try:
            data = open(data, "rb")
        except IOError:
            logging.error(f"Expected filename or binary data: {data}")

    bucket = _get_bucket(region, bucket)
    obj = bucket.Object(object_key)
    obj.put(Body=data)
    obj.wait_until_exists()
    logging.info(f"Put data in S3 at {object_key}")
