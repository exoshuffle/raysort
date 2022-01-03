import argparse
import io
import os

import boto3
import numpy as np
import ray


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--bucket", default="lsf-berkeley-edu")
    parser.add_argument("--prefix", default="gcptest")
    parser.add_argument("--num_objects", default=1024, type=int)
    parser.add_argument("--object_size", default=1024 * 1024 * 128, type=int)
    return parser.parse_args()


args = get_args()


@ray.remote
def upload_s3(region, bucket, object_key, object_size):
    buf = io.BytesIO(np.random.bytes(object_size))
    buf.seek(0)
    s3 = boto3.client("s3", region_name=region)
    s3.upload_fileobj(buf, bucket, object_key)
    return object_size


def main():
    ray.init()
    tasks = [
        upload_s3.remote(
            args.region,
            args.bucket,
            os.path.join(args.prefix, f"object-{i:04}"),
            args.object_size,
        )
        for i in range(args.num_objects)
    ]
    bytes_count = sum(ray.get(tasks))
    print(f"Uploaded {bytes_count} bytes to {args.bucket}")


if __name__ == "__main__":
    main()
