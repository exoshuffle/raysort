import asyncio
import time

from absl import app
from absl import flags
import aiobotocore
import boto3
import pandas as pd

import raysort.params as params

FLAGS = flags.FLAGS

flags.DEFINE_integer("concurrency", 10000, "number of download threads", lower_bound=1)
flags.DEFINE_integer(
    "chunksize", 1000, "number of bytes to download from S3", lower_bound=1
)
flags.DEFINE_float(
    "window_step",
    0.1,
    "window step size in seconds to calculate moving average of request per second",
)
flags.DEFINE_string(
    "output_file_fmt", "time-{concurrency}.csv", "output file format string"
)


async def make_request(s3, start=0, end=1000):
    assert isinstance(start, int)
    assert isinstance(end, int)
    start_time = time.time()
    resp = await s3.get_object(
        Bucket=params.S3_BUCKET,
        Key="input/input-000000",
        Range=f"bytes={start}-{end}",
    )
    buf = await resp["Body"].read()
    end_time = time.time()
    bytes_read = len(buf)
    return start_time, end_time, bytes_read


def analyze(data):
    df = pd.DataFrame(data, columns=["start_time", "end_time", "bytes_read"])
    df.sort_values(by="start_time")
    output_file = FLAGS.output_file_fmt.format(concurrency=FLAGS.concurrency)
    df.to_csv(output_file, index=False)

    window_size = 1  # seconds
    window_start = df.iloc[0]["start_time"] - window_size
    end_time = df.iloc[-1]["start_time"]
    rps_series = []
    while window_start <= end_time:
        rps = count_requests(df, window_start, window_start + window_size)
        rps_series.append(rps)
        window_start += FLAGS.window_step
    return rps_series


def count_requests(df, window_start, window_end):
    return sum(
        df.apply(
            lambda row: row["start_time"] >= window_start
            and row["start_time"] <= window_end,
            axis=1,
        )
    )


async def async_main():
    print("Hello")
    sess = aiobotocore.get_session()
    tasks = []
    async with sess.create_client("s3", region_name=params.S3_REGION) as s3:
        for i in range(FLAGS.concurrency):
            start = i * FLAGS.chunksize
            end = start + FLAGS.chunksize - 1
            tasks.append(make_request(s3, start, end))
        ret = await asyncio.gather(*tasks)

    rps = analyze(ret)
    print(rps)


def main(argv):
    asyncio.run(async_main())


if __name__ == "__main__":
    app.run(main)
