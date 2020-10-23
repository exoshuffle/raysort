import argparse
import asyncio
import time

from absl import app
import aiobotocore
import boto3
import pandas as pd
import ray

import raysort.params as params

parser = argparse.ArgumentParser()
parser.add_argument("--nodes", default=6, help="number of nodes")
parser.add_argument(
    "--concurrency", default=1000, help="number of download threads per node"
)
parser.add_argument("--chunksize", default=1000, help="number of bytes to download")
parser.add_argument(
    "--window_step",
    default=0.1,
    help="window step size in seconds to calculate moving average of request per second",
)
parser.add_argument(
    "--output_file_fmt",
    default="time-{concurrency}.csv",
    help="output filename format string",
)
args = parser.parse_args()


@ray.remote
class RequestActor:
    def __init__(self, id):
        self.id = id
        self.object_key = "input/input-{part_id:06}".format(part_id=id)
        self.session = aiobotocore.get_session()

    async def make_request(self, s3, start=0, end=1000):
        assert isinstance(start, int)
        assert isinstance(end, int)
        start_time = time.time()
        resp = await s3.get_object(
            Bucket=params.S3_BUCKET,
            Key=self.object_key,
            Range=f"bytes={start}-{end}",
        )
        buf = await resp["Body"].read()
        end_time = time.time()
        bytes_read = len(buf)
        return start_time, end_time, bytes_read

    async def make_requests(self):
        print(f"Actor #{self.id} launching requests to {self.object_key}")
        tasks = []
        async with self.session.create_client("s3", region_name=params.S3_REGION) as s3:
            for i in range(args.concurrency):
                start = i * args.chunksize
                end = start + args.chunksize - 1
                tasks.append(self.make_request(s3, start, end))
            ret = await asyncio.gather(*tasks)
        return ret


def analyze(data):
    df = pd.DataFrame(data, columns=["start_time", "end_time", "bytes_read"])
    df.sort_values(by="start_time")
    output_file = args.output_file_fmt.format(concurrency=args.concurrency)
    df.to_csv(output_file, index=False)

    window_size = 1  # seconds
    window_start = df.iloc[0]["start_time"] - window_size
    end_time = df.iloc[-1]["start_time"]
    rps_series = []
    while window_start <= end_time:
        rps = count_requests(df, window_start, window_start + window_size)
        rps_series.append(rps)
        window_start += args.window_step
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
    # ray.init(address="3.101.105.105:6379", _redis_password="5241590000000000")
    ray.init()

    actors = [RequestActor.remote(i) for i in range(args.nodes)]
    tasks = [actor.make_requests.remote() for actor in actors]
    data = ray.get(tasks)
    data = [x for xs in data for x in xs]
    rps = analyze(data)
    print(rps)


def main(argv):
    asyncio.run(async_main())


if __name__ == "__main__":
    app.run(main)
