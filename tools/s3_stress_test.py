import argparse
import asyncio
import datetime
import time

import aiobotocore
import boto3
import pandas as pd
import ray

parser = argparse.ArgumentParser()
parser.add_argument(
    "--cluster",
    action="store_true",
    help="try connecting to an existing Ray cluster",
)
parser.add_argument(
    "--export_timeline", action="store_true", help="export a Ray timeline trace"
)
parser.add_argument("--nodes", type=int, default=16, help="number of nodes")
parser.add_argument(
    "--concurrency", type=int, default=2000, help="number of download threads per node"
)
parser.add_argument(
    "--chunksize", type=int, default=1000 * 10, help="number of bytes to download"
)
parser.add_argument(
    "--window_step",
    type=float,
    default=0.1,
    help="window step size in seconds to calculate moving average of request per second",
)
parser.add_argument(
    "--output_file_fmt",
    default="time-{concurrency}.csv",
    help="output filename format string",
)
args = parser.parse_args()


# We want each actor to run on its own node. m5.large has 2 CPUs;
# Hence we say we need 2 CPUs.
@ray.remote(num_cpus=2)
class RequestActor:
    def __init__(self, id):
        self.id = id
        self.object_key = "input/input-{part_id:06}".format(part_id=id % 16)
        self.session = aiobotocore.get_session()

    async def make_request(self, s3, start=0, end=1000):
        assert isinstance(start, int)
        assert isinstance(end, int)
        start_time = time.time()
        resp = await s3.get_object(
            Bucket="raysort-debug",
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
        async with self.session.create_client("s3", region_name="us-west-2") as s3:
            for i in range(args.concurrency):
                start = i * args.chunksize
                end = start + args.chunksize - 1
                tasks.append(self.make_request(s3, start, end))
            ret = await asyncio.gather(*tasks)
        print(f"Actor #{self.id} finished")
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
    if args.cluster:
        ray.init(address="auto", _redis_password="5241590000000000")
    else:
        ray.init()

    actors = [RequestActor.remote(i) for i in range(args.nodes)]
    tasks = [actor.make_requests.remote() for actor in actors]
    data = ray.get(tasks)
    data = [x for xs in data for x in xs]
    rps = analyze(data)
    print(rps)

    if args.export_timeline:
        time.sleep(5)  # wait for traces to come back to head node
        timestr = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        filename = f"timeline-{timestr}.json"
        ray.timeline(filename=filename)
        print(f"Exported timeline to {filename}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
