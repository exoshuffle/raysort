import argparse
import asyncio
import collections
import datetime
import logging
import time

import aiobotocore
import intervaltree
import numpy as np
import pandas as pd
import ray
import wandb

from raysort import constants
from raysort import file_utils
from raysort import logging_utils
from raysort import ray_utils
from raysort import s3_utils
from raysort.types import *

BYTES_PER_MB = 1024 * 1024
GB_RECORDS = 1000 * 1000 * 10  # 1 GiB worth of records.

DownloadDataPoint = collections.namedtuple(
    "DownloadDataPoint",
    ["start_time", "end_time", "reducer_id", "duration", "bytes", "average_speed"],
)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        "--num_mappers",
        default=4,
        type=int,
        help="number of mapper workers",
    )
    parser.add_argument(
        "-r",
        "--num_reducers",
        type=int,
        help="number of reducer workers; default to num_mappers",
    )
    parser.add_argument(
        "--min_concurrency",
        default=4,
        type=int,
        help="minimum concurrency to benchmark",
    )
    parser.add_argument(
        "--records_per_mapper",
        default=int(4 * GB_RECORDS),
        type=int,
        help="total number of records = this * num_mappers",
    )
    parser.add_argument(
        "--timeseries_sampling_rate",
        default=0.5,
        type=float,
        help="sampling rate for calculating average throughput",
    )
    args = parser.parse_args()

    # Derive additional arguments.
    if args.num_reducers is None:
        args.num_reducers = args.num_mappers
    args.num_records = args.records_per_mapper * args.num_mappers
    return args


async def measure_downloads(
    reducer_id,
    chunks,
    region=constants.S3_REGION,
    bucket=constants.S3_BUCKET,
):
    """
    Asynchronously download all chunks.
    - chunks: a list of ChunkInfo.
    - Returns: a list of download timing data.
    """
    logging.info(f"R-{reducer_id} started")
    session = aiobotocore.get_session()
    async with session.create_client("s3", region_name=region) as s3:

        async def measure_download(chunk):
            start_time = time.time()
            await s3_utils.download_chunk(s3, chunk, bucket)
            end_time = time.time()
            duration = end_time - start_time
            size = chunk.size / BYTES_PER_MB
            avg_speed = size / duration
            logging.info(f"R-{reducer_id} finished downloading {chunk}")
            return DownloadDataPoint(
                start_time, end_time, reducer_id, duration, size, avg_speed
            )

        tasks = [asyncio.create_task(measure_download(chunk)) for chunk in chunks]
        return await asyncio.gather(*tasks)


@ray.remote(resources={"worker": 1})
def reducer_download(args, reducer_id):
    logging_utils.init()

    chunksize = int(args.records_per_mapper / args.num_reducers * constants.RECORD_SIZE)
    chunks = [
        file_utils.get_chunk_info(i, chunksize * i, chunksize, kind="input")
        for i in range(args.num_mappers)
    ]
    metrics = asyncio.run(measure_downloads(reducer_id, chunks))
    return metrics


def make_interval_tree(df):
    t = intervaltree.IntervalTree()
    for i, row in df.iterrows():
        t[row["start_time"] : row["end_time"]] = i
    return t


def analyze(args, concurrency, datapoints):
    df = pd.DataFrame(datapoints)
    tree = make_interval_tree(df)
    start_time = df["start_time"].min()
    end_time = df["end_time"].max()
    timepoints = np.arange(start_time, end_time, args.timeseries_sampling_rate)

    def get_throughput(t):
        idxs = [i for _, _, i in tree[t]]
        return df.loc[idxs, "average_speed"].sum()

    throughput = np.vectorize(get_throughput)(timepoints)

    data = [[t - start_time, y] for (t, y) in zip(timepoints, throughput)]
    table = wandb.Table(data=data, columns=["time", "throughput"])
    title = f"throughput/throughput-{concurrency}"
    wandb.log({title: wandb.plot.line(table, "time", "throughput", title=title)})

    peak_throughput = throughput.max()
    peak_throughput_per_node = peak_throughput / concurrency
    logging_utils.wandb_log(
        {
            "concurrency": concurrency,
            "peak_throughput": peak_throughput,
            "peak_throughput_per_node": peak_throughput_per_node,
        }
    )


def benchmark(args, concurrency):
    logging.info(f"Benchmarking with {concurrency} reducers")
    tasks = [reducer_download.remote(args, i) for i in range(concurrency)]
    results = ray.get(tasks)
    datapoints = [x for xs in results for x in xs]
    analyze(args, concurrency, datapoints)
    logging.info(f"Done benchmarking {concurrency} reducers")


def wandb_init(args):
    wandb.init(project="s3_throughput_test")
    wandb.config.update(args)


def main():
    logging_utils.init()
    ray.init(address="auto")
    args = get_args()
    wandb_init(args)
    ray_utils.request_resources(args)
    concurrency = args.num_reducers
    while concurrency >= args.min_concurrency:
        benchmark(args, concurrency)
        concurrency = int(concurrency / 2)


if __name__ == "__main__":
    main()
