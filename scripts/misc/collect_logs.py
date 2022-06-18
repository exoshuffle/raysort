import glob
import json
import os
from typing import Dict, List

import pandas as pd
import ray

RAY_LOG_DIR = "/tmp/ray/session_latest/logs"


def parse_log_file(filename: str) -> List[Dict]:
    worker_ip = ray.util.get_node_ip_address()
    worker_pid = filename.split("-")[-1].split(".")[0]
    worker = f"{worker_ip}:{worker_pid}"
    ret = []
    with open(filename) as fin:
        for line in fin.readlines():
            try:
                data = json.loads(line)
                assert isinstance(data, dict), data
                data["worker"] = worker
                ret.append(data)
            except json.JSONDecodeError:
                pass
    return ret


@ray.remote
def parse_logs() -> List[Dict]:
    ret = []
    for file in glob.glob(os.path.join(RAY_LOG_DIR, "io_worker-*.out")):
        data = parse_log_file(file)
        ret.extend(data)
    return ret


def load_object_ref_mapping() -> Dict[str, str]:
    ret = {}
    with open("/tmp/raysort-latest-objects.txt") as fin:
        for line in fin.readlines():
            ref, name = line.strip().split()
            ret[ref] = name
    return ret


def main():
    ray.init("auto")
    # 1. Load the object ref mapping.
    ref_mapping = load_object_ref_mapping()
    # 2. Parse log files on each worker node.
    node_resources = [r for r in ray.cluster_resources()
                      if r.startswith("node:")]
    tasks = [
        parse_logs.options(resources={node: 1e-3}).remote() for node in node_resources
    ]
    results = ray.get(tasks)
    rows = [x for xs in results for x in xs]

    # 2. Aggregate the results and decode.
    df = pd.DataFrame(rows)
    df["objects"] = df["object_refs"].apply(
        lambda refs: [ref_mapping.get(ref, ref) for ref in refs]
    )
    df["throughput"] = df["size"] / df["duration"] / 1024 / 1024
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df.sort_values("time", inplace=True)
    print(df)
    df.to_csv("objects.csv")


if __name__ == "__main__":
    main()
