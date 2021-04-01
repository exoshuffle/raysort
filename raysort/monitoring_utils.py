import collections
import contextlib
import json
import logging
import os
import time
import urllib.parse

import ray
import requests
import wandb

from raysort import constants
from raysort import logging_utils


def _get_service_discovery_content():
    """Return the content for Prometheus serivce discovery file."""
    nodes = ray.nodes()
    metrics_export_addresses = [
        "{}:{}".format(node["NodeManagerAddress"], constants.PROM_NODE_EXPORTER_PORT)
        for node in nodes
    ]
    return json.dumps(
        [{"labels": {"job": "node"}, "targets": metrics_export_addresses}]
    )


def update_service_discovery_file():
    # See https://github.com/ray-project/ray/blob/master/python/ray/metrics_agent.py
    # Writes to the service discovery file must be atomic.
    filepath = constants.PROM_NODE_EXPORTER_SD_FILE_PATH
    temp_filepath = filepath + ".tmp"
    with open(temp_filepath, "w") as fout:
        fout.write(_get_service_discovery_content())
    os.rename(temp_filepath, filepath)
    logging.info(f"Updated Prometheus service discovery file {filepath}")


def redis_init(start_time):
    r = logging_utils.get_redis()
    r.flushall()
    r.set("start_time", start_time)


# run as a Ray actor on the head node
class MonitoringAgent:
    def __init__(self, args):
        update_service_discovery_file()
        # launch Prom server
        self.args = args
        self.start_time = time.time()
        redis_init(self.start_time)

    def _prom_query_range(self, query, start_time, end_time, step):
        url = urllib.parse.urljoin(constants.PROM_HTTP_ENDPOINT, "query_range")
        params = {"query": query, "start": start_time, "end": end_time, "step": step}
        print(url, params)
        resp = requests.get(url, params)
        data = json.loads(resp.text)
        return data

    def _prom_time_series(self, query, start_time=None, end_time=None, step=1):
        if start_time is None:
            start_time = self.start_time
        if end_time is None:
            end_time = time.time()
        data = self._prom_query_range(query, start_time, end_time, step)
        result = data["data"]["result"]
        if len(result) == 0:
            logging.warning(
                f"Prometheus returned empty series for query: {query}, {start_time}, {end_time}"
            )
            return []

        labels = [str(d["metric"]) for d in result]
        series = [d["values"] for d in result]
        ss0 = series[0]
        sslen = len(ss0)
        for ss in series[1:]:
            assert len(ss) == sslen, ("Series have different lengths", ss, sslen)

        xs = [dp[0] - start_time for dp in ss0]
        ys = [[float(dp[1]) for dp in ss] for ss in series]

        return xs, ys, labels

    def _make_wandb_plot(self, metric, query, title=None):
        if title is None:
            title = metric
        xs, ys, labels = self._prom_time_series(query)
        return wandb.plot.line_series(xs, ys, labels, title, "time")

    def _make_wandb_plots(self, query_dict):
        return {
            metric: self._make_wandb_plot(metric, query)
            for metric, query in query_dict.items()
        }

    def submit_metrics(self):
        end_time = time.time()
        try:
            ret = self._make_wandb_plots(
                {
                    "cpu_usage": "1 - avg by (instance) (rate(node_cpu_seconds_total{mode='idle'}[2s]))",
                    "memory_usage": "1 - node_memory_MemFree_bytes / node_memory_MemTotal_bytes",
                    "network_in": "rate(node_network_receive_bytes_total{device='ens5'}[2s])",
                    "network_out": "rate(node_network_transmit_bytes_total{device='ens5'}[2s])",
                }
            )
            wandb.log(ret)
        except Exception as e:
            logging.warning("Cannot query Prometheus, skipping performance plots")
            logging.warning(repr(e))
        logging_utils.log_benchmark_result(self.args, end_time - self.start_time)
        submit_statistics()


@contextlib.contextmanager
def log_task_completed(task, task_id):
    start = time.time()
    yield
    end = time.time()
    duration = end - start
    logging_utils.log_metric(
        "task_completed",
        {
            "time": end,
            "duration": duration,
            "task": task,
            "task_id": task_id,
        },
    )


@contextlib.contextmanager
def timeit(event="operation", size=0):
    start = time.time()
    yield
    end = time.time()
    duration = end - start
    logging_utils.log_metric(
        event,
        {
            "time": end,
            "duration": duration,
            "size": size,
            "throughput": size / duration,
        },
    )


def get_all_datapoints(r, key):
    resp = r.lrange(key, 0, -1)
    return [json.loads(s.decode()) for s in resp]


def submit_histogram(r, key, attr):
    data = get_all_datapoints(r, key)
    if len(data) == 0:
        return
    attrs = list(data[0].keys())
    data = [[d[k] for k in attrs] for d in data]
    table = wandb.Table(data=data, columns=attrs)
    title = f"{key}_{attr}"
    wandb.log({title: wandb.plot.histogram(table, attr, title=title)})


def submit_task_completions(r):
    start_time = float(r.get("start_time"))
    tasks = get_all_datapoints(r, "task_completed")
    counters = {t: 0 for t in ["mapper", "reducer"]}
    for task, count in counters.items():
        wandb.log({"t": 0, task: count})
    for d in reversed(tasks):
        t = d["time"] - start_time
        task = d["task"]
        counters[task] += 1
        count = counters[task]
        wandb.log({"t": t, task: count})


def submit_statistics():
    r = logging_utils.get_redis()
    submit_histogram(r, "mapper_download", "duration")
    submit_histogram(r, "shuffle_upload", "duration")
    submit_histogram(r, "shuffle_download", "duration")


def progress_tracker(mapper_results, reducer_futs):
    logging.info("Progress tracker started")
    future_to_id = {}
    mapper_futs = mapper_results[:, 0].tolist()
    for i, fut in enumerate(mapper_futs):
        future_to_id[fut] = ("mapper", f"M-{i}")
    for i, fut in enumerate(reducer_futs):
        future_to_id[fut] = ("reducer", f"R-{i}")

    for kind in ["mapper", "reducer"]:
        logging_utils.wandb_log({f"{kind}_completed": 0})

    done_count_dict = collections.defaultdict(int)
    rest = mapper_futs + reducer_futs
    while len(rest) > 0:
        done, rest = ray.wait(rest)
        assert len(done) == 1, (done, rest)
        kind, task_id = future_to_id[done[0]]
        done_count_dict[kind] += 1
        logging_utils.wandb_log({f"{kind}_completed": done_count_dict[kind]})