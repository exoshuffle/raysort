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


# run as a Ray actor on the head node
class MonitoringAgent:
    def __init__(self, args):
        update_service_discovery_file()
        self.args = args
        self.start_time = time.time()

    def _prom_query_range(self, query, start_time, end_time, step):
        if start_time is None:
            start_time = self.start_time
        if end_time is None:
            end_time = time.time()
        resp = requests.get(
            urllib.parse.urljoin(constants.PROM_HTTP_ENDPOINT, "query_range"),
            {"query": query, "start": start_time, "end": end_time, "step": step},
        )
        data = json.loads(resp.text)
        return data

    def _prom_time_series(self, query, start_time=None, end_time=None, step=1):
        data = self._prom_query_range(query, start_time, end_time, step)
        values = data["data"]["result"]["values"]
        return values

    def _make_wandb_plot(self, query, metric, title=None):
        if title is None:
            title = metric
        values = self._prom_time_series(query)
        x_label = "timestamp"
        y_label = metric
        table = wandb.Table(values, [x_label, y_label])
        return wandb.plot.line(table, x_label, y_label, title)

    def _make_wandb_plots(self, query_dict):
        return [
            self._make_wandb_plot(metric, query) for metric, query in query_dict.items()
        ]

    def log_metrics(self, completed=False):
        end_time = time.time()
        # try:
        #     ret = self._make_wandb_plots(
        #         {
        #             "cpu_usage": "1 - avg by (instance) (rate(node_cpu_seconds_total{mode='idle'}[2s]))",
        #             "memory_usage": "1 - node_memory_MemFree_bytes / node_memory_MemTotal_bytes",
        #             "network_in": "rate(node_network_receive_bytes_total[2s])",
        #             "network_out": "rate(node_network_transmit_bytes_total[2s])",
        #         }
        #     )
        #     wandb.log(ret)
        # except requests.exceptions.ConnectionError as e:
        #     logging.warning(e)
        #     logging.warning("Cannot query Prometheus, skipping performance plots")
        if completed:
            logging_utils.log_benchmark_result(self.args, end_time - self.start_time)
