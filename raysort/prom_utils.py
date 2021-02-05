import json
import logging
import os

import ray

from raysort import constants


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
