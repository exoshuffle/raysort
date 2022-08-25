import json
import logging
import os
import pathlib
import signal
import socket
import subprocess
import time
from typing import List

import psutil
import ray


RAY_METRICS_EXPORT_PORT=8090
PROMETHEUS_NODE_EXPORTER_PORT=8091
PROMETHEUS_SERVER_PORT=9090
SCRIPT_DIR = pathlib.Path(os.path.dirname(__file__))
GRAFANA_SERVER_PORT = 3000


# Copying prometheus setup fns from cls.py to reduce unnecessary imports and
# unnecessary package installs on the VMs.
def free_port(port: int):
    for proc in psutil.process_iter():
        try:
            for conns in proc.connections(kind="inet"):
                if conns.laddr.port == port:
                    proc.send_signal(signal.SIGTERM)
        except (PermissionError, psutil.AccessDenied):
            continue

def get_prometheus_sd_content(head_ip: str, ips: List[str]) -> str:
    def get_addrs(port, include_head=True):
        return [f"{ip}:{port}" for ip in (ips if not include_head else ips + [head_ip])]

    return json.dumps(
        [
            {
                "labels": {"job": "ray"},
                "targets": get_addrs(RAY_METRICS_EXPORT_PORT),
            },
            {
                "labels": {"job": "node"},
                "targets": get_addrs(PROMETHEUS_NODE_EXPORTER_PORT, include_head=False),
            },
        ]
    )


def setup_prometheus(head_ip: str, ips: List[str]) -> None:
    prometheus_data_path = "/tmp/prometheus"
    os.makedirs(prometheus_data_path, exist_ok=True)
    with open("/tmp/prometheus/service_discovery.json", "w") as fout:
        fout.write(get_prometheus_sd_content(head_ip, ips))
    free_port(PROMETHEUS_SERVER_PORT)
    cmd = str(SCRIPT_DIR.parent / "raysort/bin/prometheus/prometheus")
    cmd += " --web.enable-admin-api"
    cmd += " --config.file=" + str(SCRIPT_DIR / "config/prometheus/prometheus.yml")
    cmd += f" --storage.tsdb.path={prometheus_data_path}"
    subprocess.Popen(cmd, shell=True)  # pylint: disable=consider-using-with


def setup_grafana() -> None:
    cwd = str(SCRIPT_DIR.parent / "raysort/bin/grafana")
    cmd = f"{cwd}/bin/grafana-server"
    free_port(GRAFANA_SERVER_PORT)
    subprocess.Popen(cmd, cwd=cwd, shell=True)  # pylint: disable=consider-using-with


def get_ips():
    resources = ray.cluster_resources()
    ips = []
    for entry in resources:
        if entry.startswith("node"):
            ips.append(entry.split(":")[1])
    return ips


def main():
    ray.init(address="auto")

    head_ip = socket.gethostbyname(socket.gethostname())
    ips = get_ips()

    setup_prometheus(head_ip, ips)
    setup_grafana()

    # Keep updating worker IPs as the cluster is autoscaled
    while True:
        print("monitoring worker IPs")
        time.sleep(60)
        ips = get_ips()
        logging.info("updated service discovery file with IPs %s", ips)
        with open("/tmp/prometheus/service_discovery.json", "w") as fout:
            fout.write(get_prometheus_sd_content(head_ip, ips))


if __name__ == "__main__":
    main()
