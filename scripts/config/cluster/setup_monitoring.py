import logging
import socket

import ray

from scripts import cls


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

    cls.setup_prometheus(head_ip, ips)
    cls.setup_grafana()

    while True:
        cls.sleep(60, "monitoring worker IPs")
        ips = get_ips()
        logging.info("updated service discovery file with IPs ", ips)
        with open("/tmp/prometheus/service_discovery.json", "w") as fout:
            fout.write(cls.get_prometheus_sd_content(head_ip, ips))


if __name__ == "__main__":
    main()
