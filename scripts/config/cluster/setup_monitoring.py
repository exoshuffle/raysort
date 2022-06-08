import logging
import ray
import socket
from scripts.cls import (
    setup_prometheus,
    setup_grafana,
    sleep,
    get_prometheus_sd_content,
)


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

    while True:
        sleep(60, "monitoring worker IPs")
        ips = get_ips()
        logging.info("updated service discovery file with IPs ", ips)
        with open("/tmp/prometheus/service_discovery.json", "w") as fout:
            fout.write(get_prometheus_sd_content(head_ip, ips))


if __name__ == "__main__":
    main()
