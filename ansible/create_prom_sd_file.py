import json
import os

from absl import app
from absl import flags
import ray

SERVICE_DISCOVERY_FILE_PATH = "/tmp/raysort/prom_service_discovery.json"
PROM_NODE_EXPORTER_PORT = 8091


FLAGS = flags.FLAGS
flags.DEFINE_integer(
    "expected_num_nodes",
    0,
    "expected number of nodes",
)


def get_sd_content(expected_num_nodes: int) -> str:
    nodes = ray.nodes()
    assert len(nodes) >= expected_num_nodes, (len(nodes), expected_num_nodes)

    def get_addrs(port=None):
        return [
            "{}:{}".format(
                node["NodeManagerAddress"], port if port else node["MetricsExportPort"]
            )
            for node in nodes
            if node["alive"] is True
        ]

    return json.dumps(
        [
            {
                "labels": {"job": "ray"},
                "targets": get_addrs(),
            },
            {
                "labels": {"job": "node"},
                "targets": get_addrs(PROM_NODE_EXPORTER_PORT),
            },
        ]
    )


def create_sd_file(expected_num_nodes: int = 0):
    dirname = os.path.dirname(SERVICE_DISCOVERY_FILE_PATH)
    os.makedirs(dirname, exist_ok=True)
    tmp_filename = f"{SERVICE_DISCOVERY_FILE_PATH}.swp"
    content = get_sd_content(expected_num_nodes)
    with open(tmp_filename, "w") as json_file:
        json_file.write(content)
    # NOTE: os.replace is atomic on both Linux and Windows, so Prometheus won't
    # have race condition reading this file.
    os.replace(tmp_filename, SERVICE_DISCOVERY_FILE_PATH)
    print(content)
    print(f"Created {SERVICE_DISCOVERY_FILE_PATH}")


def main(argv):
    del argv  # Unused.
    ray.init("auto")
    create_sd_file(FLAGS.expected_num_nodes)


if __name__ == "__main__":
    app.run(main)
