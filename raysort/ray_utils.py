import os

import ray
from ray import cluster_utils


def _build_cluster(
    system_config,
    num_nodes=4,
    num_cpus_per_node=1,
    object_store_memory=1 * 1024 * 1024 * 1024,
):
    cluster = cluster_utils.Cluster()
    cluster.add_node(
        resources={"head": 1},
        object_store_memory=object_store_memory,
        _system_config=system_config,
    )
    for i in range(num_nodes):
        cluster.add_node(
            resources={
                "worker": 1,
                f"node:10.0.0.{i + 1}": 1,
            },
            object_store_memory=object_store_memory,
            num_cpus=num_cpus_per_node,
        )
    cluster.wait_for_nodes()
    return cluster


def init(addr: str):
    if addr:
        ray.init(address=addr)
        return
    system_config = {
        "max_io_workers": 1,
        "object_spilling_threshold": 1,
    }
    if os.path.exists("/mnt/nvme0/tmp"):
        system_config.update(
            object_spilling_config='{"type":"filesystem","params":{"directory_path":["/mnt/nvme0/tmp/ray"]}}'
        )
    cluster = _build_cluster(system_config)
    cluster.connect()
