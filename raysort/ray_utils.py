import argparse
import logging
import os
import tempfile
from typing import Dict, List, Tuple

import ray
from ray import cluster_utils


Args = argparse.Namespace


def wait(
    futures, wait_all: bool = False, **kwargs
) -> Tuple[List[ray.ObjectRef], List[ray.ObjectRef]]:
    to_wait = [f for f in futures if f is not None]
    default_kwargs = dict(fetch_local=False)
    if wait_all:
        default_kwargs.update(num_returns=len(to_wait))
    kwargs = dict(**default_kwargs, **kwargs)
    return ray.wait(to_wait, **kwargs)


def _build_cluster(
    system_config: Dict,
    num_nodes: int,
    num_cpus_per_node: int = 1,
    object_store_memory: int = 1 * 1024 * 1024 * 1024,
) -> cluster_utils.Cluster:
    cluster = cluster_utils.Cluster()
    cluster.add_node(
        resources={"head": 1},
        object_store_memory=object_store_memory,
        _system_config=system_config,
    )
    cluster.connect()
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


def _get_mount_points():
    mnt = "/mnt"
    if os.path.exists(mnt):
        ret = [os.path.join(mnt, d) for d in os.listdir(mnt) if d.startswith("nvme")]
        if len(ret) > 0:
            return ret
    return [tempfile.gettempdir()]


def _get_resources_args(args: Args):
    resources = ray.cluster_resources()
    logging.info(f"Cluster resources: {resources}")
    args.num_workers = int(resources["worker"])
    head_node_str = "node:" + ray.util.get_node_ip_address()
    args.worker_ips = [
        r.split(":")[1]
        for r in resources
        if r.startswith("node:") and r != head_node_str
    ]
    args.num_nodes = args.num_workers + 1
    assert args.num_workers == len(args.worker_ips), args
    args.mount_points = _get_mount_points()
    args.node_workmem = resources["memory"] / args.num_nodes
    args.node_objmem = resources["object_store_memory"] / args.num_nodes


def _init_local_cluster():
    system_config = {
        "max_io_workers": 1,
        "object_spilling_threshold": 1,
        # "send_unpin": True,
    }
    if os.path.exists("/mnt/nvme0/tmp"):
        system_config.update(
            object_spilling_config='{"type":"filesystem","params":{"directory_path":["/mnt/nvme0/tmp/ray"]}}'
        )
    num_nodes = os.cpu_count() // 2
    cluster = _build_cluster(system_config, num_nodes)
    return cluster


def init(args: Args):
    if args.ray_address:
        ray.init(address=args.ray_address)
        cluster = None
    else:
        cluster = _init_local_cluster()
    _get_resources_args(args)
    return cluster
