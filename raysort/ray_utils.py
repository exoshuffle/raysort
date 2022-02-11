import argparse
import logging
import os
import socket
import subprocess
import tempfile
from typing import Dict, List, Tuple

import ray
from ray import cluster_utils


Args = argparse.Namespace

local_cluster = None


def _fail_and_restart_local_node(args: Args):
    idx = int(args.fail_node)
    worker_node = list(local_cluster.worker_nodes)[idx]
    resource_spec = worker_node.get_resource_spec()
    print("Killing worker node", worker_node, resource_spec)
    local_cluster.remove_node(worker_node)
    local_cluster.add_node(
        resources=resource_spec.resources,
        object_store_memory=resource_spec.object_store_memory,
        num_cpus=resource_spec.num_cpus,
    )


def _fail_and_restart_remote_node(worker_ip: str):
    # TODO(lsf): Can get a worker IP address directly from ray.node()
    # without having to specify. Also might be able to make this more
    # elegant using resource_spec.
    # Expect a specific worker IP in this case
    assert worker_ip != ""
    # Use subprocess to ssh and stop/start a worker.
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    object_store = 28 * 1024 * 1024 * 1024
    python_dir = "~/miniconda3/envs/raysort/bin"
    start_cmd = """{python_dir}/ray start --address={local_ip}:6379
            --object-manager-port=8076
            --metrics-export-port=8090
            --resources={{\"node:{worker_ip}\": 1}}
            --object-store-memory={obj_st_mem}""".format(
        python_dir=python_dir,
        local_ip=local_ip,
        worker_ip=worker_ip,
        obj_st_mem=str(object_store),
    )
    stop_cmd = "{python_dir}/ray stop -f".format(python_dir=python_dir)
    ssh = "ssh -i ~/.aws/login-us-west-2.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    print("Killing worker node", worker_ip)
    outs, errs = subprocess.Popen(
        "{ssh} {worker_ip} pgrep raylet".format(ssh=ssh, worker_ip=worker_ip),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()
    print("Raylets before kill:", outs)
    subprocess.Popen(
        "{ssh} {worker_ip} {cmd}".format(ssh=ssh, worker_ip=worker_ip, cmd=stop_cmd),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()
    outs, errs = subprocess.Popen(
        "{ssh} {worker_ip} pgrep raylet".format(ssh=ssh, worker_ip=worker_ip),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()
    print("Raylets after kill:", outs)
    subprocess.Popen(
        "{ssh} {worker_ip} {cmd}".format(ssh=ssh, worker_ip=worker_ip, cmd=start_cmd),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()


def fail_and_restart_node(args: Args):
    if not args.fail_node:
        return
    if local_cluster is not None:
        _fail_and_restart_local_node(args.fail_node)
    else:
        _fail_and_restart_remote_node(args.fail_node)


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
        # "max_io_workers": 1,
        # Ablations:
        # "object_spilling_threshold": 1,
        "min_spilling_size": 0,
        # "send_unpin": True,
    }
    if os.path.exists("/mnt/nvme0/tmp"):
        system_config.update(
            object_spilling_config='{"type":"filesystem","params":{"directory_path":["/mnt/nvme0/tmp/ray"]}}'
        )
    # num_nodes = os.cpu_count() // 2
    num_nodes = 1
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
