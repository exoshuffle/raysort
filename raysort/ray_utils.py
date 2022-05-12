import json
import logging
import os
import socket
import subprocess
import tempfile
from typing import Callable, Dict, List, Tuple

import ray
from ray import cluster_utils

from raysort import constants
from raysort.typing import Args

local_cluster = None


def remote(args: Args, fn: Callable) -> ray.remote_function.RemoteFunction:
    """
    Returns a remote function that runs on the current node and takes
    1 / args.io_parallelism 'io_worker' resource.
    """
    opt = dict(num_cpus=0, **current_node_res())
    opt["resources"][constants.IO_WORKER_RESOURCE] = 1 / args.io_parallelism
    return ray.remote(**opt)(fn)


def current_node_res(parallelism: int = 1000) -> Dict:
    return node_res(ray.util.get_node_ip_address(), parallelism)


def node_res(node_ip: str, parallelism: int = 1000) -> Dict:
    assert node_ip is not None, node_ip
    return {"resources": {f"node:{node_ip}": 1 / parallelism}}


def node_i(args: Args, node_i: int, parallelism: int = 1000) -> Dict:
    return node_res(args.worker_ips[node_i % args.num_workers], parallelism)


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
    start_cmd = """{python_dir}/ray start --address={local_ip}:6379 \
    --object-manager-port=8076 \
    --metrics-export-port=8090 \
    --resources=\\'{{\\"node:{worker_ip}\\": 1}}\\' \
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
    futures,
    wait_all: bool = False,
    soft_timeout: float = 120,
    **kwargs,
) -> Tuple[List[ray.ObjectRef], List[ray.ObjectRef]]:
    to_wait = [f for f in futures if f is not None]
    if len(to_wait) == 0:
        return [], []
    kwargs_ = dict(
        fetch_local=False,
        num_returns=len(to_wait) if wait_all else 1,
        timeout=soft_timeout,
    )
    kwargs_.update(kwargs)
    num_returns = kwargs_["num_returns"]
    ready, not_ready = ray.wait(to_wait, **kwargs_)
    if len(ready) == num_returns:
        return ready, not_ready
    logging.warning(
        f"Only {len(ready)}/{num_returns} tasks ready in {soft_timeout} seconds; "
        f"tasks hanging: {not_ready}"
    )
    return wait(
        futures,
        wait_all=wait_all,
        soft_timeout=soft_timeout,
        **kwargs,
    )


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
                constants.IO_WORKER_RESOURCE: 1,
                constants.WORKER_RESOURCE: 1,
                f"node:10.0.0.{i + 1}": 1,
            },
            object_store_memory=object_store_memory,
            num_cpus=num_cpus_per_node,
        )
    cluster.wait_for_nodes()
    return cluster


def _get_data_dirs():
    mnt = "/mnt"
    if os.path.exists(mnt):
        ret = sorted(
            [
                os.path.join(mnt, d, "raysort")
                for d in os.listdir(mnt)
                if d.startswith("data")
            ]
        )
        if len(ret) > 0:
            return ret
    return [tempfile.gettempdir()]


def _get_resources_args(args: Args):
    resources = ray.cluster_resources()
    logging.info(f"Cluster resources: {resources}")
    assert (
        constants.WORKER_RESOURCE in resources
    ), "Ray cluster is not set up correctly: no worker resources. Did you forget `--local`?"
    args.num_workers = int(resources[constants.WORKER_RESOURCE])
    head_node_str = "node:" + ray.util.get_node_ip_address()
    args.worker_ips = [
        r.split(":")[1]
        for r in resources
        if r.startswith("node:") and r != head_node_str
    ]
    args.num_nodes = args.num_workers + 1
    assert args.num_workers == len(args.worker_ips), args
    args.data_dirs = _get_data_dirs()
    args.node_workmem = resources["memory"] / args.num_nodes
    args.node_objmem = resources["object_store_memory"] / args.num_nodes


def _init_local_cluster(args: Args):
    system_config = {}
    if args.ray_spill_path:
        if "://" in args.ray_spill_path:
            system_config.update(
                object_spilling_config=json.dumps(
                    {"type": "smart_open", "params": {"uri": args.ray_spill_path}},
                )
            )
        else:
            system_config.update(
                object_spilling_config=json.dumps(
                    {
                        "type": "filesystem",
                        "params": {"directory_path": args.ray_spill_path},
                    },
                )
            )
    num_nodes = os.cpu_count() // 2
    cluster = _build_cluster(system_config, num_nodes)
    return cluster


def init(args: Args):
    cluster = None
    if args.local:
        cluster = _init_local_cluster(args)
    else:
        ray.init(address="auto")
    _get_resources_args(args)
    return cluster
