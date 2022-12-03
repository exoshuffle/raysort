import json
import logging
import os
import socket
import subprocess
import tempfile
import time
from typing import Callable

import ray
from ray import cluster_utils
from ray.remote_function import RemoteFunction

from raysort import constants
from raysort.config import AppConfig, JobConfig

local_cluster = None

KiB = 1024
MiB = KiB * 1024
GiB = MiB * 1024


def run_on_all_workers(
    cfg: AppConfig,
    fn: ray.remote_function.RemoteFunction,
    include_current: bool = False,
) -> list[ray.ObjectRef]:
    opts = [node_aff(node) for node in cfg.worker_ids]
    if include_current:
        opts.append(current_node_aff())
    return [fn.options(**opt).remote(cfg) for opt in opts]


def schedule_tasks(
    fn: Callable, task_args: list[tuple], parallelism: int = 0
) -> list[ray.ObjectRef]:
    """
    Schedule tasks with a maximum parallelism on the current node.
    """
    task = remote(fn)
    ret = []
    for i, a in enumerate(task_args):
        if parallelism > 0 and i >= parallelism:
            wait(ret, num_returns=i - parallelism + 1)
        ret.append(task.remote(*a))
    return ret


def remote(fn: Callable) -> RemoteFunction:
    """
    Return a remote function that runs on the current node with num_cpus=0.
    """
    opt = dict(num_cpus=0, **current_node_aff())
    return ray.remote(**opt)(fn)


def current_node_aff() -> dict:
    return node_aff(ray.get_runtime_context().node_id)


def node_ip_aff(cfg: AppConfig, node_ip: str) -> dict:
    assert node_ip is not None, node_ip
    return node_aff(cfg.worker_ip_to_id[node_ip])


def node_aff(node_id: ray.NodeID, *, soft: bool = False) -> dict:
    return {
        "scheduling_strategy": ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            node_id=node_id,
            soft=soft,
        )
    }


def node_i(cfg: AppConfig, node_idx: int) -> dict:
    return node_aff(cfg.worker_ids[node_idx % cfg.num_workers])


def _fail_and_restart_local_node(cfg: AppConfig):
    idx = int(cfg.fail_node)
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
    with subprocess.Popen(
        "{ssh} {worker_ip} pgrep raylet".format(ssh=ssh, worker_ip=worker_ip),
        check=True,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as p:
        out, _ = p.communicate()
        print("Raylets before kill:", out)
    with subprocess.Popen(
        "{ssh} {worker_ip} {cmd}".format(ssh=ssh, worker_ip=worker_ip, cmd=stop_cmd),
        check=True,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as p:
        _ = p.communicate()
    with subprocess.Popen(
        "{ssh} {worker_ip} pgrep raylet".format(ssh=ssh, worker_ip=worker_ip),
        check=True,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as p:
        out, _ = p.communicate()
        print("Raylets after kill:", out)
    with subprocess.Popen(
        "{ssh} {worker_ip} {cmd}".format(ssh=ssh, worker_ip=worker_ip, cmd=start_cmd),
        check=True,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as p:
        _ = p.communicate()


def fail_and_restart_node(cfg: AppConfig):
    if not cfg.fail_node:
        return
    if local_cluster is not None:
        _fail_and_restart_local_node(cfg.fail_node)
    else:
        _fail_and_restart_remote_node(cfg.fail_node)


def fail_one_node():
    resources = ray.cluster_resources()
    head_node_str = "node:" + ray.util.get_node_ip_address()
    worker_ips = [
        r.split(":")[1]
        for r in resources
        if r.startswith("node:") and r != head_node_str
    ]
    _fail_and_restart_remote_node(worker_ips[0])


@ray.remote
def sleep_before_failure():
    time.sleep(30)
    fail_one_node()


def wait(
    futures,
    wait_all: bool = False,
    soft_timeout: float = 120,
    **kwargs,
) -> tuple[list[ray.ObjectRef], list[ray.ObjectRef]]:
    to_wait = [f for f in futures if f is not None]
    if len(to_wait) == 0:
        return [], []
    kwargs_ = dict(
        fetch_local=False,
        num_returns=len(to_wait) if wait_all else kwargs.get("num_returns", 1),
        timeout=soft_timeout,
    )
    kwargs_.update(kwargs)
    num_returns = kwargs_["num_returns"]
    ready, not_ready = ray.wait(to_wait, **kwargs_)
    if len(ready) == num_returns:
        return ready, not_ready
    logging.warning(
        "Only %d/%d tasks ready in %d seconds; tasks hanging: %s",
        len(ready),
        num_returns,
        soft_timeout,
        not_ready,
    )
    return wait(
        futures,
        wait_all=wait_all,
        soft_timeout=soft_timeout,
        **kwargs,
    )


def _build_cluster(
    num_nodes: int, ray_args: dict, system_config: dict
) -> cluster_utils.Cluster:
    cluster = cluster_utils.Cluster()
    cluster.add_node(
        resources={"head": 1},
        _system_config=system_config,
        **ray_args,
    )
    cluster.connect()
    for i in range(num_nodes):
        cluster.add_node(
            resources={
                constants.WORKER_RESOURCE: 1,
                f"node:10.0.0.{i + 1}": 1,
            },
            **ray_args,
        )
    cluster.wait_for_nodes()
    return cluster


def _json_dump_no_space(data) -> str:
    return json.dumps(data, separators=(",", ":"))


def _init_local_cluster(job_cfg: JobConfig):
    system_config = {}
    if job_cfg.system.s3_spill > 0:
        system_config.update(
            **{
                "max_io_workers": job_cfg.system.s3_spill,
                "object_spilling_config": _json_dump_no_space(
                    {
                        "type": "ray_storage",
                        "params": {"buffer_size": 16 * MiB},
                    }
                ),
            }
        )
    ray_args = dict(
        num_cpus=1,
        object_store_memory=1 * GiB,
        storage=job_cfg.system.ray_storage,
    )
    num_nodes = job_cfg.cluster.instance_count
    cluster = _build_cluster(num_nodes, ray_args, system_config)
    return cluster


# TODO@(lsf): maybe move this to config.py
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


@ray.remote
def get_node_id() -> ray.NodeID:
    return ray.get_runtime_context().node_id


def _init_runtime_context(cfg: AppConfig):
    resources = ray.cluster_resources()
    logging.info("Cluster resources: %s", resources)
    assert (
        constants.WORKER_RESOURCE in resources
    ), "Ray cluster is not set up correctly: no worker resources. Did you forget `--local`?"
    cfg.num_workers = int(resources[constants.WORKER_RESOURCE])
    head_node_str = "node:" + ray.util.get_node_ip_address()
    cfg.worker_ips = [
        r.split(":")[1]
        for r in resources
        if r.startswith("node:") and r != head_node_str
    ]
    assert cfg.num_workers == len(cfg.worker_ips), cfg
    cfg.worker_ids = ray.get(
        [
            get_node_id.options(resources={f"node:{node_ip}": 1e-3}).remote()
            for node_ip in cfg.worker_ips
        ]
    )
    cfg.worker_ip_to_id = dict(zip(cfg.worker_ips, cfg.worker_ids))
    cfg.data_dirs = _get_data_dirs()


def init(job_cfg: JobConfig):
    cluster = None
    if job_cfg.cluster.local:
        cluster = _init_local_cluster(job_cfg)
    else:
        ray.init(address="auto")
    _init_runtime_context(job_cfg.app)
    return cluster
