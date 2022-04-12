"""Cluster management toolkit."""
import json
import os
import pathlib
import psutil
import signal
import shutil
import string
import subprocess
import time
from typing import Dict, List, Tuple, Union
import yaml

import boto3
import click
import ray

DEFAULT_CLUSTER_NAME = "raysort-lsf"
DEFAULT_INSTANCE_COUNT = 10
DEFAULT_INSTANCE_TYPE = "d3.2xlarge"

MNT_PATH_PATTERN = "/mnt/data*"
MNT_PATH_FMT = "/mnt/data{i}"
PARALLELISM = os.cpu_count() * 4
SCRIPT_DIR = pathlib.Path(os.path.dirname(__file__))

ANSIBLE_DIR = "config/ansible"
HADOOP_TEMPLATE_DIR = "config/hadoop"
TERRAFORM_DIR = "config/terraform"
TERRAFORM_TEMPLATE_DIR = "aws-template"
RAY_SYSTEM_CONFIG_FILE_PATH = SCRIPT_DIR.parent / "_ray_config.yml"
RAY_S3_SPILL_PATH = "s3://raysort-tmp/ray-{:03d}"

GRAFANA_SERVER_PORT = 3000
PROMETHEUS_SERVER_PORT = 9090
PROMETHEUS_NODE_EXPORTER_PORT = 8091
RAY_METRICS_EXPORT_PORT = 8090
RAY_OBJECT_MANAGER_PORT = 8076

KB = 1024
MB = KB * 1024


def error(*args, **kwargs):
    click.secho(fg="red", *args, **kwargs)
    raise RuntimeError()


def sleep(duration: float, reason: str = ""):
    msg = f"Waiting for {duration} seconds"
    if reason:
        msg += f" ({reason})"
    click.echo(msg)
    time.sleep(duration)


def run(
    cmd: str,
    *,
    echo: bool = True,
    retries: int = 0,
    time_between_retries: float = 30,
    **kwargs,
) -> subprocess.CompletedProcess:
    if echo:
        click.secho(f"> {cmd}", fg="cyan")
    try:
        return subprocess.run(cmd, shell=True, check=True, **kwargs)
    except subprocess.CalledProcessError as e:
        if retries == 0:
            raise e
        click.secho(f"> {e.cmd} failed with code {e.returncode}", fg="yellow")
        sleep(time_between_retries, f"{retries} times left")
        return run(
            cmd,
            retries=retries - 1,
            time_between_retries=time_between_retries,
            **kwargs,
        )


def run_output(cmd: str, **kwargs) -> str:
    return (
        run(cmd, stdout=subprocess.PIPE, echo=False, **kwargs)
        .stdout.decode("ascii")
        .strip()
    )


# ------------------------------------------------------------
#     Terraform and AWS
# ------------------------------------------------------------


def get_tf_dir(cluster_name: str) -> pathlib.Path:
    return SCRIPT_DIR / TERRAFORM_DIR / ("_" + cluster_name)


def get_instances(filters: Dict[str, str]) -> List[Dict]:
    ec2 = boto3.client("ec2")
    paginator = ec2.get_paginator("describe_instances")
    ret = []
    for page in paginator.paginate(
        Filters=[{"Name": k, "Values": v} for k, v in filters.items()]
    ):
        ret.extend(page["Reservations"])
    return [item["Instances"][0] for item in ret]


def check_cluster_existence(cluster_name: str, raise_if_exists: bool = False) -> bool:
    instances = get_instances(
        {
            "tag:ClusterName": [cluster_name],
            # Excluding the "Terminated" state (0x30).
            # https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html
            "instance-state-code": [
                str(code) for code in [0x00, 0x10, 0x20, 0x40, 0x50]
            ],
        }
    )
    cnt = len(instances)
    ret = cnt > 0
    if raise_if_exists and ret:
        error(f"{cluster_name} must not exist (found {cnt} instances)")
    return ret


def get_terraform_vars(**kwargs: Dict) -> str:
    return "".join([f' -var="{k}={v}"' for k, v in kwargs.items()])


def get_or_create_tf_dir(cluster_name: str, must_exist: bool = False) -> pathlib.Path:
    tf_dir = get_tf_dir(cluster_name)
    if os.path.exists(tf_dir):
        if not must_exist:
            click.echo(f"Found existing configuration for {cluster_name}")
        return tf_dir
    elif must_exist:
        raise FileNotFoundError(f"Cluster configuration does not exist {tf_dir}")

    template_dir = SCRIPT_DIR / TERRAFORM_DIR / TERRAFORM_TEMPLATE_DIR
    assert not os.path.exists(tf_dir), f"{tf_dir} must not exist"
    assert os.path.exists(template_dir), f"{template_dir} must exist"
    shutil.copytree(template_dir, tf_dir)
    click.echo(f"Created configuration directory for {cluster_name}")
    click.echo(f"You can manually edit {tf_dir}/main.tf and then run this launcher")
    return tf_dir


def terraform_provision(
    cluster_name: str, instance_count: int, instance_type: str
) -> None:
    tf_dir = get_or_create_tf_dir(cluster_name)
    run("terraform init", cwd=tf_dir)
    cmd = "terraform apply -auto-approve" + get_terraform_vars(
        cluster_name=cluster_name,
        instance_count=instance_count,
        instance_type=instance_type,
    )
    run(cmd, cwd=tf_dir)


def get_tf_output(
    cluster_name: str, key: Union[str, List[str]]
) -> Union[List[str], List[List[str]]]:
    tf_dir = get_or_create_tf_dir(cluster_name, must_exist=True)
    p = run("terraform output -json", cwd=tf_dir, stdout=subprocess.PIPE)
    data = json.loads(p.stdout.decode("ascii"))
    if isinstance(key, list):
        return [data[k]["value"] for k in key]
    return data[key]["value"]


def aws_action(cluster_name: str, method: str, verb: str) -> None:
    ids = get_tf_output(cluster_name, "instance_ids")
    ec2 = boto3.client("ec2")
    fn = getattr(ec2, method)
    fn(InstanceIds=ids)
    click.secho(f"{verb} {cluster_name} ({len(ids)} instances)", fg="green")


# ------------------------------------------------------------
#     Ansible
# ------------------------------------------------------------


def get_ansible_inventory_content(node_ips: List[str]) -> str:
    def get_item(ip):
        host = "node_" + ip.replace(".", "_")
        return host, {"ansible_host": ip}

    hosts = [get_item(ip) for ip in node_ips]
    ansible_vars = {
        "ansible_user": "ubuntu",
        "ansible_ssh_private_key_file": "/home/ubuntu/.aws/login-us-west-2.pem",
        "ansible_host_key_checking": False,
    }
    ret = {
        "all": {
            "hosts": {k: v for k, v in hosts},
            "vars": ansible_vars,
        }
    }
    return yaml.dump(ret)


def get_or_create_ansible_inventory(
    cluster_name: str, ips: List[str] = []
) -> pathlib.Path:
    path = SCRIPT_DIR / ANSIBLE_DIR / f"_{cluster_name}.yml"
    if len(ips) == 0:
        if os.path.exists(path):
            return path
        raise ValueError("No hosts provided to Ansible")
    with open(path, "w") as fout:
        fout.write(get_ansible_inventory_content(ips))
    click.secho(f"Created {path}", fg="green")
    return path


def run_ansible_playbook(
    inventory_path: pathlib.Path,
    playbook: str,
    *,
    ev: Dict[str, str] = {},
    retries: int = 1,
    time_between_retries: float = 10,
) -> subprocess.CompletedProcess:
    if not playbook.endswith(".yml"):
        playbook += ".yml"
    playbook_path = SCRIPT_DIR / ANSIBLE_DIR / playbook
    cmd = f"ansible-playbook -f {PARALLELISM} {playbook_path} -i {inventory_path}"
    if len(ev) > 0:
        cmd += f" --extra-vars '{json.dumps(ev)}'"
    return run(cmd, retries=retries, time_between_retries=time_between_retries)


def is_hdd(instance_type: str) -> bool:
    return instance_type.startswith("d3.")


def get_nvme_device_count(instance_type: str) -> int:
    return {
        "i3.4xlarge": 2,
        "i3.8xlarge": 4,
        "i3.16xlarge": 8,
        "d3.xlarge": 3,
        "d3.2xlarge": 6,
        "d3.4xlarge": 12,
        "d3.8xlarge": 24,
        "m5d.4xlarge": 2,
        "m5d.8xlarge": 2,
        "m5d.12xlarge": 2,
        "m5d.16xlarge": 4,
        "m5d.24xlarge": 4,
    }.get(instance_type, 1)


def get_data_disks(instance_type: str) -> List[str]:
    cnt = get_nvme_device_count(instance_type)
    offset = 0 if instance_type.startswith("i3.") else 1
    return [f"/dev/nvme{i + offset}n1" for i in range(cnt)]


def get_mnt_paths(instance_type: str, no_disk: bool) -> List[str]:
    cnt = 0 if no_disk else get_nvme_device_count(instance_type)
    return [MNT_PATH_FMT.format(i=i) for i in range(cnt)]


def get_ansible_vars(instance_type: str, no_disk: bool) -> Dict:
    ret = {}
    if no_disk:
        ret["mnt_prefix"] = ""
    ret["data_disks"] = get_data_disks(instance_type)
    return ret


# ------------------------------------------------------------
#     YARN
# ------------------------------------------------------------


def update_hosts_file(ips: List[str]) -> None:
    PATH = "/etc/hosts"
    MARKER = "### HADOOP_YARN_HOSTS ###\n"
    with open(PATH) as fin:
        content = fin.read()

    marker_idx = content.find(MARKER)
    content = content if marker_idx < 0 else content[:marker_idx]
    content += "\n" + MARKER

    for i, ip in enumerate(ips):
        content += f"{ip} dn{i + 1}\n"

    run(f"sudo cp {PATH} {PATH}.backup")
    run(f"sudo echo <<EOF\n{content}\nEOF > {PATH}")
    click.secho(f"Updated {PATH}", fg="green")


def update_workers_file(ips: List[str]) -> None:
    PATH = os.path.join(os.getenv("HADOOP_HOME"), "etc/hadoop/workers")
    run(f"cp {PATH} {PATH}.backup")
    with open(PATH, "w") as fout:
        fout.write("\n".join(ips))
    click.secho(f"Updated {PATH}", fg="green")


def update_hadoop_xml(
    filename: str, head_ip: str, mnt_paths: List[str], is_hdd: bool
) -> None:
    with open(SCRIPT_DIR / HADOOP_TEMPLATE_DIR / (filename + ".template")) as fin:
        template = string.Template(fin.read())
    content = template.substitute(
        DEFAULT_FS=f"hdfs://{head_ip}:9000",
        HEAD_IP=head_ip,
        IO_BUFFER_SIZE=128 * KB if is_hdd else 4 * KB,
        DATA_DIRS=",".join(os.path.join(p, "hadoop/dfs/data") for p in mnt_paths),
        LOCAL_DIRS=",".join(os.path.join(p, "hadoop/yarn/local") for p in mnt_paths),
    )
    output_path = os.path.join(os.getenv("HADOOP_HOME"), "etc/hadoop", filename)
    with open(output_path, "w") as fout:
        fout.write(content)
    click.secho(f"Updated {output_path}", fg="green")


def update_hadoop_config(head_ip: str, mnt_paths: List[str], is_hdd: bool) -> None:
    for filename in ["core-site.xml", "hdfs-site.xml", "yarn-site.xml"]:
        update_hadoop_xml(filename, head_ip, mnt_paths, is_hdd)


# ------------------------------------------------------------
#     Prometheus
# ------------------------------------------------------------


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
    # TODO: only remove this when data is too big
    if os.path.exists(prometheus_data_path):
        shutil.rmtree(prometheus_data_path)
    os.makedirs(prometheus_data_path, exist_ok=True)
    with open("/tmp/prometheus/service_discovery.json", "w") as fout:
        fout.write(get_prometheus_sd_content(head_ip, ips))
    free_port(PROMETHEUS_SERVER_PORT)
    cmd = str(SCRIPT_DIR.parent / "raysort/bin/prometheus/prometheus")
    cmd += " --config.file=" + str(SCRIPT_DIR / "config/prometheus/prometheus.yml")
    cmd += f" --storage.tsdb.path={prometheus_data_path}"
    subprocess.Popen(cmd, shell=True)


def setup_grafana() -> None:
    cwd = str(SCRIPT_DIR.parent / "raysort/bin/grafana")
    cmd = f"{cwd}/bin/grafana-server"
    free_port(GRAFANA_SERVER_PORT)
    subprocess.Popen(cmd, cwd=cwd, shell=True)


# ------------------------------------------------------------
#     Interface Methods
# ------------------------------------------------------------


def common_setup(
    cluster_name: str, instance_type: str, cluster_exists: bool, no_disk: bool
) -> pathlib.Path:
    head_ip = run_output("ec2metadata --local-ipv4")
    ips = get_tf_output(cluster_name, "instance_ips")
    inventory_path = get_or_create_ansible_inventory(cluster_name, ips=ips)
    if not os.environ.get("HADOOP_HOME"):
        click.secho("$HADOOP_HOME not set, skipping Hadoop setup", color="yellow")
    else:
        update_hosts_file(ips)
        update_workers_file(ips)
        update_hadoop_config(
            head_ip, get_mnt_paths(instance_type, no_disk), is_hdd(instance_type)
        )
    # TODO: use boto3 to wait for describe_instance_status to be "ok" for all
    if not cluster_exists:
        sleep(60, "worker nodes starting up")
    ev = get_ansible_vars(instance_type, no_disk)
    run_ansible_playbook(inventory_path, "setup_aws", ev=ev, retries=10)
    setup_prometheus(head_ip, ips)
    setup_grafana()
    return inventory_path


def json_dump_no_space(data) -> str:
    return json.dumps(data, separators=(",", ":"))


def get_ray_start_cmd(
    s3_spill: int,
    is_hdd: bool,
    mnt_paths: List[str],
) -> Tuple[str, Dict]:
    system_config = {}
    if s3_spill > 0:
        system_config.update(
            **{
                "object_spilling_config": json_dump_no_space(
                    {
                        "type": "smart_open",
                        "params": {
                            "uri": [
                                RAY_S3_SPILL_PATH.format(i) for i in range(s3_spill)
                            ],
                            "buffer_size": 100 * MB,
                        },
                    }
                ),
            }
        )
    else:
        assert mnt_paths, "must have disks mounted or use S3 for spilling"
        system_config.update(
            **{
                "max_io_workers": max(4, len(mnt_paths) * 2),
                "object_spilling_config": json_dump_no_space(
                    {
                        "type": "filesystem",
                        "params": {
                            "directory_path": [f"{mnt}/ray" for mnt in mnt_paths],
                            "buffer_size": 10 * MB if is_hdd else -1,
                        },
                    },
                ),
            }
        )
    system_config_str = json_dump_no_space(system_config)
    resources = json_dump_no_space({"head": 1})
    cmd = "ray start --head"
    cmd += f" --metrics-export-port={RAY_METRICS_EXPORT_PORT}"
    cmd += f" --object-manager-port={RAY_OBJECT_MANAGER_PORT}"
    cmd += f" --system-config='{system_config_str}'"
    cmd += f" --resources='{resources}'"
    return cmd, system_config


def write_ray_system_config(config: Dict, path: str) -> None:
    with open(path, "w") as fout:
        yaml.dump(config, fout)


def restart_ray(
    inventory_path: pathlib.Path,
    instance_type: str,
    clear_data_dir: bool,
    reinstall_ray: bool,
    s3_spill: int,
    no_disk: bool,
) -> None:
    mnt_paths = get_mnt_paths(instance_type, no_disk)
    # Clear all mounts in case the previous setup has more mounts than we have.
    run(f"sudo rm -rf {MNT_PATH_PATTERN}")
    for mnt in mnt_paths:
        run(f"sudo mkdir -m 777 -p {mnt}")
    run(f"rsync -a {SCRIPT_DIR.parent}/ray-patch/ {ray.__path__[0]}")
    run("ray stop -f")
    ray_cmd, ray_system_config = get_ray_start_cmd(
        s3_spill, is_hdd(instance_type), mnt_paths
    )
    run(ray_cmd)
    head_ip = run_output("ec2metadata --local-ipv4")
    ev = {
        "head_ip": head_ip,
        "clear_data_dir": clear_data_dir,
        "reinstall_ray": reinstall_ray,
        "ray_object_manager_port": RAY_OBJECT_MANAGER_PORT,
        "ray_merics_export_port": RAY_METRICS_EXPORT_PORT,
        "mnt_paths": mnt_paths,
    }
    run_ansible_playbook(inventory_path, "ray", ev=ev)
    sleep(3, "waiting for Ray nodes to connect")
    run("ray status")
    write_ray_system_config(ray_system_config, RAY_SYSTEM_CONFIG_FILE_PATH)


def restart_yarn(
    inventory_path: pathlib.Path,
    instance_type: str,
    no_disk: bool,
) -> None:
    mnt_paths = get_mnt_paths(instance_type, no_disk)
    env = dict(
        os.environ,
        HADOOP_SSH_OPTS="-i ~/.aws/login-us-west-2.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
        HADOOP_OPTIONAL_TOOLS="hadoop-aws",
    )
    HADOOP_HOME = os.getenv("HADOOP_HOME")
    SPARK_HOME = os.getenv("SPARK_HOME")
    SPARK_EVENTS_DIR = "hdfs:///spark-events"
    run(f"{SPARK_HOME}/sbin/stop-history-server.sh")
    run(f"{HADOOP_HOME}/sbin/stop-yarn.sh", env=env)
    run(f"{HADOOP_HOME}/sbin/stop-dfs.sh", env=env)
    run(f"{HADOOP_HOME}/bin/hdfs namenode -format -force")
    run_ansible_playbook(inventory_path, "yarn", ev={"mnt_paths": mnt_paths})
    run(f"{HADOOP_HOME}/sbin/start-dfs.sh", env=env)
    run(f"{HADOOP_HOME}/sbin/start-yarn.sh", env=env)
    run(f"{HADOOP_HOME}/bin/hdfs dfs -mkdir {SPARK_EVENTS_DIR}")
    run(
        f"{SPARK_HOME}/sbin/start-history-server.sh",
        env=dict(
            env,
            SPARK_HISTORY_OPTS=f"-Dspark.history.fs.logDirectory={SPARK_EVENTS_DIR}",
        ),
    )


def print_after_setup(cluster_name: str) -> None:
    success_msg = f"Cluster {cluster_name} is up and running."
    click.secho("\n" + "-" * len(success_msg), fg="green")
    click.secho(success_msg, fg="green")
    click.echo(f"  Terraform config directory: {get_tf_dir(cluster_name)}")
    click.echo(f"  Ray system config written to: {RAY_SYSTEM_CONFIG_FILE_PATH}")


# ------------------------------------------------------------
#     CLI Interface
# ------------------------------------------------------------


def setup_command_options(cli_fn):
    decorators = [
        cli.command(),
        click.argument("cluster_name", default=DEFAULT_CLUSTER_NAME),
        click.argument("instance_count", default=DEFAULT_INSTANCE_COUNT),
        click.argument("instance_type", default=DEFAULT_INSTANCE_TYPE),
        click.option(
            "--ray",
            default=False,
            is_flag=True,
            help="start a Ray cluster",
        ),
        click.option(
            "--yarn",
            default=False,
            is_flag=True,
            help="start a YARN cluster",
        ),
        click.option(
            "--clear_data_dir",
            default=False,
            is_flag=True,
            help="whether to remove input data directory",
        ),
        click.option(
            "--reinstall_ray",
            default=False,
            is_flag=True,
            help="whether to reinstall Ray nightly",
        ),
        click.option(
            "--s3_spill",
            default=0,
            type=int,
            help="whether to ask Ray to spill to S3",
        ),
        click.option(
            "--no-disk",
            default=False,
            is_flag=True,
            help="whether to skip setting up local disks",
        ),
    ]
    ret = cli_fn
    for dec in decorators:
        ret = dec(ret)
    return ret


@click.group()
def cli():
    pass


@setup_command_options
def up(
    cluster_name: str,
    instance_count: int,
    instance_type: str,
    ray: bool,
    yarn: bool,
    clear_data_dir: bool,
    reinstall_ray: bool,
    s3_spill: int,
    no_disk: bool,
):
    cluster_exists = check_cluster_existence(cluster_name)
    config_exists = os.path.exists(get_tf_dir(cluster_name))
    if cluster_exists and not config_exists:
        error(f"{cluster_name} exists on the cloud but nothing is found locally")
    terraform_provision(cluster_name, instance_count, instance_type)
    inventory_path = common_setup(cluster_name, instance_type, cluster_exists, no_disk)
    if ray:
        restart_ray(
            inventory_path,
            instance_type,
            clear_data_dir,
            reinstall_ray,
            s3_spill,
            no_disk,
        )
    if yarn:
        restart_yarn(inventory_path, instance_type, no_disk)
    print_after_setup(cluster_name)


@setup_command_options
@click.option(
    "--no-common",
    default=False,
    is_flag=True,
    help="whether to skip common setup (file sync, mounts, etc)",
)
def setup(
    cluster_name: str,
    _: int,  # unused: instance_count
    instance_type: str,
    ray: bool,
    yarn: bool,
    clear_data_dir: bool,
    reinstall_ray: bool,
    s3_spill: int,
    no_disk: bool,
    no_common: bool,
):
    if no_common:
        inventory_path = get_or_create_ansible_inventory(cluster_name)
    else:
        inventory_path = common_setup(cluster_name, instance_type, True, no_disk)
    if ray:
        restart_ray(
            inventory_path,
            instance_type,
            clear_data_dir,
            reinstall_ray,
            s3_spill,
            no_disk,
        )
    if yarn:
        restart_yarn(inventory_path, instance_type, no_disk)
    print_after_setup(cluster_name)


@cli.command()
@click.argument("cluster_name", default=DEFAULT_CLUSTER_NAME)
def down(cluster_name: str):
    tf_dir = get_or_create_tf_dir(cluster_name, must_exist=True)
    cmd = "terraform destroy -auto-approve" + get_terraform_vars(
        cluster_name=cluster_name
    )
    run(cmd, cwd=tf_dir)
    check_cluster_existence(cluster_name, raise_if_exists=True)


@cli.command()
@click.argument("cluster_name", default=DEFAULT_CLUSTER_NAME)
def start(cluster_name: str):
    aws_action(cluster_name, "start_instances", "Started")


@cli.command()
@click.argument("cluster_name", default=DEFAULT_CLUSTER_NAME)
def stop(cluster_name: str):
    aws_action(cluster_name, "stop_instances", "Stopped")


@cli.command()
@click.argument("cluster_name", default=DEFAULT_CLUSTER_NAME)
def reboot(cluster_name: str):
    aws_action(cluster_name, "reboot_instances", "Rebooted")


@cli.command()
@click.argument("cluster_name", default=DEFAULT_CLUSTER_NAME)
@click.argument("worker_id_or_ip", type=str, default="0")
def ssh(cluster_name: str, worker_id_or_ip: str):
    ips = get_tf_output(cluster_name, "instance_ips")
    click.echo(f"worker_ips = {ips}")
    try:
        ip = ips[int(worker_id_or_ip)]
    except ValueError:
        ip = worker_id_or_ip
    run(
        f"ssh -i ~/.aws/login-us-west-2.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {ip}"
    )


if __name__ == "__main__":
    cli()
