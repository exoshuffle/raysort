"""Cluster management toolkit."""
import json
import os
import pathlib
import psutil
import signal
import shutil
import subprocess
import time
from typing import Dict, List, Tuple, Union
import yaml

import boto3
import click

DEFAULT_CLUSTER_NAME = "raysort-lsf"

EBS_MNT = "/mnt/ebs0/tmp"
PARALLELISM = os.cpu_count() * 4
SCRIPT_DIR = pathlib.Path(os.path.dirname(__file__))

ANSIBLE_DIR = "config/ansible"
TERRAFORM_DIR = "config/terraform"
TERRAFORM_TEMPLATE_DIR = "aws-template"
RAY_SYSTEM_CONFIG_FILE_PATH = SCRIPT_DIR.parent / "_ray_config.yml"
RAY_S3_SPILL_PATH = "s3://raysort-tmp/ray"

PROMETHEUS_SERVER_PORT = 9090
PROMETHEUS_NODE_EXPORTER_PORT = 8091
RAY_METRICS_EXPORT_PORT = 8090
RAY_OBJECT_MANAGER_PORT = 8076
GRAFANA_SERVER_PORT = 3000


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
    time_between_retries: float = 10,
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


def run_ansible_playbook(
    inventory_path: pathlib.Path,
    playbook: str,
    *,
    vars: Dict[str, str] = {},
    retries: int = 10,
    time_between_retries: float = 30,
) -> subprocess.CompletedProcess:
    if not playbook.endswith(".yml"):
        playbook += ".yml"
    playbook_path = SCRIPT_DIR / ANSIBLE_DIR / playbook
    cmd = f"ansible-playbook -f {PARALLELISM} {playbook_path} -i {inventory_path}"
    if len(vars) > 0:
        cmd += f" --extra-vars '{json.dumps(vars)}'"
    return run(cmd, retries=retries, time_between_retries=time_between_retries)


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


def check_cluster_existence(cluster_name: str, raise_if_exists: bool = False) -> None:
    instances = get_instances(
        {
            "tag:ClusterName": [cluster_name],
            # Exclude "Terminated" state (0x30).
            # https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html
            "instance-state-code": [
                str(code) for code in [0x00, 0x10, 0x20, 0x40, 0x50]
            ],
        }
    )
    cnt = len(instances)
    if raise_if_exists and cnt > 0:
        error(f"{cluster_name} must not exist (found {cnt} instances)")


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


def terraform_provision(cluster_name: str) -> None:
    tf_dir = get_or_create_tf_dir(cluster_name)
    run("terraform init", cwd=tf_dir)
    cmd = "terraform apply -auto-approve"
    cmd += f' -var="cluster_name={cluster_name}"'
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


def get_prometheus_sd_content(ips: List[str]) -> str:
    def get_addrs(port):
        return [f"{ip}:{port}" for ip in ips]

    return json.dumps(
        [
            {
                "labels": {"job": "ray"},
                "targets": get_addrs(RAY_METRICS_EXPORT_PORT),
            },
            {
                "labels": {"job": "node"},
                "targets": get_addrs(PROMETHEUS_NODE_EXPORTER_PORT),
            },
        ]
    )


def setup_prometheus(ips: List[str]) -> None:
    prometheus_data_path = "/tmp/prometheus"
    if os.path.exists(prometheus_data_path):
        shutil.rmtree(prometheus_data_path)
    os.makedirs(prometheus_data_path, exist_ok=True)
    with open("/tmp/prometheus/service_discovery.json", "w") as fout:
        fout.write(get_prometheus_sd_content(ips))
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


def common_setup(cluster_name: str) -> pathlib.Path:
    head_ip = run_output("ec2metadata --local-ipv4")
    ids, ips = get_tf_output(cluster_name, ["instance_ids", "instance_ips"])
    inventory_path = get_or_create_ansible_inventory(cluster_name, ips=ips)
    if not os.environ.get("HADOOP_HOME"):
        click.secho("$HADOOP_HOME not set, skipping Hadoop setup", color="yellow")
    else:
        update_hosts_file(ips)
        update_workers_file(ips)
        # TODO: Update core-site.xml and yarn-site.xml with head node IP
    # TODO: use boto3 to wait for describe_instance_status to be "ok" for all
    run_ansible_playbook(inventory_path, "setup_aws")
    setup_prometheus(ips + [head_ip])
    setup_grafana()
    return inventory_path


def json_dump_no_space(data) -> str:
    return json.dumps(data, separators=(",", ":"))


def get_ray_start_cmd(s3_spill: bool) -> Tuple[str, Dict]:
    system_config = {}
    if s3_spill:
        # uris = [RAY_S3_SPILL_PATH + f"/{i}" for i in range(10)]
        system_config.update(
            **{
                "object_spilling_config": json_dump_no_space(
                    {"type": "smart_open", "params": {"uri": RAY_S3_SPILL_PATH}}
                ),
            }
        )
    else:
        system_config.update(
            **{
                "object_spilling_config": json_dump_no_space(
                    {
                        "type": "filesystem",
                        "params": {"directory_path": [f"{EBS_MNT}/ray"]},
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
    clear_input_dir: bool,
    reinstall_ray: bool,
    s3_spill: bool,
) -> None:
    head_ip = run_output("ec2metadata --local-ipv4")
    run(f"sudo mkdir -p {EBS_MNT} && sudo chmod 777 {EBS_MNT}")
    run("ray stop -f")
    ray_cmd, ray_system_config = get_ray_start_cmd(s3_spill)
    run(ray_cmd)
    vars = {
        "head_ip": head_ip,
        "clear_input_dir": clear_input_dir,
        "reinstall_ray": reinstall_ray,
        "ray_object_manager_port": RAY_OBJECT_MANAGER_PORT,
        "ray_merics_export_port": RAY_METRICS_EXPORT_PORT,
    }
    run_ansible_playbook(inventory_path, "ray", vars=vars, retries=1)
    sleep(3, "waiting for Ray nodes to connect")
    run("ray status")
    write_ray_system_config(ray_system_config, RAY_SYSTEM_CONFIG_FILE_PATH)


def restart_yarn() -> None:
    # TODO: convert this to Python
    run(str(SCRIPT_DIR / "config/ansible/start_spark.sh"))


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
            "--clear_input_dir",
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
            default=False,
            is_flag=True,
            help="whether to ask Ray to spill to S3",
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
    ray: bool,
    yarn: bool,
    clear_input_dir: bool,
    reinstall_ray: bool,
    s3_spill: bool,
):
    cluster_exists = check_cluster_existence(cluster_name)
    config_exists = os.path.exists(SCRIPT_DIR / TERRAFORM_DIR / cluster_name)
    if cluster_exists and not config_exists:
        error(f"{cluster_name} exists on the cloud but nothing is found locally")
    terraform_provision(cluster_name)
    inventory_path = common_setup(cluster_name)
    if ray:
        restart_ray(inventory_path, clear_input_dir, reinstall_ray, s3_spill)
    if yarn:
        restart_yarn()
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
    ray: bool,
    yarn: bool,
    clear_input_dir: bool,
    reinstall_ray: bool,
    s3_spill: bool,
    no_common: bool,
):
    if no_common:
        inventory_path = get_or_create_ansible_inventory(cluster_name)
    else:
        inventory_path = common_setup(cluster_name)
    if ray:
        restart_ray(inventory_path, clear_input_dir, reinstall_ray, s3_spill)
    if yarn:
        restart_yarn()
    print_after_setup(cluster_name)


@cli.command()
@click.argument("cluster_name", default=DEFAULT_CLUSTER_NAME)
def down(cluster_name: str):
    tf_dir = get_or_create_tf_dir(cluster_name, must_exist=True)
    cmd = "terraform destroy -auto-approve"
    cmd += f' -var="cluster_name={cluster_name}"'
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
@click.argument("worker_id", type=int, default=0)
def ssh(cluster_name: str, worker_id: int):
    ips = get_tf_output(cluster_name, "instance_ips")
    click.echo(f"worker_ips = {ips}")
    ip = ips[worker_id]
    run(
        f"ssh -i ~/.aws/login-us-west-2.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {ip}"
    )


if __name__ == "__main__":
    cli()
