import ast
import json
import pathlib
import subprocess
import yaml
from typing import Dict, List

from absl import app
from absl import flags
from absl import logging

FLAGS = flags.FLAGS
flags.DEFINE_enum(
    "cloud",
    "aws",
    ["aws", "azure", "aws_spark"],
    "which cloud is the cluster on",
)
flags.DEFINE_string(
    "subscription",
    "aa86df77-e703-453e-b2f4-955c3b33e534",
    "Subscription ID",
)
flags.DEFINE_string(
    "resource_group",
    "raysort-ncus-rg",
    "Resource group name",
)
flags.DEFINE_string(
    "vmss",
    "raysort-vmss",
    "VMSS name",
)


def run(cmd, **kwargs):
    logging.info("$ " + cmd)
    return subprocess.run(cmd, shell=True, check=True, **kwargs)


def run_output(cmd, **kwargs):
    proc = run(cmd, stdout=subprocess.PIPE, **kwargs)
    return proc.stdout.decode("ascii")


def run_json(cmd, **kwargs):
    return json.loads(run_output(cmd, **kwargs))


def get_vmss_ips() -> List[str]:
    if FLAGS.cloud == "aws":
        return get_aws_vmss_ips()
    elif FLAGS.cloud == "azure":
        return get_azure_vmss_ips()
    assert False, FLAGS


def get_aws_vmss_ips() -> List[str]:
    terraform_cwd = pathlib.Path(__file__).parent.parent / "terraform" / "aws"
    out = run_output("terraform output", cwd=terraform_cwd)
    out = out.split(" = ")[1]
    ret = ast.literal_eval(out)
    assert isinstance(ret, list) and isinstance(ret[0], str), (ret, out)
    return ret


def get_azure_vmss_ips() -> List[str]:
    res = run_json(
        f"az vmss nic list --subscription={FLAGS.subscription} --resource-group={FLAGS.resource_group} --vmss-name={FLAGS.vmss}"
    )
    assert isinstance(res, list), res

    def get_ip(nic):
        ip_conf = nic["ipConfigurations"]
        assert isinstance(ip_conf, list) and len(ip_conf) == 1, ip_conf
        ip_conf = ip_conf[0]
        ip = ip_conf["privateIpAddress"]
        return ip.strip()

    return [get_ip(nic) for nic in res]


def get_ansible_vars() -> Dict[str, str]:
    if FLAGS.cloud == "aws":
        return {
            "ansible_user": "ubuntu",
            "ansible_ssh_private_key_file": "/home/ubuntu/.aws/login-us-west-2.pem",
        }
    elif FLAGS.cloud == "azure":
        return {
            "ansible_user": "azureuser",
            "ansible_ssh_private_key_file": "/home/azureuser/.ssh/lsf-azure-aa.pem",
        }
    assert False, FLAGS


def get_inventory_content(node_ips):
    def get_item(ip):
        host = "node_" + ip.replace(".", "_")
        return host, {"ansible_host": ip}

    hosts = [get_item(ip) for ip in node_ips]
    ansible_vars = get_ansible_vars()
    ansible_vars.update(
        **{
            "ansible_host_key_checking": False,
        }
    )

    ret = {
        "all": {
            "hosts": {k: v for k, v in hosts},
            "vars": ansible_vars,
        }
    }
    return yaml.dump(ret)


def write_inventory_file(content):
    path = f"_{FLAGS.cloud}.yml"
    with open(path, "w") as fout:
        fout.write(content)


def main(argv):
    del argv  # Unused.
    node_ips = get_vmss_ips()
    content = get_inventory_content(node_ips)
    write_inventory_file(content)
    print(content)


if __name__ == "__main__":
    app.run(main)
