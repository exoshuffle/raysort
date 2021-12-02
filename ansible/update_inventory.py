import json
import subprocess
import yaml

from absl import app
from absl import flags
from absl import logging

FLAGS = flags.FLAGS
flags.DEFINE_string(
    "inventory_file",
    "raysort-vmss.yml",
    "path to the inventory file",
)
flags.DEFINE_string(
    "subscription",
    "aa86df77-e703-453e-b2f4-955c3b33e534",
    "Subscription ID",
)
flags.DEFINE_string(
    "resource_group",
    "lsf-aa",
    "Resource group name",
)
flags.DEFINE_string(
    "vmss",
    "raysort-vmss",
    "VMSS name",
)


def get_host_ip():
    return "10.10.0.4"


def run(cmd, **kwargs):
    logging.info("$ " + cmd)
    return subprocess.run(cmd, shell=True, check=True, **kwargs)


def run_json(cmd):
    proc = run(cmd, stdout=subprocess.PIPE)
    return json.loads(proc.stdout.decode("ascii"))


def get_vmss_ips():
    res = run_json(
        f"az vmss nic list "
        f"--subscription={FLAGS.subscription} --resource-group={FLAGS.resource_group} --vmss-name={FLAGS.vmss}"
    )
    assert isinstance(res, list), res

    def get_ip(nic):
        ip_conf = nic["ipConfigurations"]
        assert isinstance(ip_conf, list) and len(ip_conf) == 1, ip_conf
        ip_conf = ip_conf[0]
        ip = ip_conf["privateIpAddress"]
        return ip.strip()

    return [get_ip(nic) for nic in res]


def get_inventory_content(node_ips):
    def get_item(ip):
        host = "node_" + ip.replace(".", "_")
        return host, {"ansible_host": ip}

    items = [get_item(ip) for ip in node_ips]

    ret = {
        "all": {
            "hosts": {k: v
                      for k, v in items},
            "vars": {
                "ansible_host_key_checking":
                False,
                "ansible_user":
                "azureuser",
                "ansible_ssh_private_key_file":
                "/home/azureuser/.ssh/lsf-azure-aa.pem",
            }
        }
    }
    return yaml.dump(ret)


def write_inventory_file(content):
    with open(FLAGS.inventory_file, "w") as fout:
        fout.write(content)


def main(argv):
    del argv  # Unused.
    node_ips = get_vmss_ips()
    content = get_inventory_content(node_ips)
    write_inventory_file(content)
    print(content)


if __name__ == "__main__":
    app.run(main)
