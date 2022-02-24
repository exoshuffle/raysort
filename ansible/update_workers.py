from update_inventory import get_aws_vmss_ips, run

# TODO: only overwrite existing lines
# See https://github.com/franklsf95/raysort/pull/13#discussion_r790120763

# What the machine starts off with in its original /etc/hosts
existing_lines = """
127.0.0.1 localhost

# The following lines are desirable for IPv6 capable hosts
::1 ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters
ff02::3 ip6-allhosts

"""

from absl import app

# TODO: duplicate flags not allowed since we import update_inventory
# from absl import flags

# FLAGS = flags.FLAGS
# flags.DEFINE_enum(
#     "cloud",
#     "aws",
#     ["aws", "azure", "aws_spark"],
#     "which cloud is the cluster on",
# )
# flags.DEFINE_enum("storage", "ebs", ["ebs", "nvme"], "type of storage the cluster uses")


def create_hosts_file(node_ips):
    hosts_file_contents = existing_lines
    for i, ip in enumerate(node_ips):
        hosts_file_contents += f"{ip} dn{i + 1}\n"

    # Save current /etc/hosts as backup just in case.
    run("sudo cp /etc/hosts hosts-backup.txt")

    run(f'echo "{hosts_file_contents}" | sudo tee /etc/hosts')


def create_workers_file(node_ips):
    # Save current workers as backup just in case.
    run("cp $HADOOP_HOME/etc/hadoop/workers workers-backup.txt")

    workers_file_contents = "\n".join(node_ips)
    run(f'echo "{workers_file_contents}" > $HADOOP_HOME/etc/hadoop/workers')


def main(argv):
    del argv  # Unused.
    CLOUD = "aws"
    STORAGE = "hdd"
    node_ips = get_aws_vmss_ips(CLOUD, STORAGE)
    create_hosts_file(node_ips)
    create_workers_file(node_ips)


if __name__ == "__main__":
    app.run(main)
