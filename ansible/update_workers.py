from update_inventory import get_aws_vmss_ips, run

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

if __name__ == "__main__":
    ips = get_aws_vmss_ips("aws_spark")

    hosts_file_contents = existing_lines
    for i, ip in enumerate(ips):
        hosts_file_contents += f"{ip} dn{i + 1}\n"

    # Save current /etc/hosts as backup just in case.
    run("sudo cp /etc/hosts hosts-backup.txt")

    run(f'echo "{hosts_file_contents}" | sudo tee /etc/hosts')

    # Save current workers as backup just in case.
    run("cp $HADOOP_HOME/etc/hadoop/workers workers-backup.txt")

    workers_file_contents = "\n".join(ips)
    run(f'echo "{workers_file_contents}" > $HADOOP_HOME/etc/hadoop/workers')
