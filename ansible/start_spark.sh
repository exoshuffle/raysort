#!/usr/bin/env bash

set -ex

CLOUD=aws_spark

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

$HADOOP_HOME/sbin/stop-yarn.sh
$HADOOP_HOME/sbin/stop-dfs.sh

$HADOOP_HOME/bin/hdfs namenode -format -force
python "$DIR/update_inventory.py"
# After formatting namenode, the datanodes are left with a different file system.
# The spark.yml will remove existing data directory which contains incorrect metadata.
ansible-playbook -i "$DIR/_$CLOUD.yml" "$DIR/spark.yml"

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
