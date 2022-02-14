#!/usr/bin/env bash

set -ex

CLOUD=aws

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

export HADOOP_SSH_OPTS="-i ~/.aws/login-us-west-2.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

$SPARK_HOME/sbin/stop-history-server.sh

$HADOOP_HOME/sbin/stop-yarn.sh
$HADOOP_HOME/sbin/stop-dfs.sh

$HADOOP_HOME/bin/hdfs namenode -format -force

python "$DIR/update_inventory.py" --cloud=$CLOUD

# After formatting namenode, the datanodes are left with a different file system.
# The spark.yml will remove existing data directory which contains incorrect metadata.
ansible-playbook -f $(($(nproc) * 4)) -i "$DIR/_$CLOUD.yml" "$DIR/spark.yml"

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

SPARK_LOGS_DIR=/spark-logs
SPARK_HISTORY_SERVER_CONF_FILE=/tmp/spark-history-server.conf
$HADOOP_HOME/bin/hdfs dfs -mkdir $SPARK_LOGS_DIR
echo "spark.history.fs.logDirectory hdfs://$SPARK_LOGS_DIR" > "$SPARK_HISTORY_SERVER_CONF_FILE"
$SPARK_HOME/sbin/start-history-server.sh --properties-file "$SPARK_HISTORY_SERVER_CONF_FILE"
