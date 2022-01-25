#!/usr/bin/env bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

CLOUD=aws

python $SCRIPT_DIR/update_inventory.py --cloud=$CLOUD
if [ $CLOUD == "aws_spark" ]; then
    python $SCRIPT_DIR/update_workers.py
fi
ansible-playbook -f $(($(nproc) * 4)) $SCRIPT_DIR/setup_$CLOUD.yml -i $SCRIPT_DIR/_$CLOUD.yml
