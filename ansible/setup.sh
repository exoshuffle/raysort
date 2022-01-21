#!/usr/bin/env bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

CLOUD=aws

python $SCRIPT_DIR/update_inventory.py
ansible-playbook -f $(nproc) $SCRIPT_DIR/setup_$CLOUD.yml -i $SCRIPT_DIR/_$CLOUD.yml
