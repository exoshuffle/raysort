#!/usr/bin/env bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

CLOUD=aws
STORAGE=hdd

python $SCRIPT_DIR/update_inventory.py --cloud=$CLOUD --storage=$STORAGE
# TODO: update_inventory.py should also make a prometheus discovery file
# TODO: update_workers.py needs to take in --cloud
# python $SCRIPT_DIR/update_workers.py
# TODO: fill in core-site.xml and yarn-site.xml automatically.
ansible-playbook -f $(($(nproc) * 4)) $SCRIPT_DIR/setup_${CLOUD}_${STORAGE}.yml -i $SCRIPT_DIR/_${CLOUD}_${STORAGE}.yml
