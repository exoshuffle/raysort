#!/usr/bin/env bash

set -ex

CLOUD=aws

python update_inventory.py
ansible-playbook setup_$CLOUD.yml -i _$CLOUD.yml
