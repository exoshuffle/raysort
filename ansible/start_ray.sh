#!/usr/bin/env bash

set -ex

CLOUD=aws

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

ray stop

ray start --head --port=6379 \
    --object-manager-port=8076 \
    --metrics-export-port=8090 \
    --resources='{"head":1}' \
    --system-config='{"max_io_workers":8,"object_spilling_threshold":1,"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":[\"/mnt/nvme0/tmp/ray\"]}}"}' \
    --object-store-memory=30064771072

HEAD_IP=$(ec2metadata --local-ipv4)
ansible-playbook "$DIR/ray.yml" -i "$DIR/_$CLOUD.yml" --extra-vars "{\"head_ip\":\"$HEAD_IP\"}"

pkill -9 prometheus || true
python ~/raysort/raysort/create_prom_sd_file.py
~/raysort/raysort/bin/prometheus/prometheus --config.file=$HOME/raysort/config/prometheus.yml &

ray status
