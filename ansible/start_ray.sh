#!/usr/bin/env bash

CLOUD=aws
PROM_DATA_PATH=/tmp/prom_data
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
TMP_DIR=/mnt/nvme0/tmp

set -ex

sudo mkdir -p $TMP_DIR
sudo chmod 777 $TMP_DIR

ray stop -f

ray start --head --port=6379 \
    --object-manager-port=8076 \
    --metrics-export-port=8090 \
    --resources='{"head":1}' \
    --system-config='{"object_spilling_threshold":1,"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":[\"/mnt/nvme0/tmp/ray\"]}}"}'
    # --system-config='{"send_unpin":true,"object_spilling_threshold":1,"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":[\"/mnt/nvme0/tmp/ray\"]}}"}'

HEAD_IP=$(ec2metadata --local-ipv4)
ansible-playbook -f $(($(nproc) * 4)) "$SCRIPT_DIR/ray.yml" -i "$SCRIPT_DIR/_$CLOUD.yml" --extra-vars "{\"head_ip\":\"$HEAD_IP\"}"
sleep 3

pkill -9 prometheus || true
python $SCRIPT_DIR/create_prom_sd_file.py
rm -rf $PROM_DATA_PATH
~/raysort/raysort/bin/prometheus/prometheus --config.file=$HOME/raysort/config/prometheus.yml --storage.tsdb.path=$PROM_DATA_PATH &
ray status
