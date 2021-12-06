#!/usr/bin/env bash

# exit when any command fails
set -e

ray stop

ray start --head --port=6379 \
    --object-manager-port=8076 \
    --metrics-export-port=8090 \
    --num-cpus=0 \
    --system-config='{"max_io_workers":8,"object_spilling_threshold":1,"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":[\"/mnt/nvme0/tmp/ray\"]}}"}' \
    --object-store-memory=30064771072

ansible-playbook -i vmss.yml ray.yml

pkill -9 prometheus
python ~/raysort/raysort/create_prom_sd_file.py
~/raysort/raysort/bin/prometheus/prometheus --config.file=/home/azureuser/raysort/config/prometheus.yml &

ray status
