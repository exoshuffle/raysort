#!/bin/bash

# Launch cluster
ray up -y scripts/config/autoscaler/_pipeline-training.yaml

# Get head node IP
HEAD_IP=$(ray exec /home/ubuntu/raysort/scripts/config/autoscaler/_pipeline-training.yaml "ray status" | grep -n "Fetched IP"| awk '{print $3}')

# Set up port forwarding for Prometheus
ssh -o IdentitiesOnly=yes -i ~/.aws/login-us-west-2.pem -f -N -L 9090:localhost:9090 $HEAD_IP
