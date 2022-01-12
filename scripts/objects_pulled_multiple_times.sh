#!/bin/bash

# Three columns of output: raylet log filename, node ID, object ID
grep "Received pull request from node" /tmp/ray/session_latest/logs/raylet.*.out | awk '{print $1, $12, $15}' | sort -t ' ' -k 3,3 > scripts/data/object_pulls_by_node.txt
