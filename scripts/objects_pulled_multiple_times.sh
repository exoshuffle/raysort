#!/bin/bash

grep "Received pull request from node" /tmp/ray/session_latest/logs/raylet.*.out | awk '{print $1, $12, $15}' | sort -t ' ' -k 3,3
