#!/bin/bash

grep -n "RayTask" /tmp/ray/session_latest/logs/raylet.*.out | awk '{print $8}' | sort | uniq > tasks.txt
while read p; do
  grep "$p" /tmp/ray/session_latest/logs/raylet.*.out | grep "blocked on object" | awk '{print $1, $8,$12}' 
done < data/tasks.txt
