#!/bin/bash

echo "Map tasks that were scheduled on raylets:"
while read p; do
  grep "$p" scripts/data/tasks_scheduled.txt
done < scripts/data/map_tasks.txt
echo "Map tasks that were blocked on objects:"
while read p; do
  grep "$p" scripts/data/tasks_waiting_on_objects.txt
done < scripts/data/map_tasks.txt
total=$(wc -l scripts/data/merge_tasks.txt)
echo "Merge tasks that were blocked on objects (total merge tasks: $total):"
while read p; do
  grep "$p" scripts/data/tasks_waiting_on_objects.txt
done < scripts/data/merge_tasks.txt
echo "Object-blocked, non-merge tasks:"
while read p; do
  if ! grep -Fxq "$p" scripts/data/merge_tasks.txt; then echo "$p"; fi
done < scripts/data/tasks_waiting_on_objects.txt  
