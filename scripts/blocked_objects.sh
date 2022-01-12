#!/bin/bash

grep "blocked on object" $LOG/raylet.*.out | awk '{print $12}' | sort | uniq -d > scripts/data/objects_blocked_multiple_times.txt
grep "blocked on object" $LOG/raylet.*.out | awk '{print $1, $8, $12}' | sort -k 3,3 > scripts/data/tasks_per_object.txt 
grep "blocked on object" $LOG/raylet.*.out | awk '{print $1, $8, $12}' | sort -k 2,2 > scripts/data/objects_per_task.txt 

touch scripts/data/blocked_objects_not_map_outputs.txt
while read p; do
    if ! grep -q "ObjectRef($p)" scripts/data/tmp.txt; then
	    echo "$p" >> scripts/data/blocked_objects_not_map_outputs.txt
    fi
done < scripts/data/objects_blocked_multiple_times.txt
