#!/bin/bash

while read p; do
    echo "Objects $p was blocked on: "
    grep "$p" scripts/data/objects_per_task.txt | wc -l
done < scripts/data/merge_tasks.txt 	
