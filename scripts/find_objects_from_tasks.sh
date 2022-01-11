#!/bin/bash
# Prints 3 columns: raylet log file name, Task ID, Object ID
grep -n "RayTask" /tmp/ray/session_latest/logs/raylet.*.out | awk '{print $8}' | sort | uniq > scripts/data/tasks.txt
while read p; do
  grep "$p" /tmp/ray/session_latest/logs/raylet.*.out | grep "blocked on object" | awk '{print $1, $8,$12}' 
done < scripts/data/tasks.txt

# Gets mapper and merge task IDs from tmp.txt, assuming tmp.txt contains all shell output (not log output) of run.
grep "mapper task TaskID" scripts/data/tmp.txt | awk '{print $5}' | sed 's/TaskID(//g' | sed 's/)//g' | sort > scripts/data/map_tasks.txt
grep "merge task TaskID" scripts/data/tmp.txt | awk '{print $5}' | sed 's/TaskID(//g' | sed 's/)//g' | sort > scripts/data/merge_tasks.txt
grep "cluster_task_manager.cc:478: Queuing and scheduling task" $LOG/raylet.*.out | awk '{print $11}' | sort | uniq > scripts/data/tasks_scheduled.txt
grep "dependency_manager.cc:183: Task" $LOG/raylet.*.out | awk '{print $8}' | sort | uniq > scripts/data/tasks_waiting_on_objects.txt
