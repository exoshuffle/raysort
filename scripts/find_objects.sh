#!/bin/bash

grep -n "blocked on object" /tmp/ray/session_latest/logs/raylet.*.out | awk '{print $12}' | sort | wc -l
echo "blocked on object"
grep -n "blocked on object" /tmp/ray/session_latest/logs/raylet.*.out | awk '{print $12}' | sort | uniq | wc -l
echo "unique"
grep -n "blocked on object" /tmp/ray/session_latest/logs/raylet.1.out | awk '{print $12}' | sort | wc -l
echo "blocked on object"
grep -n "blocked on object" /tmp/ray/session_latest/logs/raylet.1.out | awk '{print $12}' | sort | uniq | wc -l
echo "unique"
grep -n "blocked on object" /tmp/ray/session_latest/logs/raylet.2.out | awk '{print $12}' | sort | wc -l
echo "blocked on object"
grep -n "blocked on object" /tmp/ray/session_latest/logs/raylet.2.out | awk '{print $12}' | sort | uniq | wc -l
echo "unique"
grep -n "blocked on object" /tmp/ray/session_latest/logs/raylet.3.out | awk '{print $12}' | sort | wc -l
echo "blocked on object"
grep -n "blocked on object" /tmp/ray/session_latest/logs/raylet.3.out | awk '{print $12}' | sort | uniq | wc -l
echo "unique"
grep -n "blocked on object" /tmp/ray/session_latest/logs/raylet.4.out | awk '{print $12}' | sort | wc -l
echo "blocked on object"
grep -n "blocked on object" /tmp/ray/session_latest/logs/raylet.4.out | awk '{print $12}' | sort | uniq | wc -l
echo "unique"

