#!/bin/bash
cd "$(dirname "$0")"
echo "Start time: $(date)" >> log.txt
./venv/bin/python item_level_holds.py >> log.txt &
wait
