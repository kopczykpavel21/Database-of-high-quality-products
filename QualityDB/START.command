#!/bin/bash
# Double-click this file on Mac to start QualityDB
cd "$(dirname "$0")"
python3 server.py &
sleep 1
open http://localhost:5000
wait
