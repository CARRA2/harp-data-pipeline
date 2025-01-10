#!/usr/bin/env bash
export ECF_PORT=3141
export ECF_HOST="ecflow-gen-${USER}-001"

module load python3
module load ecflow
python3 run_new_period.py
