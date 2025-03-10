#!/usr/bin/env bash
#SBATCH --mem-per-cpu=16GB
#SBATCH --time=00:30:00
#SBATCH --error=log_means_daily.%j.err
#SBATCH --output=log_means_daily.%j.out


export ECF_PORT=3141
export ECF_HOST="ecflow-gen-${USER}-001"

module load python3
module load ecflow
python3 run_new_period.py
