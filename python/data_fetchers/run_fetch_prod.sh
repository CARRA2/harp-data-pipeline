#!/usr/bin/env bash
#SBATCH --job-name=fetch_carra2
#SBATCH --error=fetch_job-%j.err
#SBATCH --output=fetch_job-%j.out
#SBATCH --time=8:00:00
#SBATCH --account=c3srrp

source ../../config/config.aa
CONF=streams_carra2.yml #for production  (to be run from fac2)
module load python3
python3 fetch_data_yearly.py -config $CONF
