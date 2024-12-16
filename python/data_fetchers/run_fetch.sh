#!/usr/bin/env bash
#SBATCH --job-name=fetch_carra2
#SBATCH --qos=nf
#SBATCH --error=fetch_job-%j.err
#SBATCH --output=fetch_job-%j.out

source ../../config/config.aa
CONF=streams_carra2.yml #for production  (to be run from fac2)
CONF=streams_test_local.yml # test with current dummy progress.log and local copying
CONF=streams_test_current.yml # test with current progress.log and local copying

module load python3
python3 fetch_data_yearly.py -config $CONF
