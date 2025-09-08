#!/usr/bin/env bash
#SBATCH --error=log_mars_fetch.%j.err
#SBATCH --output=log_mars_fetch.%j.out
#SBATCH --job-name=mars_fetch
#SBATCH --qos=nf
#SBATCH --mem-per-cpu=16000
#SBATCH --account="c3srrp"


module load python3
if [ -z $1 ]; then
  echo "Script to submit fetching of data from marscratch for archival to mars by fac2"
  echo "Please provide period to process in format YYYYMM (ie, 199010)"
  exit 1
else
  PERIOD=$1
fi

DATADIR="/ec/res4/scratch/nhd/mars-pull/carra2/fetch_to_archive"
python3 fetch_from_marsscr.py $PERIOD $DATADIR ./mars_config.yaml
