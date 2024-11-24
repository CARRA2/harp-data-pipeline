#!/usr/bin/env bash
#SBATCH --error=log_mars_archive.%j.err
#SBATCH --output=log_mars_archive.%j.out
#SBATCH --job-name=mars_prep
#SBATCH --qos=nf
#SBATCH --mem-per-cpu=16000
#SBATCH --account="c3srrp"

module load ecmwf-toolbox
module load python3

if [ -z $1 ]; then
  echo "Please provide period to process in format YYYYMM (ie, 199010)"
  exit 1
else
  PERIOD=$1
fi

DATA="/ec/res4/scratch/nhd/mars-pull/carra2/fetch_to_archive"
python3 archive_to_mars.py $PERIOD $DATA
chmod -R 755 $DATA/$PERIOD
