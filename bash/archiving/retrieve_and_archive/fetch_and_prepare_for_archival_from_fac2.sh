#!/usr/bin/env bash
#SBATCH --error=log_arxiv_prep.%j.err
#SBATCH --output=log_arxiv_prep.%j.out
#SBATCH --job-name=mars_arxiv_prep
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
#fetch
python3 fetch_from_marsscr.py $PERIOD $DATA

#create archival scripts to be used by fac2
python3 archive_to_mars.py $PERIOD $DATA
chmod -R 755 $DATA/$PERIOD
