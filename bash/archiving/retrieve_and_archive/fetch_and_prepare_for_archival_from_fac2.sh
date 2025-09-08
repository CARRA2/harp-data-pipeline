#!/usr/bin/env bash
#SBATCH --error=log_arxiv_prep.%j.err
#SBATCH --output=log_arxiv_prep.%j.out
#SBATCH --job-name=mars_arxiv_prep
#SBATCH --qos=nf
#SBATCH --mem-per-cpu=16000
#SBATCH --account="c3srrp"


module load ecmwf-toolbox
module load python3
CONFIG1=./mars_config.yaml
CONFIG2=./mars_config_archive.yaml
if [ -z $1 ]; then
  echo "General script to fetch data from marsscratch (archived there by nhd periodically)"
  echo "and then create scripts to archive in mars (to be done later by fac2)"
  echo "Using config file $CONFIG1 for data fetching"
  echo "Using config file $CONFIG2 for creating archiving scripts for fac2"
  echo "Please provide period to process in format YYYYMM (ie, 199010)"
  exit 1
else
  PERIOD=$1
fi
began=$(date  '+%Y%m%d_%H%M%S')
DUMP_PATH="/ec/res4/scratch/nhd/mars-pull/carra2/fetch_to_archive"
#fetch data from mars scratch
python3 fetch_from_marsscr.py $PERIOD $DUMP_PATH $CONFIG1

#create archival scripts to be used by fac2
python3 archive_to_mars.py $PERIOD $DUMP_PATH $CONFIG2
chmod -R 755 $DUMP_PATH/$PERIOD

now=$(date  '+%Y%m%d_%H%M%S')
NL=$(wc -l < archive_${PERIOD}_from_fac2.sh)
if [ "$NL" -gt 170 ]; then
    echo "$PERIOD  $began $now" >> fetching_registry.txt
fi
