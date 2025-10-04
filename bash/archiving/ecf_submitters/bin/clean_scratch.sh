#!/usr/bin/env bash
#SBATCH --mem-per-cpu=8GB
#SBATCH --time=00:30:00
#SBATCH --account=c3srra

#source ${ECFPROJ_LIB}/bin/env.sh #set some environment variables below
if [ -f ./env.sh ]; then
  source ./env.sh
else
  source $ECFPROJ_LIB/share/config/config.aa
fi

ml eclib

if [[ -z $1 ]] && [[ -z $2 ]]; then
  echo "Please provide period (YYYYMM) and domain"
  exit 1
else
  PERIOD=$1
  DOM=$2
fi
YYYY=$(substring $PERIOD 1 4)
MM=$(substring $PERIOD 5 6)


echo "Removing local directories for $PERIOD and $DOM"
WDIR=$MEANS_OUTPUT/$DOM/$YYYY/$MM

rm -rf $WDIR

