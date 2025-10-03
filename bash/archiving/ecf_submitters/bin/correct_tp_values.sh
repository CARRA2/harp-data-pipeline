#!/usr/bin/env bash
if [ -f ./env.sh ]; then
  source ./env.sh 
else
  source $ECFPROJ_LIB/share/config/config.aa
fi



if [ -z $1 ]; then
 echo "This script acts only on monthly means file of type sfc"
 echo "It will only select the parameter total precipitation"
 echo "and set any negative minimum value to zero"
 echo "Minimum argument is the period in format YYYYMM"
 echo "Alternatively include the domain (ie, no-ar-ce or no-ar-cw)"
 echo "Example: ./correct_tp_values.sh 202301 no-ar-cw [1:IHL 1:IPL 1:ISL 1:ISF 1:IML 1:IAC]"
 echo "If domain not provided, it will do both domains!"
 exit 1
else
 PERIOD=$1
 DOM=$2
fi

export JOB_NAME_DMEANS=$PERIOD
#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS
#SBATCH --job-name=$JOB_NAME_DMEANS

ml conda
conda activate glat

YYYY=$(echo $PERIOD | awk '{print substr($1,1,4)}')
MM=$(echo $PERIOD | awk '{print substr($1,5,6)}')

check_min()
{
  for DOM in no-ar-ce no-ar-cw; do
    echo "Doing $DOM"
    for F in $MEANS_OUTPUT/$DOM/$YYYY/$MM/monthly_mean_accum_${DOM}_fc_sfc_$PERIOD.grib2; do
      if [ ! -f $F ] ; then
        echo "$F missing!"
        exit 1
      else
      echo "minimum value for tp in $F"
      grib_get -F "%.6f" -p minimum -w shortName=tp $F
      fi
    done
  done
}

set_min_to_zero()
{
if [ -z $DOM ]; then
   echo "provide domain (ie, no-ar-pa)"
   exit 1
else
    echo "Doing $DOM"
    for F in $MEANS_OUTPUT/$DOM/$YYYY/$MM/SUMS/monthly_mean_accum_${DOM}_fc_sfc_${PERIOD}_228228.grib2; do
      if [ ! -f $F ] ; then
        echo "$F missing!"
        echo "Note in this case field is expected to be on separate file"
        exit 1
      fi
      OUT=$MEANS_OUTPUT/$DOM/$YYYY/$MM/SUMS/$(basename $(basename $F) .grib2)_corr.grib2
      ${ECFPROJ_LIB}/bin/set_tp_to_zero.py $F $OUT $DOM $PERIOD
      echo "Checking min value of tp after correction"
      grib_get -F "%.6f" -p minimum -w shortName=tp $OUT
      #replace original file
      mv $OUT $F
    done
fi
}

set_min_to_zero
