#!/bin/bash

#source env.sh #set some environment variables below
#source $ECFPROJ_LIB/share/config/config.aa

if [ -f ./env.sh ]; then
  source ./env.sh
else
  source $ECFPROJ_LIB/share/config/config.aa
fi



#SBATCH --mem-per-cpu=64GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

# monthly means for fc type accumulated

ml ecmwf-toolbox #eccodes and the like
ml eclib # includes scripts like newdata to get correct dates, including leap years


if [[ -z $1 ]]; then
  echo "Please provide period and domain to process"
  echo "Example: 202106 no-ar-pa"
  exit 1
else
  period=$1
  origin=$2
  param=$3
fi

YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
MM=$(substring $period 5 6)
WDIR=$MEANS_OUTPUT/$origin/$YYYY/$MM/SUMS ; [[ ! -d $WDIR ]] && mkdir -p $WDIR #final results

maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
date_end=${period}$MAXDAY
date_beg=${period}01
# for fc files, accumulated at the surface
type=fc
levtype=sfc

merge_month_files()
{
echo "Merging the monthly means of daily sums"
# merge monthly means for all parameters in one file
# also merge the daily means in one file afterwards
month_means=() # to concatenate monthly means in one file at the end
for param in ${PARAMS[@]}; do
 OUT=$WDIR/monthly_mean_accum_${origin}_${type}_${levtype}_${period}_${param}.grib2
 month_means+=($OUT) # saving to concatenate at the end
done

MONTHLY_MEAN=$MEANS_OUTPUT/$origin/$YYYY/$MM/monthly_mean_accum_${origin}_${type}_${levtype}_${period}.grib2
echo "Merging all monthly means of daily sums in $MONTHLY_MEAN"
cat ${month_means[@]} > $MONTHLY_MEAN
}

merge_day_files()
{
echo "Merging the daily sums"
# merge daily means for all parameters in one file
 for date in $(seq -w $date_beg $date_end); do
  day_means=() # to concat day means
   for param in ${PARAMS[@]}; do
   IN=$WDIR/daily_sum_${origin}_${type}_${levtype}_${date}_${param}.grib2
   day_means+=($IN)
  done
  DAILY_MEAN=$MEANS_OUTPUT/$origin/$YYYY/$MM/daily_sum_${origin}_${type}_${levtype}_${date}.grib2
  echo "Merging all daily means for $date in $DAILY_MEAN"
  cat ${day_means[@]} > $DAILY_MEAN
 done

}

do_monthly_fc_accum()
{
for param in ${PARAMS[@]}; do
echo "Doing monthly means for accumulated $type $param"
 input_files=()
 for date in $(seq -w $date_beg $date_end); do
   IN=$WDIR/daily_sum_${origin}_${type}_${levtype}_${date}_${param}.grib2
   if [ ! -f $IN ]; then
     echo "ERROR: data stream incomplete! Date $date is missing: $IN"
     exit 1
   fi
   input_files+=("-i $IN") # this creates the whole string for all the input files
 done #date
 OUT=$WDIR/monthly_mean_accum_${origin}_${type}_${levtype}_${period}_${param}.grib2
 $gmean -k date ${input_files[@]} -o $OUT  -n $MAXDAY
done #param
}

if [ -z $3 ]; then
echo "Doing all parameters in $CARRA_PAR_FC_ACC"
# Get the parameters
# Set the Internal Field Separator to "/"
IFS='/'
# Read the string into an array
read -ra PARAMS <<< "$CARRA_PAR_FC_ACC"
# Reset IFS to default (whitespace)
unset IFS
else
PARAMS=($param)
echo "Doing only $param"
fi


#calculate the monthly means on each separate in the correspoding paths under $MEANS_OUTPUT/$origin/$YYYY/$MM/SUMS
echo "Doing monthly means of daily sums for all parameters in accumulated sums"
do_monthly_fc_accum

#Not doing this anymore, since the archival is done directly on the unmerged files
#echo "Doing the merge"
#merge_month_files

#merge_day_files
#remove the temporary directory where I split the parameters for the level type
#NEED TO KEEP THIS! Remove it after archiving
#rm -rf $WDIR

