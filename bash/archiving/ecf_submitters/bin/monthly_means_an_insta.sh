#!/bin/bash

if [ -f ./env.sh ]; then
  source ./env.sh
else
  source $ECFPROJ_LIB/share/config/config.aa
fi

#source $ECFPROJ_LIB/share/config/config.aa
#source $ARPROJ_LIB/bash/archiving/ecf_submitters/bin/env.sh
#source env.sh #set some environment variables below

#SBATCH --mem-per-cpu=64GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

# monthly means for an type instantaneous, levels: hl, ml and pl

# This script takes all the days and calculates the monthly mean for all levels specified above
# The files are merged into one per day for all parameters
# and one monthly file per period and level type is generated at the end

ml ecmwf-toolbox #eccodes and the like
ml eclib # includes scripts like newdata to get correct dates, including leap years

NF_EXP=715 #expected number of fields in monthly or daily file. Pre calculated for ML ONLY. TODO for the rest?

if [[ -z $1 ]]; then
  echo "Please provide period and domain to process"
  echo "Example: 202106 no-ar-pa"
  exit 1
else
  period=$1
  origin=$2
fi

YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
MM=$(substring $period 5 6)
WDIR=$MEANS_OUTPUT/$origin/$YYYY/$MM ; [[ ! -d $WDIR ]] && mkdir -p $WDIR #final results

maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
date_end=${period}$MAXDAY
date_beg=${period}01
type=an
#levtype=ml

#############################################
#monthly mean for instantaneous parameters
#############################################
do_monthly_means()
{
LEVTYPE=${levtype^^} #capitalize for path
DATADIR=$MEANS_OUTPUT/$origin/$YYYY/$MM/$LEVTYPE

for param in ${PARAMS[@]}; do
 input_files=() # to feed the gmean command
 counts_files=() # just to check number of fields in each file
 for date in $(seq -w $date_beg $date_end); do
   IN=$DATADIR/daily_mean_${origin}_${type}_${levtype}_${date}_${param}.grib2
   if [ ! -f $IN ]; then
     echo "ERROR: data stream incomplete! Date $date is missing: $IN"
     exit 1     
   fi
   input_files+=("-i $IN") # this creates the whole string for the gmean command
   counts_files+=($(grib_count $IN))
 done
 OUT=$DATADIR/monthly_mean_${origin}_${type}_${param}_${levtype}_$period.grib2
 $gmean -k date ${input_files[@]} -o $OUT  -n $MAXDAY
 chmod 755 $OUT
 #check number of fields:
 final_count=$(grib_count $OUT)
 echo "Final count of parameters in $OUT: $final_count"
 echo "Number of parameters in all $levtype files: ${counts_files[@]}"
done
}

merge_files()
{
LEVTYPE=${levtype^^} #capitalize for path
DATADIR=$MEANS_OUTPUT/$origin/$YYYY/$MM/$LEVTYPE
echo $LEVTYPE

# merge monthly means for all parameters in one file
# also merge the daily means in one file afterwards

month_means=() # to concatenate monthly means in one file at the end
for param in ${PARAMS[@]}; do
 OUT=$DATADIR/monthly_mean_${origin}_${type}_${param}_${levtype}_$period.grib2
 month_means+=($OUT) # saving to concatenate at the end
done
MONTHLY_MEAN=$WDIR/monthly_mean_${origin}_${type}_${levtype}_${period}.grib2
echo "Merging the monthly means in one file for $LEVTYPE levels: $MONTHLY_MEAN"
cat ${month_means[@]} > $MONTHLY_MEAN
FCOUNT=$(grib_count $MONTHLY_MEAN)
#echo "Final count of fields: $FCOUNT (expected: $NF_EXP)"

echo "Now merging the daily means"
# merge daily means for all parameters in one file
 for date in $(seq -w $date_beg $date_end); do
  day_means=() # to concat day means
   for param in ${PARAMS[@]}; do
   IN=$DATADIR/daily_mean_${origin}_${type}_${levtype}_${date}_${param}.grib2
   day_means+=($IN)
  done
  DAILY_MEAN=$WDIR/daily_mean_${origin}_${type}_${levtype}_${date}.grib2
  echo "Merging the daily means for $date in one file for $LEVTYPE levels: $DAILY_MEAN"
  cat ${day_means[@]} > $DAILY_MEAN
  FCOUNT=$(grib_count $DAILY_MEAN)
  #echo "Final count of fields: $FCOUNT (expected: $NF_EXP)"
 done 

#remove the temporary directory where I split the parameters for the level type
 #### rm -rf $DATADIR

}


#the daily means of type sfc are treated separately, since they are all in one file per day already
do_monthly_sfc()
{
LEVTYPE=${levtype^^} #capitalize for path
DATADIR=$MEANS_OUTPUT/$origin/$YYYY/$MM/$LEVTYPE
echo "Doing monthly means for analysis instantaneous parameters of type $levtype"
 input_files=()
 for date in $(seq -w $date_beg $date_end); do
   IN=$DATADIR/daily_mean_${origin}_${type}_${levtype}_${date}.grib2
   if [ ! -f $IN ]; then
     echo "ERROR: data stream incomplete! Date $date is missing: $IN"
     exit 1
   fi
   input_files+=("-i $IN") # this creates the whole string for all the input files
 done
 OUT=$WDIR/monthly_mean_${origin}_${type}_${levtype}_$period.grib2
 $gmean -k date ${input_files[@]} -o $OUT  -n $MAXDAY
 # move the daily means to the main directory?
 # mv $DATADIR/daily_mean_${origin}_${type}_${levtype}_* $WDIR
 # rmdir $DATADIR
}



#dictionary for parameters

declare -A par_dic

get_params()
{
# Set the Internal Field Separator to "/"
IFS='/'
# Read the string into an array
read -ra parameters <<< "$param_string"
# Reset IFS to default (whitespace)
unset IFS
}

param_string=$CARRA_PAR_AN_HL
get_params
par_dic[hl]=${parameters[@]}

param_string=$CARRA_PAR_AN_ML
get_params
par_dic[ml]=${parameters[@]}

param_string=$CARRA_PAR_AN_PL
get_params
par_dic[pl]=${parameters[@]}

#echo ${par_dic[hl]}
#echo ${par_dic[ml]}
#echo ${par_dic[pl]}


#calculate the monthly means on each separate in the correspoding paths under $MEANS_OUTPUT/$origin/$YYYY/$MM/PL,ML and HL

for levtype in hl ml pl; do
echo "Doing monthly means for all parameters of leveltype $levtype"
PARAMS=${par_dic[$levtype]}
do_monthly_means
done


#merge all monthly means in one file. Merge all daily means on one file per day
for levtype in hl ml pl; do
echo "Doing merging of monthly means for all parameters of leveltype $levtype"
PARAMS=${par_dic[$levtype]}
merge_files
done

# for monthly means of sfc type use the function below
levtype="sfc"
echo "Doing monthly means for all parameters of leveltype $levtype"
do_monthly_sfc

# clean the old directories
clean_old_data()
{
for LEVTYPE in HL ML PL; do
DATADIR=$MEANS_OUTPUT/$origin/$YYYY/$MM/$LEVTYPE
rm -rf $DATADIR
done
}
