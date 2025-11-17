#!/bin/bash

if [ -f ./env.sh ]; then
  source ./env.sh
else
  source $ECFPROJ_LIB/share/config/config.aa
fi

#SBATCH --mem-per-cpu=64GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS


# BACKLOG for the monthly means. Calculating:
# 1. monthly means for an files, instantaneous.  EXCEPT ML (done in do_monthly_merge_an_ml.sh)
# 2. monthly means for fc files, instantaneous (only sfc params)
# 3. monthly means of accumulated parameters (fc only)

ml ecmwf-toolbox #eccodes and the like
#ml eclib # includes scripts like newdata to get correct dates, including leap years
#source $HOME/bin/utils.sh #a few other functions I need below
#gmean=/home/nhd/scripts/carra/carra_means/src/grib_mean.x

if [[ -z $1 ]]; then
  echo "Please provide period,domain, file and level type"
  echo "Example: 202106 no-ar-pa an ml"
  exit 1
else
  period=$1
  origin=$2
  type=$3
  levtype=$4

fi
echo $period $origin $type $levtype

#YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
#MM=$(substring $period 5 6)
YYYY=$(echo $period | awk '{print substr($1,1,4)}')
MM=$(echo $period | awk '{print substr($1,5,6)}')

WDIR=$MEANS_OUTPUT/$origin/$YYYY/$MM

if [ ! -d $WDIR ]; then
 echo "ERROR: data path $WDIR does not exist!"
 exit 1
fi
maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
date_end=${period}$MAXDAY
date_beg=${period}01


#Function to extract selected variable. Tested in do_monthly_fc_insta 
# Not using at the moment since grib_mean can use -f param=param
extract_var()
{
tmpfile=$WDIR/tmp_${param}_${date}.grib2
cat >  filter_var << EOF
if ( param == $param )
{
  write "${tmpfile}";
}
EOF

grib_filter filter_var $IN

}

#This function will extract only selected variables
#to be used in the fc sfc variables.
# This is to separate the precipitation type from the monthly means (ie, 260015 is excluded)
extract_selected_fc()
{
tmpfile=$WDIR/tmp_fc_sfc_daily_${date}.grib2
cat >  filter_var << EOF
if ( param == 78 || param == 79 || param == 260648 )
{
  write "${tmpfile}";
}
EOF
grib_filter filter_var $IN
export IN=$tmpfile
}

#############################################
#monthly mean for instantaneous parameters
#############################################
# For an files
do_monthly_an_insta()
{
echo "Doing monthly means for analysis instantaneous parameters of type $levtype"
 input_files=()
 for date in $(seq -w $date_beg $date_end); do
   IN=$WDIR/daily_mean_${origin}_${type}_${levtype}_${date}.grib2
   if [ ! -f $IN ]; then
     echo "ERROR: data stream incomplete! Date $date is missing: $IN"
     exit 1     
   fi
   input_files+=("-i $IN") # this creates the whole string for all the input files
 done
 OUT=$WDIR/monthly_mean_${origin}_${type}_${levtype}_$period.grib2
 $gmean -k date ${input_files[@]} -o $OUT  -n $MAXDAY
}

# instantaneous variables found in the fc files only
do_monthly_fc_insta()
{
# for fc files
echo "Doing monthly means for $type instantaneous parameters of type ${levtype}"
 input_files=()
 for date in $(seq -w $date_beg $date_end); do
   IN=$WDIR/daily_mean_${origin}_${type}_${levtype}_${date}.grib2
   extract_selected_fc #extracts only selected variables (excluding ptype)
   if [ ! -f $IN ]; then
     echo "ERROR: data stream incomplete! Date $date is missing: $IN"
     exit 1     
   fi
   #the two lines below use the grib_mean command 
   input_files+=("-i $IN") # this creates the whole string for all the input files

 done
 OUT=$WDIR/monthly_mean_${origin}_${type}_${levtype}_$period.grib2
 $gmean -k date ${input_files[@]} -o $OUT -n $MAXDAY
}


do_monthly_fc_accum()
{
# for fc files, accumulated
levtype=sfc
echo "Doing monthly means for accumulated $type parameters of type $levtype"
 input_files=()
 for date in $(seq -w $date_beg $date_end); do
   IN=$WDIR/daily_sum_${origin}_${type}_${levtype}_${date}.grib2
   if [ ! -f $IN ]; then
     echo "ERROR: data stream incomplete! Date $date is missing: $IN"
     exit 1     
   fi
   input_files+=("-i $IN") # this creates the whole string for all the input files
 done
 OUT=$WDIR/monthly_mean_accum_${origin}_${type}_${levtype}_$period.grib2
 $gmean -k date ${input_files[@]} -o $OUT  -n $MAXDAY
}



# fc files (only one variable being done now, since the rest are min or max over the month)
if [[ $levtype == sfc ]] && [[ $type == fc ]]; then
  do_monthly_fc_insta
fi
