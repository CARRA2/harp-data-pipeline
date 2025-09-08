#!/usr/bin/env bash
#source ${ECFPROJ_LIB}/bin/env.sh #set some environment variables below
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

ml ecmwf-toolbox #eccodes and the like
ml eclib # includes scripts like newdata to get correct dates, including leap years

# BACKLOG for the monthly means
# This script calculates the min or max monthly value over
# a number of min/max parameters found in the files:
# daily_minmax_ORIGIN_fc_sfc_YYYYMMDD.grib2
# --------------------------------------------------------------------------------
# ONLY THESE VARIABLES are processed
# Maximum temperature at 2 metres since previous post-processing  mx2t        201        
# Minimum temperature at 2 metres since previous post-processing  mn2t        202        
# 10 metre wind gust since previous post-processing               10fg        49
# 10 metre eastward wind gust since previous post-processing  10efg       260646     
# 10 metre northward wind gust since previous post-processing  10nfg       260647     
# --------------------------------------------------------------------------------
#
# The script uses either the cdo operators cdo ensmin and cdo ensmax to
# to calculate these values or a python script
# 1. Extract the selected parameter to a temporary file using grib_filter, so the
#    cdo operator will act over these only
# 2. use the cdo operator or a python script over all the days of the given month
# After testing both, the cdo output is correct but the grib codes are wrong.
# Attempting to correct them with grib_set did not work after first attempt.
# Decided to use the python script instead. Leaving the cdo version below for reference

# These parameters require taking the max. The remaining parameter, 202, uses the min
# This list is used below to decide if using min or max in the calculation
max_param=(49 201 260646 260647)
all_permitted=(49 201 202 260646 260647)
all_permitted=(201 202) #for CARRA2
type=fc
levtype=sfc
declare -A levtype_int
levtype_int[sfc]=103



if [[ -z $1 ]]; then
  echo "Please provide period and domain to process" #, as well as the parameter code (only 49,201,202,260646,260647)"
  echo "Example: 202106 no-ar-cw" #201
  exit 1
else
  period=$1
  origin=$2
  #param=$3
fi

YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
MM=$(substring $period 5 6)
WDIR=$MEANS_OUTPUT/$origin/$YYYY/$MM

if [ ! -d $WDIR ]; then
 echo "ERROR: data path $WDIR does not exist!"
 exit 1
fi


maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
date_end=${period}$MAXDAY
date_beg=${period}01

calc_with_cdo()
{
# calculate min or max per month using cdo
#this is for grib_set
declare -A snames

snames[49]="10fg"
snames[201]="mx2t"
snames[202]="mn2t"
snames[260646]="10efg"
snames[260647]="10nfg"
ml cdo
input_files=()
for date in $(seq -w $date_beg $date_end); do
  #echo "Doing $date"
  IN=$WDIR/daily_minmax_${origin}_${type}_${levtype}_${date}.grib2
   if [ ! -f $IN ]; then
     echo "ERROR: data stream incomplete! Date $date is missing: $IN"
     exit 1
   fi
tmpfile=$WDIR/${param}_${origin}_${date}.grib2
cat >  filter_var << EOF
if ( param == $param && levelType == ${levtype_int[$levtype]} )
{
  write "${tmpfile}";
}
EOF

grib_filter filter_var $IN
input_files+=("$tmpfile") # this creates the whole string for all the input files
done
OUT=$WDIR/monthly_mean_cdo_${origin}_${type}_${levtype}_$period.grib2
if [[ ${max_param[@]} =~ $param ]]
then
  echo "Calculating the maximum monthly value over all days for $param"
  cdo ensmax ${input_files[@]} $OUT
else
  echo "Calculating the minimum monthly value over all days for $param"
  cdo ensmin ${input_files[@]} $OUT
fi
echo "Removing temporary files"
rm ${input_files[@]}

#check the output, change the codes
grib_ls -p name,shortName,param $OUT

}


calc_with_python()
{
ml conda
conda activate glat #py38
# calculate min or max per month using python
# First combine all grib files for the month in one, then use
# python to calculate monthly min or max

input_files=()
for date in $(seq -w $date_beg $date_end); do
  #echo "Doing $date"
  IN=$WDIR/daily_minmax_${origin}_${type}_${levtype}_${date}.grib2
   if [ ! -f $IN ]; then
     echo "ERROR: data stream incomplete! Date $date is missing: $IN"
     exit 1
   fi
   input_files+=("$IN") # this creates the whole string for all the input files
done
#this file contains all the min/max params for the whole month
tmpfile=$WDIR/tmp_daily_cat_${origin}_${type}_${levtype}.grib2
if [ ! -f $tmpfile ]; then
    cat ${input_files[@]} > $tmpfile
else 
    echo "$tmpfile already created"
fi

input_files=()
for param in ${all_permitted[@]}; do
  OUT=$WDIR/monthly_${param}_${origin}_${type}_${levtype}_$period.grib2
  echo "Doing $param"
  python ${ECFPROJ_LIB}/bin/calc_monthly_minmax.py $tmpfile $OUT $param $origin $period
  input_files+=("$OUT") # this creates the whole string for all the input files
done
rm $tmpfile
OUT=$WDIR/monthly_minmax_${origin}_${type}_${levtype}_$period.grib2
cat ${input_files[@]} > $OUT
rm ${input_files[@]}
}

# run with either, compare later

# This one requires the param as 3rd argument in CLI
#calc_with_cdo

# This one simply loops over all of them in the function
calc_with_python


