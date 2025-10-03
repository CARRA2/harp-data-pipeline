#!/usr/bin/env bash

# Functions to check if all levels are where they are supposed to be

# This must be loaded in the config file
# ml eclib
# ml ecmwf-toolbox

# available types:
levels_an=(hl pl sfc sol ml)



check_messages()
{
#print a message if there is an error with the messages in the file
#initially tried count but took too long. Now trying grib_ls to check if messages ok
MESS=$(grib_ls -p name $IN | grep ERROR | awk '{print $1}')
#MESS=$(grib_count $IN | grep Invalid | awk '{print $1}')
#grib_ls -p name,time,step $IN

if [ -n "${MESS}" ]; then
 echo "ERROR in $IN"
fi
}

check_size()
{
SIZE=$(du -sh $IN | awk '{print $1}')
#UNIT=$(echo $STRING | grep -Eo '[[:alpha:]]+')
#SIZE=$(du -sh $IN | grep -Eo '[+-]?[0-9]+([.][0-9]+)?')
}

count_all_daily()
{
# Counts all files in each level type and gives a warning if something is missing
YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
MM=$(substring $period 5 6)
maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
date_beg=${period}01
date_end=${period}$MAXDAY
WDIR=$MEANS_OUTPUT/$origin/$YYYY/$MM

#check the daily means for an file
if [[ ${levels_an[@]} =~ $levtype ]] && [[ $type == an ]]; then

  echo "Counting and checking daily means for $type $levtype files"
  for date in $(seq -w $date_beg $date_end); do
    IN=$WDIR/daily_mean_${origin}_${type}_${levtype}_${date}.grib2
  check_size
  check_messages
  if [[ ! -f $IN ]] || [[ $SIZE == 0 ]]; then
      echo "ERROR: $IN not found! or size too small: $SIZE" 
      echo "Exiting file count for file of type $type and level $levtype"
      exit 1
  fi
  done
fi


#check the daily sum
if [[ $levtype == sfc ]] && [[ $type == sum ]]; then
  echo "Counting and checking daily sums for $type $levtype files"
  #check the sfc for forecast files
  for date in $(seq -w $date_beg $date_end); do
    IN=$WDIR/daily_sum_${origin}_${type}_${levtype}_${date}.grib2
    check_size
    check_messages
    if [[ ! -f $IN ]] || [[ $SIZE == 0 ]]; then
        echo "ERROR: $IN not found! or size too small: $SIZE" 
    fi
  done
fi


#check the daily means for fc files
if [[ $levtype == sfc ]] && [[ $type == fc ]]; then
  echo "Counting and checking daily means for $type $levtype files"
  for date in $(seq -w $date_beg $date_end); do
    IN=$WDIR/daily_mean_${origin}_${type}_${levtype}_${date}.grib2
    check_size
    check_messages
    if [[ ! -f $IN ]] || [[ $SIZE == 0 ]]; then
        echo "ERROR: $IN not found! or size too small: $SIZE" 
    fi
  done
fi

#check the daily means for fc files with minmax
if [[ $levtype == mm ]] && [[ $type == fc ]]; then
  echo "Counting and checking daily means for $type $levtype files"
  for date in $(seq -w $date_beg $date_end); do
    IN=$WDIR/daily_minmax_${origin}_${type}_sfc_${date}.grib2
    check_size
    check_messages
    if [[ ! -f $IN ]] || [[ $SIZE == 0 ]]; then
        echo "ERROR: $IN not found! or size too small: $SIZE" 
    fi
  done
fi
}

if [[ -z $1 ]]; then
  echo "Please provide period, domain, file type and level type"
  echo "Example: 202106 no-ar-cw an pl"
  exit 1
else
  period=$1
  origin=$2
  type=$3
  levtype=$4
fi

source ${ECFPROJ_LIB}/bin/env.sh #set some environment variables below
count_all_daily
