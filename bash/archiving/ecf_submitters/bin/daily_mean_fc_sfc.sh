#!/bin/bash
# BACKLOG for SFC levels, FC files
# All data is staged, retrieved and processed below. The output is one single file
# The output is one file per day with all the daily means for the given level type

# This file processes a few surface parameters not found in the analysis files.
# They are all min/max or some sort. The daily mean of the day is based on the
# min or max or the corresponding variable. See below for details

#source ${ECFPROJ_LIB}/bin/env.sh #set some environment variables below
source env.sh #set some environment variables below

#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

ml ecmwf-toolbox #eccodes and the like
ml eclib # includes scripts like newdata to get correct dates, including leap years
#source $HOME/bin/utils.sh #a few other functions I need below

#gmean=${ECFPROJ_LIB}/bin/grib_mean.x

expver=prod
class=rr
stream=oper
type=fc
step="3"
levtype=sfc
levelist=off

param=$CARRA_PAR_FC_SFC_IN


if [[ -z $1 ]]; then
  echo "Please provide period and domain to process"
  echo "Example: 202106 no-ar-cw"
  exit 1
else
  period=$1
  origin=$2
  day_beg=$3
  day_end=$4
  #If I give two extra arguments it will set the initial and final of the month
  #Otherwise it will do the whole month. This is just for testing
  YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
  MM=$(substring $period 5 6)
  maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
  [ -z $day_beg ] && day_beg=01
  [ -z $day_end ] && day_end=$MAXDAY
fi

YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
MM=$(substring $period 5 6)

WDIR=$MEANS_OUTPUT/$origin/$YYYY/$MM ; [[ ! -d $WDIR ]] && mkdir -p $WDIR

maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
#get the date of yesterday for the start of the period
date_end=${period}${day_end}
date_beg=${period}${day_beg}

#Call the mars staging first...
alldates="$date_beg/TO/$date_end"
echo "Doing mars staging for the period $date_beg to $date_end"
com="origin=$origin,expver=$expver,class=$class,stream=$stream,type=$type,step=$step,levtype=$levtype,levelist=$levelist,param=$param"
 mars << eof
     stage, $com, date=$alldates,time=0000/0300/0600/0900/1200/1500/1800/2100
eof

echo "Doing mars retrieval and means calculation for the period $date_beg to $date_end"
for date in $(seq -w $date_beg $date_end); do
    #fc sfc variables in the same list. Consider moving to other script if list grows
    gfile=$WDIR/${origin}_${type}_${levtype}_${date}.grib2
    ydat=$(newdate -D $date -1)
    com="origin=$origin,expver=$expver,class=$class,stream=$stream,type=$type,step=$step,levtype=$levtype,levelist=$levelist,param=$param"
mars << eof
     retrieve, $com, date=$ydat,time=21          ,target="$gfile"
     retrieve, $com, date=$date,time=0/to/18/by/3,target="$gfile"
eof

    gfile=$WDIR/${origin}_${type}_${levtype}_${date}.grib2
    base=$(basename $gfile)
    mfile=$WDIR/daily_mean_${base}

    #with gribmean
    #$gmean -k time,step -i $gfile -o $mfile -n 24
    $gmean -k date,time -i $gfile -o $mfile -n 8
    chmod 755 $mfile
done
#remove the temporary input files
rm -f $WDIR/${origin}_${type}_${levtype}_*.grib2

