#!/bin/bash
# BACKLOG for SFC levels, SUM of ACCUMULATED variables in FC files
# Maybe here only makes sense to include the precipitation?
# All data is staged, retrieved and processed below. The output is one single file
# The output is one file per day with all the daily means for the given level type
#source env.sh #set some environment variables below
source $ECFPROJ_LIB/share/config/config.aa



#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

ml ecmwf-toolbox #eccodes and the like
ml eclib # includes scripts like newdata to get correct dates, including leap years
#source $HOME/bin/utils.sh #a few other functions I need below
#gmean=/home/nhd/scripts/carra/carra_means/src/grib_mean.x

expver=prod
class=rr
stream=oper
type=fc
step=3  
levtype=sfc
#This list I got from the google doc in https://docs.google.com/document/d/1rULkNAdFGBgzksslRGZNvhwB03xR8vkWrEozhMS6dgM/edit#
params=$CARRA_PAR_FC_ACC
RE_WRITE=0 #for testing. Set to 1 if want to re-write the data


if [[ -z $1 ]]; then
  echo "Please provide period and domain to process"
  echo "Example: 202106 no-ar-cw"
  exit 1
else
  period=$1
  origin=$2
  param=$3
  #day_beg=$3
  #day_end=$4
  #YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
  #MM=$(substring $period 5 6)
  #maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
  #[ -z $day_beg ] && day_beg=01
  #[ -z $day_end ] && day_end=$MAXDAY
fi
YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
MM=$(substring $period 5 6)
maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
day_beg=01
day_end=$MAXDAY

WDIR=$MEANS_OUTPUT/$origin/$YYYY/$MM/SUMS ; [[ ! -d $WDIR ]] && mkdir -p $WDIR

#get the date of yesterday for the start of the period
date_end=${period}${day_end}
date_beg=${period}${day_beg}
date_ydat=$(newdate -D ${period}${day_beg} -1)

#Call the mars staging first...
alldates="$date_ydat/TO/$date_end"
#param="all"
echo "Doing mars staging for the period $date_beg to $date_end"
com="origin=$origin,expver=$expver,class=$class,stream=$stream,type=$type,step=$step,levtype=$levtype,param=$param"
 mars << eof
     stage, $com, date=$alldates,time=0000/0300/0600/0900/1200/1500/1800/2100
eof



do_all_params()
{
# Set the Internal Field Separator to "/"
IFS='/'
# Read the string into an array
read -ra PARAMS <<< "$params"
# Reset IFS to default (whitespace)
unset IFS

for param in ${PARAMS[@]}; do
echo "Doing mars retrieval and means calculation for the period $date_beg to $date_end for $param"
com="origin=$origin,expver=$expver,class=$class,stream=$stream,type=$type,levtype=$levtype,param=$param"
for date in $(seq -w $date_beg $date_end); do
    #1. pull the data
    gfile=$WDIR/daily_sum_${origin}_${type}_${levtype}_${date}_${param}.grib2
      ydat=$(newdate -D $date -1)
     #Following internal discussion, for param M
     #acc24(N) = acc0to6 + acc6to18 + acc18to24   where
     #acc0to6 = M(N-1;Z=12;t=18) - M(N-1;Z=12;t=12)
     #acc6to18 = M(N,Z=0,t=18) - M(N,Z=0,t=06)
     #acc18to24 = M(N,Z=12,t=12) - M(N,Z=12,t=06)
     mars << eof
     retrieve, $com, date=$ydat,time=12,step=18,fieldset=yd_12_18
     retrieve, $com, date=$ydat,time=12,step=12,fieldset=yd_12_12
     retrieve, $com, date=$date,time=00,step=18,fieldset=td_00_18
     retrieve, $com, date=$date,time=00,step=06,fieldset=td_00_06
     retrieve, $com, date=$date,time=12,step=12,fieldset=td_12_12
     retrieve, $com, date=$date,time=12,step=06,fieldset=td_12_06
     compute, formula="(yd_12_18 - yd_12_12) + (td_00_18 - td_00_06) + (td_12_12 - td_12_06)",
     target="$gfile"
eof


    chmod 755 $gfile
done #day

done #parameter

}

do_one_param()
{
echo "Doing mars retrieval and means calculation for the period $date_beg to $date_end for $param"
com="origin=$origin,expver=$expver,class=$class,stream=$stream,type=$type,levtype=$levtype,param=$param"
for date in $(seq -w $date_beg $date_end); do
    #1. pull the data
    gfile=$WDIR/daily_sum_${origin}_${type}_${levtype}_${date}_${param}.grib2
    if [[ -f $gfile ]] && [[ $RE_WRITE == 0 ]] ; then
     echo "$gfile is already downloaded"
     else 
     echo "Going to pull the data for $gfile"
      ydat=$(newdate -D $date -1)
     #Following internal discussion, for param M
     #acc24(N) = acc0to6 + acc6to18 + acc18to24   where
     #acc0to6 = M(N-1;Z=12;t=18) - M(N-1;Z=12;t=12)
     #acc6to18 = M(N,Z=0,t=18) - M(N,Z=0,t=06)
     #acc18to24 = M(N,Z=12,t=12) - M(N,Z=12,t=06)
     mars << eof
     retrieve, $com, date=$ydat,time=12,step=18,fieldset=yd_12_18
     retrieve, $com, date=$ydat,time=12,step=12,fieldset=yd_12_12
     retrieve, $com, date=$date,time=00,step=18,fieldset=td_00_18
     retrieve, $com, date=$date,time=00,step=06,fieldset=td_00_06
     retrieve, $com, date=$date,time=12,step=12,fieldset=td_12_12
     retrieve, $com, date=$date,time=12,step=06,fieldset=td_12_06
     compute, formula="(yd_12_18 - yd_12_12) + (td_00_18 - td_00_06) + (td_12_12 - td_12_06)",
     target="$gfile"
eof
     fi

    chmod 755 $gfile
done #day
}

if [ -z $param ]; then
  echo "Doing all set of params"
  do_all_prams
else
  echo "Doing $param"
  do_one_param
fi
