#!/usr/bin/env bash

# BACKLOG for HL levels, AN files
# All data is staged, retrieved and processed below. The output is one single file
# The output is one file per day with all the daily means for the given level type
#source $ECFPROJ_LIB/share/config/config.aa
if [ -f ./env.sh ]; then
  source ./env.sh
else
  source $ECFPROJ_LIB/share/config/config.aa
fi


#source env.sh #set some environment variables below
#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

ml ecmwf-toolbox #eccodes and the like
ml eclib # includes scripts like newdata to get correct dates, including leap years

#gmean=/home/nhd/scripts/carra/carra_means/src/grib_mean.x now in env.sh

expver=prod
class=rr
stream=oper
type=an  
step=3 
levtype=hl
levelist="500/400/300/250/200/150/100/75/50/30/15" 
param=$CARRA_PAR_AN_HL

# Set the Internal Field Separator to "/"
IFS='/'
# Read the string into an array
read -ra all_params <<< "$param"
unset IFS #Otherwise this will fuck up my loops below


if [[ -z $1 ]]; then
  echo "Please provide period and domain to process"
  echo "Example: 202106 no-ar-pa"
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

#WDIR=$MEANS_OUTPUT/$origin/$YYYY/$MM ; [[ ! -d $WDIR/ ]] && mkdir -p $WDIR
WDIR=$MEANS_OUTPUT/$origin/$YYYY/$MM/HL ; [[ ! -d $WDIR ]] && mkdir -p $WDIR


maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
#get the date of yesterday for the start of the period
date_beg=${period}${day_beg}
date_end=${period}${day_end}

#including the date before the beginning for ydat below
#Call the mars staging first...
alldates="$date_beg/TO/$date_end"
#param="all"
echo "Doing mars staging for the period $date_beg to $date_end"
com="origin=$origin,expver=$expver,class=$class,stream=$stream,type=$type,step=$step,levtype=$levtype,levelist=$levelist,param=$param"
 mars << eof
     stage, $com, date=$alldates,time=0000/0300/0600/0900/1200/1500/1800/2100
eof

echo "Doing mars retrieval and means calculation for the period $date_beg to $date_end"
for date in $(seq -w $date_beg $date_end); do
for param in "${all_params[@]}"; do

    #1. pull the data
    #gfile=$WDIR/${origin}_${type}_${levtype}_${date}.grib2
    gfile=$WDIR/${origin}_${type}_${levtype}_${date}_${param}.grib2
      ydat=$(newdate -D $date -1)
     com="origin=$origin,expver=$expver,class=$class,stream=$stream,type=$type,step=$step,levtype=$levtype,levelist=$levelist,param=$param"
     mars << eof
     retrieve, $com, date=$date,time=0/to/21/by/3,target="$gfile"
eof
  base=$(basename $gfile)
  mfile=$WDIR/daily_mean_${base}

  $gmean -k time,step -i $gfile -o $mfile -n 8
  chmod 755 $mfile

done #param
done #date

#remove the temporary files
rm -f $WDIR/${origin}_${type}_${levtype}_*.grib2
