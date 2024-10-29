#!/bin/bash
# BACKLOG for SFC levels, AN files
# All data is staged, retrieved and processed below. The output is one single file
# The output is one file per day with all the daily means for the given level type

#source env.sh #set some environment variables below
source $ECFPROJ_LIB/share/config/config.aa

#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

#ml ecmwf-toolbox #eccodes and the like
source ../../load_eccodes.sh
ml eclib # includes scripts like newdata to get correct dates, including leap years
#source $HOME/bin/utils.sh #a few other functions I need below
#gmean=/home/nhd/scripts/carra/carra_means/src/grib_mean.x setting this up in env.sh now

expver=prod
class=rr
stream=oper
type=an  
step=3  
levtype=sfc
levelist=off
#removed here static variables: 31/172/173/228002
#Also removed: 174008 (Surface runoff) and 260430 (Percolation), since they are accumulated
param=$CARRA_PAR_AN_SFC


if [[ -z $1 ]]; then
  echo "Please provide period and domain to process"
  echo "Example: 202106 no-ar-pa"
  exit 1
else
  period=$1
  origin=$2
  day_beg=$3
  day_end=$4
  #If I give two extra arguments it will set the initial and final day of the month
  #Otherwise it will do the whole month. This is just for testing
  YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
  MM=$(substring $period 5 6)
  maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
  [ -z $day_beg ] && day_beg=01
  [ -z $day_end ] && day_end=$MAXDAY
fi

YYYY=$(substring $period 1 4) #substring is parf ot the eclib tools
MM=$(substring $period 5 6)

WDIR=$MEANS_OUTPUT/$origin/$YYYY/$MM/SFC ; [[ ! -d $WDIR ]] && mkdir -p $WDIR

maxday_month #from $HOME/bin/utils.sh, requires MM and YYYY to be set
#get the date of yesterday for the start of the period
date_end=${period}${day_end}
date_beg=${period}${day_beg}

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
    #1. pull the data
    gfile=$WDIR/${origin}_${type}_${levtype}_${date}.grib2
    #if [[ ! -s $gfile ]] ; then
      ydat=$(newdate -D $date -1)
     com="origin=$origin,expver=$expver,class=$class,stream=$stream,type=$type,step=$step,levtype=$levtype,levelist=$levelist,param=$param"
     mars << eof
     retrieve, $com, date=$date,time=0/to/21/by/3,target="$gfile"
eof
    #fi
  base=$(basename $gfile)
  mfile=$WDIR/daily_mean_${base}
  #$gmean -k date,time -i $gfile -o $mfile -s date=$date,time=00,step=24 -n 8
  $gmean -k time,step -i $gfile -o $mfile -n 8
  #ls -lh $mfile $gfile
  chmod 755 $mfile

done
#remove the temporary input files
rm -f $WDIR/${origin}_${type}_${levtype}_*.grib2

#move the daily means to the main path
mv $WDIR/daily_mean*grib2 $MEANS_OUTPUT/$origin/$YYYY/$MM/
rmdir $WDIR
