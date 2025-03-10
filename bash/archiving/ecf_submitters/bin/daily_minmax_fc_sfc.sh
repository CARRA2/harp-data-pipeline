#!/bin/bash
# BACKLOG for SFC levels, FC files
# All data is staged, retrieved and processed below. The output is one single file
# The output is one file per day with all the daily means for the given level type

# This file processes a few surface parameters not found in the analysis files.
# They are all min/max or some sort. The daily mean of the day is based on the
# min or max or the corresponding variable. See below for details

#source env.sh #set some environment variables below
#source $ECFPROJ_LIB/share/config/config.aa
if [ -f ./env.sh ]; then
  source ./env.sh
else
  source $ECFPROJ_LIB/share/config/config.aa
fi


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
step="1/2/3"
levtype=sfc
levelist=off
#these are the only insta params apperaring exclusively in fc files. Removed the static ones
#Currently considering only the min/max variables below:
#10m eastward wind gust since previous post-processing. CODE: 260646
#10m northward wind gust since previous post-processing. CODE: 260647
#10m wind gust since previous post-processing:          CODE: 49
#Maximum 2m temperature since previous post-processing  CODE: 201 
#Minimum 2m temperature since previous post-processing  CODE: 202

param=$CARRA_PAR_FC_SFC #This is only used in the mars "staging" part. The retrievals are done separately


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
#param="all"
echo "Doing mars staging for the period $date_beg to $date_end"
com="origin=$origin,expver=$expver,class=$class,stream=$stream,type=$type,step=$step,levtype=$levtype,levelist=$levelist,param=$param"
 mars << eof
     stage, $com, date=$alldates,time=0000/0300/0600/0900/1200/1500/1800/2100
eof

echo "Doing mars retrieval and means calculation for the period $date_beg to $date_end"
for date in $(seq -w $date_beg $date_end); do
    #writing the codes explicitly below, since I will use max or min depending on the variable
    #merging everything in one file at the end, since I do not want to re-write the variables
    # (maybe there is a way to do that)

    #tmax
    g_tmax=$WDIR/201_${origin}_${type}_${levtype}_${date}.grib2
    com="origin=$origin,date=$date,expver=$expver,class=$class,stream=$stream,type=$type,levtype=$levtype,levelist=$levelist,param=201,time=0000/0300/0600/0900/1200/1500/1800/2100,step=1/2/3"
     mars << eof
     retrieve, $com,fieldset=max2t
     compute, formula="max(max2t)",
     target="$g_tmax"
eof

    #tmin
    g_tmin=$WDIR/202_${origin}_${type}_${levtype}_${date}.grib2
    com="origin=$origin,date=$date,expver=$expver,class=$class,stream=$stream,type=$type,levtype=$levtype,levelist=$levelist,param=202,time=0000/0300/0600/0900/1200/1500/1800/2100,step=1/2/3"
     mars << eof
     retrieve, $com,fieldset=min2t
     compute, formula="min(min2t)",
     target="$g_tmin"
eof

    #10m wind gust
    g_wg=$WDIR/228029_${origin}_${type}_${levtype}_${date}.grib2
    com="origin=$origin,date=$date,expver=$expver,class=$class,stream=$stream,type=$type,levtype=$levtype,levelist=$levelist,param=228029,time=0000/0300/0600/0900/1200/1500/1800/2100,step=1/2/3"
    #com="origin=$origin,date=$date,expver=$expver,class=$class,stream=$stream,type=$type,levtype=$levtype,levelist=$levelist,param=228029,time=0000/0300/0600/0900/1200/1500/1800/2100,step=3/4/5"
     mars << eof
     retrieve, $com,fieldset=10fg
     compute, formula="max(10fg)",
     target="$g_wg"
eof
#
#    #10m eastward wind gust
#    g_wge=$WDIR/260646_${origin}_${type}_${levtype}_${date}.grib2
#    com="origin=$origin,date=$date,expver=$expver,class=$class,stream=$stream,type=$type,levtype=$levtype,levelist=$levelist,param=260646,time=0000/0300/0600/0900/1200/1500/1800/2100,step=1/2/3"
#     mars << eof
#     retrieve, $com,fieldset=10efg
#     compute, formula="max(10efg)",
#     target="$g_wge"
#eof
#
#    #10m northward wind gust
#    g_wgn=$WDIR/260647_${origin}_${type}_${levtype}_${date}.grib2
#    com="origin=$origin,date=$date,expver=$expver,class=$class,stream=$stream,type=$type,levtype=$levtype,levelist=$levelist,param=260647,time=0000/0300/0600/0900/1200/1500/1800/2100,step=1/2/3"
#     mars << eof
#     retrieve, $com,fieldset=10nfg
#     compute, formula="max(10nfg)",
#     target="$g_wgn"
#eof
    gfile=$WDIR/${origin}_${type}_${levtype}_${date}.grib2
    base=$(basename $gfile)
    mfile=$WDIR/daily_minmax_${base}
    cat $g_tmin $g_tmax $g_wg  > $mfile
  chmod 755 $mfile

done

#remove the temporary files
rm -f $WDIR/201_${origin}_${type}_${levtype}_*.grib2
rm -f $WDIR/202_${origin}_${type}_${levtype}_*.grib2
