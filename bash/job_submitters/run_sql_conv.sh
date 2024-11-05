#!/usr/bin/env bash
#SBATCH --job-name=sqlconv_carra2
#SBATCH --qos=nf
#SBATCH --error=sqlconv_job-%j.err
#SBATCH --output=sqlconv_job-%j.out

source ../../config/config.aa
PROGFILE=periods.txt

check_progress()
{
  [ -f $PROGFILE ] && cp $PROGFILE periods_prev.txt
  if [ ! -f $ECFPROJ_LIB/go/data_preparation/count_dates ]; then
    ml go
    go build $ECFPROJ_LIB/go/data_preparation/count_date.go
    module unload go
  fi
  $ECFPROJ_LIB/go/data_preparation/count_dates
}
run_vfld()
{
STREAMS=($(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | awk '{print $1}'))
echo "Streams to process: ${STREAMS[@]}"
#for STREAM in $ECFPROJ_STREAMS; do
for STREAM in ${STREAMS[@]}; do
#NOTE: this only works for the case in which all stream names are different!
PERIOD=$(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | grep $STREAM | awk '{print $2 " " $3}')

# Option for when all streams have the same name (for example doing ERA5 for all periods)
HITS=$(echo $PERIOD | wc -l)
if [ $HITS > 1 ]; then
  echo "More than one hit in the file! All the streams are the same: ${STREAMS[@]}!"
  echo "Periods for $STREAM: $PERIOD"
  echo "Doing all periods for $STREAM and ignoring previous loop"
  PERIODS=$(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | grep ${STREAM} | awk '{print $2 "-" $3}')
  for P in ${PERIODS}; do
    P1=${P%%-*}    # Gets everything before the first "-"
    P2=${P#*-} 
    $ECFPROJ_LIB/bash/job_submitters/vfld2sql.sh ${P1} ${P2} ${STREAM}
  done
  echo "stop here"
  exit 0
else
  $ECFPROJ_LIB/bash/job_submitters/vfld2sql.sh $PERIOD $STREAM || exit 1
fi
#turn this on once in a while:
#$ECFPROJ_LIB/bash/job_submitters/vfld2sql.sh $PERIOD ERA5 || exit 1
done
}

run_vobs()
{
for MODEL in $ECFPROJ_STREAMS; do
echo $MODEL
PERIOD=$(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | grep $MODEL | awk '{print $2 " " $3}')
$ECFPROJ_LIB/bash/job_submitters/vobs2sql.sh $PERIOD || exit 1
done
}


if [[ -z $1 ]]; then
  echo "Updating periods in $PROGFILE"
  #check_progress
  run_vfld
  #this one does not change much. Maybe run once in a while?
  #run_vobs
else
  PROGFILE=$1
  echo "Using selected periods in $PROGFILE"
 #check_progress
  run_vfld
  #this one does not change much. Maybe run once in a while?
  #run_vobs
fi
