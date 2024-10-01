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
for MODEL in $ECFPROJ_STREAMS; do
PERIOD=$(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | grep $MODEL | awk '{print $2 " " $3}')
$ECFPROJ_LIB/bash/job_submitters/vfld2sql.sh $PERIOD $MODEL || exit 1
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

#check_progress
PROGFILE=periods_selected.txt
run_vfld
exit
#this one does not change much. Maybe run once in a while?
run_vobs
