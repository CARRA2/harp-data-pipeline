#!/usr/bin/env bash
#SBATCH --job-name=sql_proc
#SBATCH --qos=nf
#SBATCH --error=sql_job-%j.err
#SBATCH --output=sql_job-%j.out

source ../../config/config.aa

check_progress()
{
  if [ ! -f $ECFPROJ_LIB/go/count_dates ]; then
    ml go
    go build $ECFPROJ_LIB/go/count_date.go
    module unload go
  fi
  $ECFPROJ_LIB/go/count_dates
}
run_vfld()
{
for MODEL in $ECFPROJ_STREAMS; do
PERIOD=$(cat $ECFPROJ_LIB/bash/job_submitters/periods.txt | grep $MODEL | awk '{print $2 " " $3}')
$ECFPROJ_LIB/bash/job_submitters/vfld2sql.sh $PERIOD $MODEL || exit 1
#$ECFPROJ_LIB/bash/job_submitters/vfld2sql.sh $PERIOD ERA5 || exit 1
done
}

run_vobs()
{
for MODEL in $ECFPROJ_STREAMS; do
echo $MODEL
PERIOD=$(cat $ECFPROJ_LIB/bash/job_submitters/periods.txt | grep $MODEL | awk '{print $2 " " $3}')
$ECFPROJ_LIB/bash/job_submitters/vobs2sql.sh $PERIOD || exit 1
done
}

#check_progress
run_vfld
#run_vobs
