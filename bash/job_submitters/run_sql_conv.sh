#!/usr/bin/env bash
#SBATCH --job-name=sql_proc
#SBATCH --qos=nf
#SBATCH --error=sql_job-%j.err
#SBATCH --output=sql_job-%j.out

source ../../config/config.aa

$ECFPROJ_LIB/go/count_dates

exit 0

run_vfld()
{
for MODEL in $ECFPROJ_STREAMS; do
echo $MODEL
PERIOD=$(cat $ECFPROJ_LIB/go/periods.txt | grep $MODEL | awk '{print $2 " " $3}')
$ECFPROJ_LIB/bash/job_submitters/vfld2sql.sh $PERIOD $MODEL || exit 1
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

run_vfld
run_vobs
