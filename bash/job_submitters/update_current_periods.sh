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

check_progress
