#!/bin/bash

## script for amending CARRA GRIB headers for daily mean, forecasts and then archiving data to MARS

#set -evx
source ./env.sh
source ./load_eccodes.sh

#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

DBASE=marsscratch

archive_param()
{
mars   << EOF
    ARCHIVE,
    CLASS      = RR,
    TYPE       = FC,
    STREAM     = DAME,
    ORIGIN     = $ORIGIN,
    EXPVER     = prod,
    LEVTYPE    = $LEVTYPE,
    PARAM      = $PARAM,
    DATE       = $DATE,
    TIME       = 2100,
    STEP       = 3,
    SOURCE     = "$FILB",
    DISP       = N,
    DATABASE   = $DBASE,
    EXPECT     = $EXPECT
EOF
}

error_log()
{
ERROR_LOG=errors_${PERIOD}_${ORIGIN}.txt
[ ! -f $ERROR_LOG ] && touch $ERROR_LOG

if echo "$OUT" | grep -q "ERROR"; then
    # Capture the error message
    error_message=$(echo "$OUT" | grep "ERROR")
    
    echo "An error occurred:"
    echo "$error_message"
    
    # Optionally, you can write the error to a file
    echo "$error_message" >> $ERROR_LOG
    
    # Exit with a non-zero status to indicate an error
    # exit 1
    
else
    echo "Archival of $PARAM completed successfully."
    # Optionally, you can do something with the successful output
    #echo "$output"
fi
}

if [ -z $1 ]; then
  echo "Please privide 2 args:"
  echo "period for daily mean in format YYYYMM"
  echo "origin (no-ar-ce or no-ar-cw)"
  exit
else
  PERIOD=$1
  ORIGIN=$2
fi

ml eclib #required for subscript below
YYYY=$(substring $PERIOD 1 4) #substring is parf ot the eclib tools
MM=$(substring $PERIOD 5 6)
maxday_month # gives MAXDAY, requires MM and YYYY to be set
date_beg=${PERIOD}01
date_end=${PERIOD}${MAXDAY}
ENDHOUR=21
ITIME=00


RULES=grb_head_chng_daily_mean_fc_rules
RULED=${RULES}_${PERIOD}_${ORIGIN}
ENDYEAR=${date_beg:0:4}
ENDMONTH=${date_beg:4:2}

PATH_DATA=$MEANS_OUTPUT/$ORIGIN/$ENDYEAR/$ENDMONTH/
echo "Processing fc/insta for $PERIOD of $ORIGIN in $PATH_DATA"

LEVTYPE=sfc
echo "Doing level $LEVTYPE"
#PATH_DATA=$MEANS_OUTPUT/$ORIGIN/$ENDYEAR/$ENDMONTH/${LEVTYPE^^}
for DATE in $(seq -w $date_beg $date_end); do
  echo "Processing $DATE"
  IDATETIME=${DATE}${ITIME}
  JDATETIME=$( newdate $IDATETIME -3 )
  DDATE=${JDATETIME:0:8}
  DTIME=${JDATETIME:8:2}00

  ENDYEAR=${DATE:0:4}
  ENDMONTH=${DATE:4:2}
  ENDDAY=${DATE:6:2}
  cp $RULES $RULED
  sed -i "s/DDATE/$DDATE/" $RULED
  sed -i "s/DTIME/$DTIME/" $RULED
  sed -i "s/ENDYEAR/$ENDYEAR/" $RULED
  sed -i "s/ENDMONTH/$ENDMONTH/" $RULED
  sed -i "s/ENDDAY/$ENDDAY/" $RULED
  sed -i "s/ENDHOUR/$ENDHOUR/" $RULED

  for FILE in $(ls $PATH_DATA/daily_mean_${ORIGIN}_fc_${LEVTYPE}_${DATE}.grib2) ;do
    FILB=$(echo $FILE|sed -e "1s/.grib2/_new.grib2/")
    grib_filter -o $FILB $RULED $FILE
    EXPECT=$(grib_count $FILB)
      PARAMS=$(grib_ls -p param $FILE | sort -u | grep -v messages | grep -v grib2 | grep -v para | sort -n)
     OUT=$(grib_ls -p level $FILE  | sort -u | grep -v messages | grep -v grib2 | grep -v lev | sort -n)
     LEVELS=$(echo $OUT | sed "s# #/#g")
     for PARAM in ${PARAMS}; do
      archive_param
    done
  done #FILE

done #DATE
echo "Removing temporary file $RULED"
rm $RULED

