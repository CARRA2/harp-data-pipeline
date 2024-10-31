#!/bin/bash

## script for amending CARRA GRIB headers for daily minmax, forecasts

#set -evx
source ./env.sh
source ./load_eccodes.sh

#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

DBASE=marsscratch

extract_param()
{
FILT_FILE=$WRK/tmp_${LEVTYPE}_${PARAM}_${DATE}.grib2
cat >  filter_var_${PERIOD} << EOF
if ( param == $PARAM )
{
  write "${FILT_FILE}";
}
EOF
grib_filter filter_var_${PERIOD} $FILE
}

archive_param()
{
mars << EOF
archive,source="$FILB",database=$DBASE,
    DATE       = $DATE,
    TIME       = 0000,
    ORIGIN     = $ORIGIN,
    STEP       = 3,
    LEVELIST   = $LEVELS,
    PARAM      = $PARAM,
    EXPVER     = prod,
    CLASS      = RR,
    LEVTYPE    = $LEVTYPE,
    TYPE       = FC,
    STREAM     = DAME,
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


ml eclib

if [ -z $1 ]; then
  echo "Please privide 2 args:"
  echo "period for daily mean in format YYYYMM"
  echo "origin (no-ar-ce or no-ar-cw)"
  exit
else
  PERIOD=$1
  ORIGIN=$2
fi

YYYY=$(substring $PERIOD 1 4) #substring is parf ot the eclib tools
MM=$(substring $PERIOD 5 6)
maxday_month # gives MAXDAY, requires MM and YYYY to be set
date_beg=${PERIOD}01
date_end=${PERIOD}${MAXDAY}
ENDYEAR=${date_beg:0:4}
ENDMONTH=${date_beg:4:2}
PATH_DATA=$MEANS_OUTPUT/$ORIGIN/$ENDYEAR/$ENDMONTH
WRK=$PATH_DATA/archive_minmax
[ ! -d $WRK ] && mkdir -p $WRK
echo "Updating headers and archiving data of daily minmax/insta for $PERIOD of $ORIGIN in $PATH_DATA"

RULES=grb_head_chng_daily_minmax_fc_rules
RULED=${RULES}_${PERIOD}_${ORIGIN}

ITIME=00
DTIME=0
for DATE in $(seq -w $date_beg $date_end); do
  echo "Processing $DATE"
  IDATETIME=${DATE}${ITIME}
  JDATETIME=$( newdate $IDATETIME 24 )
  DDATE=$DATE
  ENDYEAR=${JDATETIME:0:4}
  ENDMONTH=${JDATETIME:4:2}
  ENDDAY=${JDATETIME:6:2}
  ENDHOUR=${JDATETIME:8:2}
  
  cp $RULES $RULED
  sed -i "s/DDATE/$DDATE/" $RULED
  sed -i "s/DTIME/$DTIME/" $RULED
  sed -i "s/ENDYEAR/$ENDYEAR/" $RULED
  sed -i "s/ENDMONTH/$ENDMONTH/" $RULED
  sed -i "s/ENDDAY/$ENDDAY/" $RULED
  sed -i "s/ENDHOUR/$ENDHOUR/" $RULED
  LEVTYPE=sfc

  for FILE in $(ls $PATH_DATA/daily_minmax_${ORIGIN}_fc_sfc_${DATE}.grib2); do

      #extract all parameters and change the headers separately
     PARAMS=$(grib_ls -p param $FILE | sort -u | grep -v messages | grep -v grib2 | grep -v para | sort -n)
     OUT=$(grib_ls -p level $FILE  | sort -u | grep -v messages | grep -v grib2 | grep -v lev | sort -n)
     LEVELS=$(echo $OUT | sed "s# #/#g")
     echo $LEVELS
    for PARAM in ${PARAMS}; do
      echo "Extracting $PARAM from $FILE to $FILT_FILE"
      extract_param

      FILB=$(echo $FILT_FILE|sed -e "1s/.grib2/_new.grib2/")
      echo "Updating the headers in $FILT_FILE. Writing to $FILB"
      grib_filter -o $FILB $RULED $FILT_FILE
      EXPECT=$(grib_count $FILB)

      # archive the parameter 
      echo "Archiving $PARAM to $DBASE"
      OUT=$(archive_param)
      error_log
    done # PARAM
  done #FILE
done #DATE
echo "Removing temporary file $RULED"
rm $RULED

echo "Removing temporary directory $WRK"
rm -rf $WRK

