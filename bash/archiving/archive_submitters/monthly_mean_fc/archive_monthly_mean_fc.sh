#!/bin/bash

source ./env.sh
source ./load_eccodes.sh

#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

DBASE=marser
ml eclib

extract_param()
{
FILT_FILE=$WRK/tmp_${PARAM}_${DATE}.grib2
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

mars   << EOF
    ARCHIVE,
    CLASS      = RR,
    TYPE       = FC,
    STREAM     = MODA,
    SOURCE     = "$FILB",
    DATE       = $DATE,
    TIME       = 2100,
    STEP       = 3,
    ORIGIN     = $ORIGIN,
    LEVELIST   = $LEVELS,
    PARAM      = $PARAM,
    EXPVER     = prod,
    LEVTYPE    = $LEVTYPE,
    DATABASE   = $DBASE,
    EXPECT     = $EXPECT
EOF
}

error_log()
{
if echo "$OUT" | grep -q "ERROR"; then
    # Capture the error message
    error_message=$(echo "$OUT" | grep "ERROR")

    echo "An error occurred:"
    echo "$error_message"

    # Optionally, you can write the error to a file
    #echo "$error_message" > error_log.txt

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
YYYY=${PERIOD:0:4}
MM=${PERIOD:4:2}
maxday_month # gives MAXDAY, requires MM and YYYY to be set
NDAYS=$MAXDAY

ITIME=00
IDAY=01
IDATETIME=${PERIOD}${IDAY}${ITIME}
JDATETIME=$( newdate $IDATETIME -3 )
DDATE=${JDATETIME:0:8}
DTIME=${JDATETIME:8:2}00


YYYY=${PERIOD:0:4}
MM=${PERIOD:4:2}
NDAYS=$MAXDAY

RULES=grb_head_chng_monthly_mean_fc_rules
RULED=${RULES}_${PERIOD}_${ORIGIN}

ENDYEAR=${PERIOD:0:4}
ENDMONTH=${PERIOD:4:2}
ENDDAY=$NDAYS
ENDHOUR=21
((LTR=NDAYS*24-3))   # range=end-start
#((LTR=NDAYS*24))
PATH_DATA=$MEANS_OUTPUT/$ORIGIN/$ENDYEAR/$ENDMONTH
WRK=$PATH_DATA/archive_monthly_fc
[ ! -d $WRK ] && mkdir -p $WRK
echo "Processing of monthly means of type fc for $PERIOD of $ORIGIN in $PATH_DATA"


cp $RULES $RULED
sed -i "s/DDATE/$DDATE/" $RULED
sed -i "s/DTIME/$DTIME/" $RULED
sed -i "s/ENDYEAR/$ENDYEAR/" $RULED
sed -i "s/ENDMONTH/$ENDMONTH/" $RULED
sed -i "s/ENDDAY/$ENDDAY/" $RULED
sed -i "s/ENDHOUR/$ENDHOUR/" $RULED
sed -i "s/LTR/$LTR/" $RULED



DATE=${PERIOD}01 #this is for monthly

LEVTYPE=sfc
FILE=$PATH_DATA/monthly_mean_${ORIGIN}_fc_sfc_${PERIOD}.grib2
PARAMS=$(grib_ls -p param $FILE | sort -u | grep -v messages | grep -v grib2 | grep -v para | sort -n)
for PARAM in ${PARAMS}; do
      extract_param
      echo "Extracting $PARAM from $FILE to $FILT_FILE"
      FILB=$(echo $FILT_FILE|sed -e "1s/.grib2/_new.grib2/")
      echo "Updating the headers in $FILT_FILE. Writing to $FILB"
      grib_filter -o $FILB $RULED $FILT_FILE
      # archive the parameter already
      echo "Archiving $PARAM to $DBASE"
      OUT=$(grib_ls -p level $FILB  | sort -u | grep -v messages | grep -v grib2 | grep -v lev | sort -n)
      LEVELS=$(echo $OUT | sed "s# #/#g")
      EXPECT=$(grib_count $FILB)
      OUT=$(archive_param)
      error_log
done
echo "Removing temporary file $RULED"
rm $RULED
echo "Removing temporary directory $WRK"
rm -rf $WRK

