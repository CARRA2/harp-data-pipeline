#!/bin/bash

## script for amending CARRA GRIB headers for monthly mean, analyses and then archive the data

#set -evx
source ./env.sh
source ./load_eccodes.sh

#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

DBASE=marser

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

echo $PARAM
if [[ $PARAM == 173 ]]; then
echo "Special case for 173 (roughness)"
LOCAL_PARAM=235244
elif [[ $PARAM == 260649 ]]; then
echo "Special case for 260649"
LOCAL_PARAM=263006
else
LOCAL_PARAM=$PARAM
fi
mars   << EOF
    ARCHIVE,
    CLASS      = RR,
    TYPE       = AN,
    STREAM     = MODA,
    ORIGIN     = $ORIGIN,
    EXPVER     = prod,
    LEVTYPE    = $LEVTYPE,
    LEVELIST  = $LEVELS,
    PARAM      = $LOCAL_PARAM,
    DATE       = $DATE,
    TIME       = 0000,
    STEP       = 0,
    SOURCE     = "$FILB",
    DISP       = N,
    DATABASE   = $DBASE,
    EXPECT     =$EXPECT 
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
  echo "origin (no-ar-pe, no-ar-ce or no-ar-cw)"
  exit
else
  PERIOD=$1
  ORIGIN=$2
fi


YYYY=${PERIOD:0:4}
MM=${PERIOD:4:2}
maxday_month # gives MAXDAY, requires MM and YYYY to be set
NDAYS=$MAXDAY

IDAY=01
DDATE=${PERIOD}$IDAY
DTIME=0

RULES=grb_head_chng_monthly_mean_an_rules
RULED=${RULES}_${PERIOD}_${ORIGIN}

ENDYEAR=${PERIOD:0:4}
ENDMONTH=${PERIOD:4:2}
ENDDAY=$NDAYS
ENDHOUR=21
((LTR=NDAYS*24-3))   # range=end-start

#PATH_DATA=$MEANS_OUTPUT/$ORIGIN/$ENDYEAR/$ENDMONTH
PATH_DATA=$MEANS_OUTPUT_FAC2/$ORIGIN/$ENDYEAR/$ENDMONTH
WRK=$MEANS_OUTPUT_FAC2/$ORIGIN/$ENDYEAR/$ENDMONTH/archive_monthly

[ ! -d $WRK ] && mkdir -p $WRK

echo "Processing of monthly means of type an for $PERIOD of $ORIGIN in $PATH_DATA"

cp $RULES $RULED
sed -i "s/DDATE/$DDATE/" $RULED
sed -i "s/DTIME/$DTIME/" $RULED
sed -i "s/ENDYEAR/$ENDYEAR/" $RULED
sed -i "s/ENDMONTH/$ENDMONTH/" $RULED
sed -i "s/ENDDAY/$ENDDAY/" $RULED
sed -i "s/ENDHOUR/$ENDHOUR/" $RULED
sed -i "s/LTR/$LTR/" $RULED

DATE=$DDATE #only one date here, needs to be set for the FFILT file and the archivin part
for LEVTYPE in sfc ml pl hl; do
for FILE in $(ls $PATH_DATA/monthly_mean_${ORIGIN}_an_${LEVTYPE}_${PERIOD}.grib2) ;do
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
       # grib_filter -o $FILB $RULED $FILE
      rm $FILB
    done #PARAM
done #FILE
done #LEVTYPE
echo "Removing temporary file $RULED"
rm $RULED

echo "Removing temporary directory $WRK"
rm -rf $WRK
