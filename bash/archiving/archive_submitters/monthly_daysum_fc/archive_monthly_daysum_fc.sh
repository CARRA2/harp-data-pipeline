#!/bin/bash

## script for amending CARRA GRIB headers for monthly daysum, forecasts and then archiving them to mars or marscratch

#set -evx
source ./env.sh
source ./load_eccodes.sh

#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS

DBASE=marsscratch

archive_params()
{


mars   << EOF
archive,source="$FILB",database=$DBASE,
   DATE       = $DATE,
   TIME       = 1200,
   ORIGIN     = $ORIGIN,
   LEVELIST   = $LEVELS,
   PARAM      = $PARAM,
   EXPVER     = prod,
   CLASS      = RR,
   LEVTYPE    = $LEVTYPE,
   TYPE       = FC,
   STREAM     = MODA,
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

module load eclib

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
PATH_DATA=$MEANS_OUTPUT/$ORIGIN/$YYYY/$MM/SUMS


RULES=grb_head_chng_monthly_daysum_fc_rules
RULED=${RULES}_${PERIOD}_${ORIGIN}

ITIME=00
IDAY=01
IDATETIME=${PERIOD}${IDAY}${ITIME}
JDATETIME=$( newdate $IDATETIME -12 )
DDATE=${JDATETIME:0:8}
DTIME=${JDATETIME:8:2}00

IDATETIME=${PERIOD}${NDAYS}${ITIME}
JDATETIME=$( newdate $IDATETIME 24 )
ENDYEAR=${JDATETIME:0:4}
ENDMONTH=${JDATETIME:4:2}
ENDDAY=${JDATETIME:6:2}
ENDHOUR=${JDATETIME:8:2}
((LTRM=(NDAYS-1)*24))     # range=end-start
#((LTRM=NDAYS*24))

cp $RULES $RULED
sed -i "s/DDATE/$DDATE/" $RULED
sed -i "s/DTIME/$DTIME/" $RULED
sed -i "s/ENDYEAR/$ENDYEAR/" $RULED
sed -i "s/ENDMONTH/$ENDMONTH/" $RULED
sed -i "s/ENDDAY/$ENDDAY/" $RULED
sed -i "s/ENDHOUR/$ENDHOUR/" $RULED
sed -i "s/LTRM/$LTRM/" $RULED

#get the parameters 
# Set the Internal Field Separator to "/"
IFS='/'
# Read the string into an array
read -ra PARAMS <<< "$CARRA_PAR_FC_ACC"
unset IFS #Otherwise this will fuck up my loops below

WRK=$MEANS_OUTPUT_FAC2/$ORIGIN/$ENDYEAR/$ENDMONTH
[ ! -d $WRK ] && mkdir -p $WRK

for PARAM in ${PARAMS[@]}; do

FILE=$PATH_DATA/monthly_mean_accum_${ORIGIN}_fc_sfc_${PERIOD}_${PARAM}.grib2


FILB=$(echo $FILE|sed -e "1s/.grib2/_new.grib2/")
FILE_LOCAL=$WRK/$(basename $FILB)
FILB=$FILE_LOCAL
# change headers
grib_filter -o $FILB $RULED $FILE
echo "Updating headers and archiving of monthly of dailysums for $PERIOD of $ORIGIN in $FILB"

#archive
LEVTYPE=sfc
DATE=${PERIOD}01 #only one date here, needs to be set for the FFILT file and the archiving part
OUT=$(grib_ls -p param $FILE | sort -u | grep -v messages | grep -v grib2 | grep -v para | sort -n)
PARAMS=$(echo $OUT | sed "s# #/#g")

OUT=$(grib_ls -p level $FILE  | sort -u | grep -v messages | grep -v grib2 | grep -v lev | sort -n)
LEVELS=$(echo $OUT | sed "s# #/#g")

EXPECT=$(grib_count $FILE)

archive_params

done #PARAM

echo "Removing temporary file $RULED"
rm $RULED
