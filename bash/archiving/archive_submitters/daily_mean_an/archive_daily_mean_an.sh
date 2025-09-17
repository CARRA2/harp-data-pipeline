#!/bin/bash

## script for amending CARRA GRIB headers for daily mean, analyses and then archiving data to MARS

#set -evx
source ./env.sh
source ./load_eccodes.sh

#SBATCH --mem-per-cpu=16GB
#SBATCH --time=48:00:00
#SBATCH --account=$SBU_CARRA_MEANS


#find_differences()
#{
#
## Convert the strings into arrays
#IFS='/' read -ra ARRAY1 <<< "$CARRA_PAR_AN_SFC"
#IFS='/' read -ra ARRAY2 <<< "$CARRA_PAR_AN_SFC_ARXIV"
#
## Compare arrays element by element
#for i in "${!ARRAY1[@]}"; do
#    if [ "${ARRAY1[$i]}" != "${ARRAY2[$i]}" ]; then
#        echo "Position $i differs:"
#        echo "CARRA_PAR_AN_SFC: ${ARRAY1[$i]}"
#        echo "CARRA_PAR_AN_SFC_ARXIV: ${ARRAY2[$i]}"
#        echo "---"
#    fi
#done
#unset IFS
#
#}

#DBASE=marsscratch
DBASE=marser

extract_param()
{
FILT_FILE=$WRK/tmp_${LEVTYPE}_${PARAM}_${DATE}.grib2
cat >  filter_var_${PERIOD}_${LEVTYPE} << EOF
if ( param == $PARAM )
{
  write "${FILT_FILE}";
}
EOF
grib_filter filter_var_${PERIOD}_${LEVTYPE} $FILE
}

archive_param()
{
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
    STREAM     = DAME,
    ORIGIN     = $ORIGIN,
    EXPVER     = prod,
    LEVELIST   = $LEVELS,
    LEVTYPE    = $LEVTYPE,
    PARAM      = $LOCAL_PARAM,
    DATE       = $DATE,
    TIME       = 0000,
    STEP       = 0,
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
module unload eclib
maxday_month # gives MAXDAY, requires MM and YYYY to be set
date_beg=${PERIOD}01
date_end=${PERIOD}${MAXDAY}
ENDHOUR=21

RULES=grb_head_chng_daily_mean_an_rules
RULED=${RULES}_${PERIOD}_${ORIGIN}
ENDYEAR=${date_beg:0:4}
ENDMONTH=${date_beg:4:2}
PATH_DATA=$MEANS_OUTPUT/$ORIGIN/$ENDYEAR/$ENDMONTH
WRK=$PATH_DATA/archive_daily_an
[ ! -d $WRK ] && mkdir -p $WRK

echo "Processing an/insta for $PERIOD of $ORIGIN in $PATH_DATA"

PATH_DATA=$MEANS_OUTPUT/$ORIGIN/$ENDYEAR/$ENDMONTH/
for LEVTYPE in sfc hl ml pl; do
echo "Doing level $LEVTYPE"
#PATH_DATA=$MEANS_OUTPUT/$ORIGIN/$ENDYEAR/$ENDMONTH/${LEVTYPE^^}
for DATE in $(seq -w $date_beg $date_end); do
  echo "Processing $DATE"
  DDATE=$DATE
  DTIME=0
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

  for FILE in $(ls $PATH_DATA/daily_mean_${ORIGIN}_an_${LEVTYPE}_${DATE}.grib2) ;do
    #extract all parameters and change the headers separately
     PARAMS=$(grib_ls -p param $FILE | sort -u | grep -v messages | grep -v grib2 | grep -v para | sort -n)
     OUT=$(grib_ls -p level $FILE  | sort -u | grep -v messages | grep -v grib2 | grep -v lev | sort -n)
     LEVELS=$(echo $OUT | sed "s# #/#g")
    for PARAM in ${PARAMS}; do
      extract_param
      echo "Extracted $PARAM from $FILE to $FILT_FILE"

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
done #level type

echo "Removing temporary directory $WRK"
rm -rf $WRK
