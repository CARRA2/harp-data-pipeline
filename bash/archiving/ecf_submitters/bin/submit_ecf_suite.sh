#!/usr/bin/env bash

if [ -z $1 ]; then
  echo "Please provide period in format YYYYMM"
  echo ">>>> TODO: if given a shorter length like YYYY, loop over all months in .sh scripts"
  echo "Additional option: give second argument as hours of the day if"
  echo "./ecfproj_start is calling create_suite_timed.py "
  echo "Use 0000 to start straight away"
exit 1
  else
  PERIOD=$1
  HHMM=$2
fi
# For daily means I do not need much memory, so using the config "means"
#./ecfproj_start -s 200001 -c means -f

#this one considers only one period
#./ecfproj_start -f -s 200001 -c means --origin "no-ar-ce"
#export HHMM="2350"

NAME_OF_SUITE="carra2_means"
export NBATCH=4
echo "Default value of NAME_OF_SUITE variable: $NAME_OF_SUITE"
echo "Splitting sums in $NBATCH pieces"
echo "Edit this script to change it"

#create new batch ecf script if not there
for i in $(seq 1 $NBATCH); do
  if [ ! -f ../share/ecf/daily_sum_fc_sfc_batch${i}.ecf ]; then
    MSCR=../share/ecf/daily_sum_fc_sfc_batch${i}.ecf
    echo "Creating missing ecf script $MSCR"
    sed "s/REPLACEBATCHNUMBER/$i/g" ../share/ecf/daily_sum_fc_sfc_batch_template.ecf > $MSCR
  fi
done
if [ -z $HHMM ]; then 
echo "Running standard suite"
echo "Name of the suite will be ${NAME_OF_SUITE}_$PERIOD"
./ecfproj_start -f -s $PERIOD -c means -e ${NAME_OF_SUITE}_$PERIOD
exit 0 #othewise it will jump to the next!
fi

if [ -n $2 ]; then
echo "Running timed suite. Setting up here HHMM=$HHMM"
echo "Name of the suite will be ${NAME_OF_SUITE}_$PERIOD"
export USE_TIMED=1
export HHMM=$2
./ecfproj_start -f -s $PERIOD -c means -e ${NAME_OF_SUITE}_$PERIOD
exit 0 #same as above
fi

# for daily means for ML i need more memory so I used the config ending in means_ml
