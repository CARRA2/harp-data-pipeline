#!/usr/bin/env bash
#SBATCH --error=verif.%j.err
#SBATCH --output=verif.%j.out
#SBATCH --job-name=verification
#SBATCH --qos=nf
#SBATCH --mem-per-cpu=64000
#SBATCH --account="c3srrp"

source ../../config/config.aa


module load R/4.2.2
# Switch to renv environment
# export R_LIBS_USER=/home/nhd/R/harp-verif/renv/library/R-4.2/x86_64-pc-linux-gnu


copy_plots() {
#Copy the png files to the VM
PERIOD=${IDATE}-${EDATE}
PLOTS=/ec/res4/scratch/nhd/verification/plots/CARRA2
VM_PATH=/srv/shiny-server/carra2_app/plots/$STREAM/${PERIOD}
TO_SEND=$PLOTS/${PERIOD}
if [ -d $TO_SEND ]; then 
  chmod -R 755 $TO_SEND
  echo "Transferring files of $STREAM for $PERIOD"
  rsync -vaux $TO_SEND/ tenantadmin@136.156.128.148:$VM_PATH
else
  echo "$TO_SEND does not exist for $STREAM!"
fi
}

copy_vprof_plots() {
#Copy the png from the vertical verification
PERIOD=${IDATE}-${EDATE}
PLOTS=/ec/res4/scratch/nhd/verification/plots/ERA5_profiles
VM_PATH=/srv/shiny-server/carra2_app/plots/$STREAM/${PERIOD}
TO_SEND=$PLOTS/${PERIOD}
if [ -d $TO_SEND ]; then 
  chmod -R 755 $TO_SEND
  echo "Transferring ERA5 vertical profile special comparison of $STREAM for $PERIOD"
  rsync -vaux $TO_SEND/ tenantadmin@136.156.128.148:$VM_PATH
else
  echo "$TO_SEND does not exist for $STREAM!"
fi
}


run_verif_current() {
PARAMS=verification/set_params_carra2.R
CONFIG=config_local/config_carra2_prod.yml
### CONFIG=config_local/config_carra2_test.yml #for testing only
STREAMS=($(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | awk '{print $1}'))
cd ${HARP_DIR}/verification
#for STREAM in $ECFPROJ_STREAMS; do
for STREAM in ${STREAMS[@]}; do
  IDATE=$(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | grep $STREAM | awk '{print $2}')
  EDATE=$(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | grep $STREAM | awk '{print $3}')
  #the verification will always start at the beginning of the current month
  #IDATE=${IDATE:0:6}0100
  echo "verification of $STREAM for ${IDATE}-${EDATE} using $CONFIG and $PARAMS"
  Rscript point_verif.R -config_file $CONFIG -start_date $IDATE -end_date $EDATE -params_file $PARAMS
  Rscript point_verif.R -config_file $CONFIG -start_date $IDATE -end_date $EDATE -params_file $PARAMS -params_list T2m,S10m
  Rscript point_verif.R -config_file $CONFIG -start_date $IDATE -end_date $EDATE -params_file $PARAMS -params_list RH2m,Pmsl
  Rscript point_verif.R -config_file $CONFIG -start_date $IDATE -end_date $EDATE -params_file $PARAMS -params_list CCtot,AccPcp12h
  Rscript point_verif.R -config_file $CONFIG -start_date $IDATE -end_date $EDATE -params_file $PARAMS -params_list S,T

# Do the ERA5 vertical profiles comparison
# Will only work if I previously did the FCTABLE processing for the vfld ERA5 separate path for the profiles
# See the script ./convert_data_for_profiles.sh in/perm/nhd/R/harp-verif/pre_processing for details
echo "Doing the vertical profile verification using ERA5 for $STREAM on period $IDATE $EDATE"
./run_verif_era5_only_vprofs.sh $IDATE $EDATE
echo "Done with the vertical profile verification using ERA5"
done
cd -
}

copy_current_plots() {
#to copy the plots only
STREAMS=($(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | awk '{print $1}'))
#for STREAM in $ECFPROJ_STREAMS; do
for STREAM in ${STREAMS[@]}; do
  IDATE=$(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | grep $STREAM | awk '{print $2}')
  EDATE=$(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | grep $STREAM | awk '{print $3}')
  IDATE=$(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | grep $STREAM | awk '{print $2}')
  EDATE=$(cat $ECFPROJ_LIB/bash/job_submitters/$PROGFILE | grep $STREAM | awk '{print $3}')
  #the verification will always start at the beginning of the current month
  #IDATE=${IDATE:0:6}0100
  echo "Copying harp verification for ${IDATE}-${EDATE}"
  copy_plots
  copy_vprof_plots #this one only for the vertical profiles special comparison
done
cd -
}

#run_verif_selected() {
#PARAMS=verification/set_params_carra2.R
#CONFIG=config_local/config_carra2_prod.yml
#cd ${HARP_DIR}/verification
#for STREAM in $ECFPROJ_STREAMS; do

#run_verif_selected() {
#PARAMS=verification/set_params_carra2.R
#CONFIG=config_local/config_carra2_prod.yml
#cd ${HARP_DIR}/verification
#for STREAM in $ECFPROJ_STREAMS; do
#  echo "Running harp verification for ${IDATE}-${EDATE} using $CONFIG and $PARAMS"
#  Rscript point_verif.R -config_file $CONFIG -start_date $IDATE -end_date $EDATE -params_file $PARAMS
#  copy_plots
#done
#cd -
#}
#

if [[ -z $1 ]]; then
  PROGFILE=periods.txt
  echo "Doing verification for current period in $PROGFILE"
  run_verif_current
  echo "Copying all plots for periods in $PROGFILE"
  copy_current_plots
else
  PROGFILE=$1
  echo "Doing verification for selected period in $PROGFILE"
  run_verif_current
  echo "Copying all plots for periods in $PROGFILE"
  copy_current_plots
fi

