#!/bin/bash

# CLI arguments:
SDATE=$1
EDATE=$2

CONFIG=config_local/config_carra2_prod.yml
cd $HARP_DIR
echo "Start conversion of vobs to sqlite"
${HARP_DIR}/pre_processing/vobs2sql.R -start_date $SDATE -end_date $EDATE -config_file $CONFIG

######################################################
# Archive the vfld data on ECFS
######################################################
echo "archiving not ready yet"
######################################################
# End
######################################################

echo "Finished $MODEL vfld to sql conversion, exiting"

sleep 5
exit 0