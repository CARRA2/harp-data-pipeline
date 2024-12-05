#!/bin/bash

# CLI arguments:
SDATE=$1
EDATE=$2
MODEL=$3

# Remove - for testing purposes
if [ "$#" -eq 3 ]; then
  ARCHIVE_FCTABLE=$3
fi

VFLD_TRG=${VFLD_DIR}/${MODEL}
[ -d ${VFLD_TRG} ] || mkdir -p ${VFLD_TRG}
cd ${VFLD_TRG}

# Define a function to untar files for local models
fn_local_untar() {
  
  PATHIN=$1
  TNAMEIN=$2
  YMDIN=$3

  numfiles=$(ls ${PATHIN}/vfld${TNAMEIN}${YMDIN}??.tar.gz | wc -l)

  if [ "$numfiles" -gt 0 ]; then
    gunzip -f *.gz
    find . -type f -name "*.tar" -exec tar -xf {} \;
    rm -f vfld${TNAMEIN}${YMDIN}*.tar
    rm -f vfld${TNAMEIN}${YMDIN}*.tar.gz
  else
    echo "Could not retrieve vfld${TNAMEIN}${YMDIN}??.tar.gz from archive, exiting"
    exit 1
  fi

}

copy_CARRA2()
{
  DEST=/ec/res4/scratch/nhd/verification/vfld/CARRA2/$STREAM/
  [ ! -d $DEST ] && mkdir $DEST
  ORIG=/ec/res4/scratch/fac2/hm_home/$STREAM/archive/extract/
  echo "Copying tarballs from $ORIG to $DEST and unpacking..."
  cp $ORIG/*tar.gz $DEST
  cd $DEST
  for TB in vfld*.tar.gz; do
    tar zxvf $TB
  done
  rm *.tar.gz
  cd -
}

copy_CARRA2_ecfs()
{
  DEST=/ec/res4/scratch/nhd/verification/vfld/CARRA2/$STREAM/
  ORIG=ec:/fac2/CARRA2/vfld/$STREAM/$PERIOD
  echo "ecfscopying $ORIG to $DEST/$PERIOD and unpacking... (this might take a while)"
  ecfsdir $ORIG $DEST/$PERIOD
  mv $DEST/$PERIOD/* $DEST
  rmdir $DEST/$PERIOD
  cd $DEST
  tar xvf vfld${STREAM}${PERIOD}.tar
  rm vfld${STREAM}${PERIOD}.tar
  for TB in vfld${STREAM}${PERIOD}*.tar.gz; do
    tar zxvf $TB
  done
  rm vfld${STREAM}${PERIOD}*.tar.gz
  cd -
}

copy_ERA5()
{
  DEST=/ec/res4/scratch/nhd/verification/vfld/ERA5
  [ ! -d $DEST ] && mkdir $DEST
  if [[ $YYYY <  2000 ]] ; then
    [ -d $DEST/$YYYY ] && rmdir $DEST/$YYYY
    ORIG=ec:/hirlam/oprint/ECMWF/ERA5/$YYYY/$PERIOD
    echo "ecfscopying $ORIG to $DEST/$PERIOD and unpacking... (this might take a while)"
    ecfsdir $ORIG $DEST/$YYYY
    mv $DEST/$YYYY/* $DEST
    rmdir $DEST/$YYYY
  else
  [ -d $DEST/$PERIOD ] && rmdir $DEST/$PERIOD
    ecfsdir ec:/hirlam/oprint/ECMWF/ERA5/$YYYY/$PERIOD $DEST/$PERIOD
    mv $DEST/$PERIOD/* $DEST
    rmdir $DEST/$PERIOD
  fi
}


######################################################
# Retrieve the vfld data
######################################################

#if [[ $MODEL == *"carra2"* ]]; then
#  echo "Copying the data for $MODEL"
#  copy_CARRA2
#elif [ "$MODEL" == "ERA5" ]; then
#  copy_ERA5
#else
#  echo "The model $MODEL is not considered, exiting"
#  exit 1
#
#fi

######################################################
# Do some file renaming
######################################################

######################################################
# Now do the conversion of vfld to sqlite
######################################################

cd $HARP_DIR
if [[ $MODEL == *"carra2"* ]]; then
  CONFIG=config_local/config_carra2_prod.yml
  echo "Start vfld conversion to sqlite for period $SDATE $EDATE for $MODEL"
  echo "Using $CONFIG"
  ${HARP_DIR}/pre_processing/vfld2sql.R -start_date $SDATE -end_date $EDATE -config_file $CONFIG
elif [[ $MODEL == "ERA5" ]]; then
  CONFIG=config_local/config_ERA5_prod.yml
  echo "Start vfld conversion to sqlite for period $SDATE $EDATE for $MODEL"
  echo "Using $CONFIG"
  ${HARP_DIR}/pre_processing/vfld2sql.R -start_date $SDATE -end_date $EDATE -config_file $CONFIG
else
  echo "Conversion not considered for $MODEL on period $SDATE $EDATE"
  exit 1
fi

######################################################
# Archive the vfld data on ECFS
######################################################

if [ "$MODEL" != "carra2" ]; then

echo "archiving not ready yet"

else 

  echo "Do not archive IFS vfld"

fi

######################################################
# End
######################################################

echo "Finished $MODEL vfld to sql conversion, exiting"

sleep 5
exit 0
