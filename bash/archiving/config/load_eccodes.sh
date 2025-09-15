

#source ../../../../bin/utils.sh
source env.sh

export ecdefinitionpath=/perm/fa0e/hm_lib/CARRA2_v1/util/gl/definitions
export sfxdefinitionpath=/perm/nhab/carra2dir/marsify_final/definitions
#this being used in CARRA2
#export ECCODES_DEFINITION_PATH=$sfxdefinitionpath

#module load ecmwf-toolbox
module load ecmwf-toolbox/2024.04.0.0 
#export ECCODES_DEFINITION_PATH=$ECCODES_DEFINITION_PATH:$sfxdefinitionpath
#export ECCODES_DEFINITION_PATH=$ECCODES_DEFINITION_PATH:$ecdefinitionpath
#export ECCODES_DEFINITION_PATH=$ecdefinitionpath


#export ECCODES_DEFINITION_PATH=$ecdefinitionpath


#module load ecmwf-toolbox/2024.02.0.0
#module load ecmwf-toolbox
# 202401: using these to test archiving of all parameters in mars
#module unload ecmwf-toolbox
#export PATH=/perm/marm/eccodes_2.33.1/bin:$PATH
#export ECCODES_DIR=/perm/marm/eccodes_2.33.1

