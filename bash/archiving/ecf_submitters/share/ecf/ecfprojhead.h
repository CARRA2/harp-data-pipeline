# Some generic varibales to load
TASK=%TASK%             export TASK
ECF_TRYNO=%ECF_TRYNO%   export ECF_TRYNO
CARRA_PERIOD=%CARRA_PERIOD%         export CARRA_PERIOD
ECFPROJ_LIB=%ECFPROJ_LIB%     export ECFPROJ_LIB
MEANS_SCR=%MEANS_SCR%     export MEANS_SCR

export ECF_PARENT ECF_GRANDPARENT
ECF_PARENT=$( perl -e "@_=split('/','$ECF_NAME');"'print $_[$#_-1]' )
ECF_GRANDPARENT=$( perl -e "@_=split('/','$ECF_NAME');"'print $_[$#_-2]' )

# Source config
. %ECFPROJ_LIB%/share/config/config.%ECFPROJ_CONFIG%

# Specific variables for this project
