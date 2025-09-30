#Set all the parameters to be processed for each daily_mean script
#This file is sourced by all of them
#source /perm/nhd/CARRA2/harp-data-pipeline/config/config.aa

ARPROJ_LIB="/perm/nhd/CARRA2/harp-data-pipeline"

#some functions I use for the dates. Copy locally if needed
#if [ -z ${ARPROJ_LIB} ]; then
#echo "Please source harp-data-pipeline/config/config.aa"
#exit 1
#else
#source ${ARPROJ_LIB}/bin/utils.sh
#fi

source ${ARPROJ_LIB}/bin/utils.sh


# set the account for SBUs
# dkhirlam for DMI
# c3srra for CARRA TU 
# c3srrp for CARRA2
#export SBU_CARRA_MEANS="dkhirlam"
#export SBU_CARRA_MEANS="c3srra"
export SBU_CARRA_MEANS="c3srrp"


# Number of levels for each type. Not including SFC for obvious reasons
export N_ML=65 #same in CARRA-TU
export N_PL=23 #same in CARRA-TU
export N_HL=18 #11 in CARRA-TU
export N_SOL=2 #NOT IN mars yet

#for daily_mean_an_insta_hl.sh
#export CARRA_PAR_AN_HL="10/54/130/157/246/247/3031" #CARRA-TU
export CARRA_PAR_AN_HL="10/54/130/157/246/247/3031"

#for daily_mean_an_insta_pl.sh
#export CARRA_PAR_AN_PL="60/75/76/129/130/131/132/157/246/247/3014/260028/260238/260257" #CARRA-TU
export CARRA_PAR_AN_PL="10/60/75/76/129/130/157/246/247/3014/3031/260028/260238/260257"

#for daily_mean_an_insta_sol.sh
export CARRA_PAR_AN_SOL="260199/260644" #NOT archived yet...

#for daily_mean_an_insta_sfc.sh
# export CARRA_PAR_AN_SFC="33/34/134/151/165/166/167/207/235/3020/3073/3074/3075/228141/228164/260057/260107/260108/260242/260260/260509/260289" #CARRA-TU


export CARRA_PAR_AN_SFC="31/34/78/79/134/151/165/166/167/173/207/235/3066/3073/3074/3075/174096/174098/228002/228141/228164/231063/260001/260038/260057/260107/260108/260242/260260/260649/260650"

export CARRA_PAR_AN_SFC="173/260649" #ONLY ON 20250307

#Modified this: removed 174098 that is of levelType 160??
#Also removed: 172, that in CARRA2 is land sea mask
#export CARRA_PAR_AN_SFC="31/34/78/79/134/151/165/166/167/172/173/207/235/3066/3073/3074/3075/174096/228002/228141/228164/231063/260001/260038/260057/260107/260108/260242/260260/260649/260650"

#for daily_mean_an_insta_ml.sh
#export CARRA_PAR_AN_ML="75/76/130/131/132/133/246/247/260028/260155/260257" #CARRA-TU
export CARRA_PAR_AN_ML="10/75/76/130/133/246/247/3031/260028/260155/260257" #CARRA2

#for daily_mean_fc_insta_sfc.sh
# Note that these are done differently than the rest.
# This list affects the staging part (for mars) and the counting of variables only (in count_all.sh). 
# The variables are extracted separately in the script. This is because most of them are min or max
# and the script calculates min or max of the variable over the whole day directly on mars.
# Only the variable 260015 (ptype) is excluded from the calculation
#export CARRA_PAR_FC_SFC="49/201/202/260646/260647/260015" #CARRA-TU
export CARRA_PAR_FC_SFC="228029/201/202/260015" #CARRA2
#export CARRA_PAR_FC_SFC_MM="49/201/202/260646/260647" #CARRA-TU
export CARRA_PAR_FC_SFC_MM="228029/201/202" #CARRA2

#export CARRA_PAR_FC_SFC_IN="260015/78/79/260648" #CARRA-TU
export CARRA_PAR_FC_SFC_IN="260648" #CARRA2

#for daily_sum_fc_accum_sfc.sh
#export CARRA_PAR_FC_ACC="228228/235015/260645/174008/260430/260259/235072/146/147/235019/235071/47/260264/176/169/210/177/175/211/178/179/235017/235018" # CARRA-TU
#Note: the CARRA2 list was ammended after a doing a list of parameters above. The following are not available: 147, 174008
#export CARRA_PAR_FC_ACC="47/146/169/175/176/177/178/179/210/211/228228/235015/235017/235018/235019/235071/235072/260259/260264/260430/260645" # CARRA2 TEMPORARY on 20250304
export CARRA_PAR_FC_ACC="47/146/169/175/176/177/178/179/210/211/228228/235015/235017/235018/235019/235071/235072/260259/260264/260430/260645/231010" # CARRA2
#export CARRA_PAR_FC_ACC="231010" # CARRA2

# For archival to mars only
# the list of certain groups are changed before archiving to marser
# or marsscratch. For example 
# Surface roughness (climatological)  173 becomes 235244
# Sea ice surface temperature  260649 becomes 263006

export CARRA_PAR_AN_SFC_ARXIV="31/34/78/79/134/151/165/166/167/235244/207/235/3066/3073/3074/3075/174096/174098/228002/228141/228164/231063/260001/260038/260057/260107/260108/260242/260260/263006/260650"

export CARRA_PAR_FC_SFC_MM_ARXIV="201/202" #CARRA2. only archiving these two since gust has problem with the headers

# Set the first path of destination directory
export MEANS_OUTPUT=/ec/res4/scratch/nhd/mars-pull/carra2
export MEANS_OUTPUT_FAC2=$SCRATCH/mars-pull/carra2


#the binary for grib_mean
export gmean=/perm/nhd/CARRA2/harp-data-pipeline/bin/grib_mean.x
