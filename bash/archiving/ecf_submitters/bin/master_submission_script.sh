#!/usr/bin/env bash
#testing all at once
ORIGIN=no-ar-pa

submit_daily_means()
{
sbatch daily_mean_an_insta_hl.sh $PERIOD $ORIGIN
sbatch daily_mean_an_insta_ml.sh $PERIOD $ORIGIN
sbatch daily_mean_an_insta_pl.sh $PERIOD $ORIGIN
sbatch daily_mean_an_insta_sfc.sh $PERIOD $ORIGIN
sbatch daily_minmax_fc_sfc.sh $PERIOD $ORIGIN
}

#split sums in parameters. Otherwise takes too long
submit_daily_sums()
{
source env.sh
params=$CARRA_PAR_FC_ACC
# Set the Internal Field Separator to "/"
IFS='/'
# Read the string into an array
read -ra PARAMS <<< "$params"
# Reset IFS to default (whitespace)
unset IFS

for PAR in ${PARAMS[@]}; do
sbatch daily_sum_fc_accum_sfc.sh $PERIOD $ORIGIN $PAR
done
}

submit_daily()
{
submit_daily_means
submit_daily_sums
}

submit_monthly()
{
#MONTH
sbatch monthly_means_an_insta.sh $PERIOD $ORIGIN
sbatch monthly_means_of_daily_sums.sh $PERIOD $ORIGIN
sbatch monthly_minmax.sh $PERIOD $ORIGIN
}

PERIOD=199510
submit_daily
