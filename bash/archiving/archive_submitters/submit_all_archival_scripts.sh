PERIOD=200609
ORIGIN=no-ar-pa

cd daily_mean_an
sbatch archive_daily_mean_an.sh $PERIOD $ORIGIN --job-name=and_${PERIOD}
cd ..

cd daily_minmax_fc
sbatch archive_daily_minmax_fc.sh $PERIOD $ORIGIN --job-name=dmm_${PERIOD}
cd ..

cd daily_sum_fc
sbatch archive_daily_sums_fc.sh $PERIOD $ORIGIN --job-name=sum_${PERIOD}
cd ..

cd monthly_mean_an
sbatch archive_monthly_mean_an.sh $PERIOD $ORIGIN --job-name=anm_${PERIOD}
cd ..

cd monthly_minmax_fc/
sbatch archive_monthly_minmax_fc.sh $PERIOD $ORIGIN --job-name=mmm_${PERIOD}
cd ..

cd monthly_daysum_fc/
sbatch archive_monthly_daysum_fc.sh $PERIOD $ORIGIN --job-name=dsm_${PERIOD}
cd ..

