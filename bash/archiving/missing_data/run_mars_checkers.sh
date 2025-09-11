#!/usr/bin/env bash
# Wrapper to run scripts that check mars availability

# to check a given period use this order
YEARS=1986-1988
MONTHS=1-12
OUT=availability_$YEARS.txt
OUT_MARS=mars_calls_out.txt
OUT_ANALYSIS=${OUT_MARS}_batch_analysis.txt
MARS_SCR=$(basename $OUT_ANALYSIS .txt)_retrieval_script.sh

#./mars_checker.sh $YEARS $MONTHS $OUT
#./mars_analyser.sh $OUT

#mars availability_${YEARS}_mars_commands.txt >& $OUT_MARS
#./mars_batch_analyzer.sh $OUT_MARS
./mars_retrieval_generator.sh  $OUT_ANALYSIS


cat > temp_header.txt << 'EOF'
#!/usr/bin/env bash
#SBATCH --error=log_pull.%j.err
#SBATCH --output=log_pull.%j.out
#SBATCH --job-name=missing_data
#SBATCH --qos=nf
#SBATCH --time=8:00:00
#SBATCH --account="c3srrp"
EOF

cat temp_header.txt  $MARS_SCR > temp_file.sh && mv temp_file.sh $MARS_SCR
rm temp_header.txt



