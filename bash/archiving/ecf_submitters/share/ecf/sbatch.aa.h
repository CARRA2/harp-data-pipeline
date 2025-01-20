#!/bin/bash
#SBATCH --job-name=%TASK%
#SBATCH --qos=%QUEUE%
#SBATCH --output=%ECF_JOBOUT%
#SBATCH --error=%ECF_JOBOUT%
#SBATCH --mem-per-cpu=16000
#SBATCH --time=12:00:00
#SBATCH --account=%SBU_CARRA_MEANS%
source /etc/profile
