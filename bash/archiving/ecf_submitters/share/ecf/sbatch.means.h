#!/bin/bash
#SBATCH --job-name=%TASK%
#SBATCH --qos=%QUEUE%
#SBATCH --output=%ECF_JOBOUT%
#SBATCH --error=%ECF_JOBOUT%
#SBATCH --mem-per-cpu=16000
source /etc/profile
