#!/bin/bash
# Set up  environment for running scripts

echo -e "setting up environment..."

module purge
module use /g/data/hh5/public/modules
module use ~access/modules
module load pbs
module load parallel

# Claire's CMOR env
#module load conda/analysis27-18.10
#export PYTHONPATH=${PYTHONPATH}:/g/data1/p66/ars599/python2.7/site-packages
#export LD_LIBRARY_PATH=/short/p66/ct5255/conda/envs/CMOR/lib:${LD_LIBRARY_PATH}

# CMIP6-pub env
module load conda
export PATH=${PATH}:/g/data3/hh5/public/apps/miniconda3/envs/cmip6-publication/bin:/g/data3/hh5/public/apps/miniconda3/bin
source activate cmip6-publication

module list
python -V

# Environment variables

# Inputs:
export APP_DIR=$(pwd)
#export APP_DIR=/g/data/p66/$USER/post_processing/APP4-0
# Input subdirectories
export ANCILLARY_FILES=/g/data/p66/CMIP6/APP_ancils
export CMIP6_TABLES=${APP_DIR}/input_files/cmip6-cmor-tables/Tables
# Input files
export EXPERIMENTS_TABLE=${APP_DIR}/input_files/experiments.csv
export MASTER_MAP=${APP_DIR}/input_files/master_map.csv
export PRIORITY_LIST=${APP_DIR}/input_files/priority_lists/priority_vars_cm2_3hr-tas-tos.csv

# Outputs:
export MAIN_DIR=/g/data/p66/CMIP6
# Default mode
if $DEFAULT_MODE; then
  export MAIN_DIR=$OUTPUT_LOC
fi
export OUT_DIR=${MAIN_DIR}/APP_job_files/${EXP_TO_PROCESS}
# Output subdirectories
export VARIABLE_MAPS=${OUT_DIR}/variable_maps
export SUCCESS_LISTS=${OUT_DIR}/success_lists
export CMOR_LOGS=${OUT_DIR}/cmor_logs
export VAR_LOGS=${OUT_DIR}/variable_logs
# Output files
export APP_JOB=${OUT_DIR}/app_job.sh
export JOB_OUTPUT=${OUT_DIR}/job_output.OU
export DATABASE=${OUT_DIR}/database.db

# Extra options
export OVERRIDEFILES=True
export PLOT=False
export DREQ_YEARS=False

