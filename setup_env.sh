#!/bin/bash
set -a
# Set up  environment for running scripts

echo -e "\nsetting up environment..."

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
PATH=${PATH}:/g/data3/hh5/public/apps/miniconda3/envs/cmip6-publication/bin:/g/data3/hh5/public/apps/miniconda3/bin
source activate cmip6-publication

module list
python -V

# Environment variables

if [[ $MODE == custom ]]; then
  CONTACT=$CONTACT
else
  CONTACT=access_csiro@csiro.au
fi

# Inputs:
APP_DIR=$(pwd)
#export APP_DIR=/g/data/p66/$USER/post_processing/APP4-0
# Input subdirectories
ANCILLARY_FILES=/g/data/p66/CMIP6/APP_ancils
if [[ $MODE == ccmi ]]; then
  CMIP_TABLES=${APP_DIR}/input_files/ccmi-2022/Tables
elif [[ $MODE == custom ]]; then
  CMIP_TABLES=${APP_DIR}/input_files/custom_mode_cmor-tables/Tables
else
  CMIP_TABLES=${APP_DIR}/input_files/cmip6-cmor-tables/Tables
fi
# Input files
EXPERIMENTS_TABLE=${APP_DIR}/input_files/experiments.csv
MASTER_MAP=${APP_DIR}/input_files/master_map.csv

# Outputs:
# output logs and job scripts/files go to MAIN_DIR
MAIN_DIR=/g/data/p66/CMIP6
# write cmorised data to DATA_DIR
#export DATA_DIR=${MAIN_DIR}
DATA_DIR=/scratch/p73/CMIP6
# Default mode
if [[ $MODE == custom ]]; then
  MAIN_DIR=$OUTPUT_LOC
  DATA_DIR=$OUTPUT_LOC
fi
OUT_DIR=${MAIN_DIR}/APP_job_files/${EXP_TO_PROCESS}
# Output subdirectories
VARIABLE_MAPS=${OUT_DIR}/variable_maps
SUCCESS_LISTS=${OUT_DIR}/success_lists
CMOR_LOGS=${OUT_DIR}/cmor_logs
VAR_LOGS=${OUT_DIR}/variable_logs
# Output files
APP_JOB=${OUT_DIR}/app_job.sh
JOB_OUTPUT=${OUT_DIR}/job_output.OU
DATABASE=${OUT_DIR}/database.db

# Extra options
OVERRIDEFILES=True
PLOT=False
DREQ_YEARS=False

