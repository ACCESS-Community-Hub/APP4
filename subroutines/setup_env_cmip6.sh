#!/bin/bash
# Set up  environment for running scripts

echo -e "setting up environment..."

if [[ -z $1 ]]; then 
  echo "no environment specified"
  exit
fi

module purge
module use /g/data/hh5/public/modules
module use ~access/modules
module load pbs
module load parallel

if [ $1 = "claire" ]; then
  # Claire's CMOR env
  module load conda/analysis27-18.10
  export PYTHONPATH=${PYTHONPATH}:/g/data1/p66/ars599/python2.7/site-packages
  export LD_LIBRARY_PATH=/short/p66/ct5255/conda/envs/CMOR/lib:${LD_LIBRARY_PATH}
elif [ $1 = "publication" ]; then
  # CMIP6-pub env
  module load conda
  export PATH=${PATH}:/g/data3/hh5/public/apps/miniconda3/envs/cmip6-publication/bin:/g/data3/hh5/public/apps/miniconda3/bin
  source activate cmip6-publication
else
  echo unknown environment specified
  exit
fi

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
export PRIORITY_LIST=${APP_DIR}/input_files/priority_lists/priority_vars_esm_covid_pseudo-fix.csv
export COMPLETED_LIST=${APP_DIR}/input_files/completed_lists/completed_${EXP_TO_PROCESS}.csv

# Outputs:
export MAIN_DIR=/g/data/p66/CMIP6
export OUT_DIR=${MAIN_DIR}/APP_job_files/${EXP_TO_PROCESS}
# Output subdirectories
export VARIABLE_MAPS=${OUT_DIR}/variable_maps
export SUCCESS_LISTS=${OUT_DIR}/success_lists
#export JOB_SCRIPTS=${OUT_DIR}/job_scripts
export CMOR_LOGS=${OUT_DIR}/cmor_logs
export VAR_LOGS=${OUT_DIR}/variable_logs
# Output files
#export MULTI_LIST=${OUT_DIR}/multi_list.csv
#export JOB_LIST=${OUT_DIR}/job_list.txt
export APP_JOB=${OUT_DIR}/app_job.sh
export JOB_OUTPUT=${OUT_DIR}/job_output.OU
export DATABASE=${OUT_DIR}/database.db

# Extra options
export OVERRIDEFILES=True
export PLOT=False
export DREQ_YEARS=False
