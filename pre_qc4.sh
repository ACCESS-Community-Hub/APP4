#!/bin/bash

################################################################
# CHOOSE OPTIONS
################################################################

export EXP_TO_PROCESS=AM-04
# If inline argument is passed
if [ ! -z $1 ]; then
  export EXP_TO_PROCESS=$1
fi
#
#
export PRINTERRORS=False
export PUBLISH=False
export PUB_DIR=/scratch/CMIP6/APP_publishable/CMIP6
export QC_DIR=/g/data/p66/CMIP6/APP_QC/CMIP6
export ONLINE_PLOT_DIR=/g/data/p66/accessdev-web/$USER/CMIP6_QC
export DEFAULT_MODE=false

export USER=$USER
# Set up environment
source ./subroutines/setup_env_cmip6.sh
# Run completion check

echo ""
echo "pre-QC tool"
echo "completion checking exp ${EXP_TO_PROCESS}..."
python ./subroutines/completion_check.py --multi
grep 'file name does not match expected' $JOB_OUTPUT
echo "completed quality checking exp ${EXP_TO_PROCESS}"
