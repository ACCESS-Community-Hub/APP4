#!/bin/bash
set -a
#
################################################################
#
# CHECK APP4 SCRIPT FOR EASY JOB SUMMARIES
# 
# EXP/OUTPUTS CAN BE DEFINED; 
# OR CAN BE READ DIRECTLY FROM CUSTOM_APP4.SH (SET: READ_FROM_CUSTOM_WRAPPER=true)
#
################################################################
#
# USER OPTIONS

EXP_TO_PROCESS=PI-C-rev-01
OUTPUT_LOC=/scratch/$PROJECT/$USER/APP4_output
# OR
READ_FROM_CUSTOM_WRAPPER=true

#MODE=custom
#PRINTERRORS=false
#PUBLISH=false
#PUB_DIR=/scratch/CMIP6/APP_publishable/CMIP6
#QC_DIR=/g/data/p66/CMIP6/APP_QC/CMIP6
#ONLINE_PLOT_DIR=/g/data/p66/accessdev-web/$USER/CMIP6_QC

################################################################
#
USER=$USER
if [ ! -z $1 ]; then
  EXP_TO_PROCESS=$1
  MODE=$2
else
  if $READ_FROM_CUSTOM_WRAPPER; then
    check_app4=true
    source ./custom_app4.sh
  fi
  MODE=custom
fi

#
# Set up environment
source ./setup_env.sh
# Run completion check
echo "custom-mode APP4 checking tool"
echo "completion checking exp ${EXP_TO_PROCESS}..."
python ./subroutines/completion_check.py --multi
echo ""
grep 'file name does not match expected' $JOB_OUTPUT
echo -e "\ncompleted quality checking exp ${EXP_TO_PROCESS}"
