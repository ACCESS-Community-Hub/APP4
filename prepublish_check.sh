#!/bin/bash

################################################################
# CHOOSE OPTIONS
################################################################

export EXP_TO_PROCESS=PI-01
export TABLE_TO_PROCESS=all

export PRINTERRORS=False
export PUBLISH=False
export PUB_DIR=/g/data/p66/CMIP6/APP_publishable/CMIP6
export QC_DIR=/g/data/p66/CMIP6/APP_QC/CMIP6
export ONLINE_PLOT_DIR=/g/data/p66/accessdev-web/$USER/CMIP6_QC

# If inline argument is passed
if [ ! -z $1 ]; then
  export EXP_TO_PROCESS=$1
fi

################################################################

# Set up environment
#source ./setup_env_cmip6.sh publication

################################################################
# CREATING JOB SCRIPT
################################################################

#cat << EOF > ${OUT_DIR}/qc_script_${EXP_TO_PROCESS}.job.sh
#!/bin/bash
#PBS -P p66
#PBS -l walltime=05:00:00,ncpus=1,mem=64Gb
#PBS -l wd
#PBS -l storage=scratch/p66+gdata/p66+gdata/hh5+scratch/access
#PBS -q normal
#PBS -j oe
#PBS -o ${OUT_DIR}/qc_results_${EXP_TO_PROCESS}.ou
#PBS -e ${OUT_DIR}/qc_results_${EXP_TO_PROCESS}.ou
#PBS -N app_qc_${EXP_TO_PROCESS}

export EXP_TO_PROCESS=${EXP_TO_PROCESS}
export TABLE_TO_PROCESS=${TABLE_TO_PROCESS}
export PRINTERRORS=${PRINTERRORS}
export PUBLISH=${PUBLISH}
export PUB_DIR=${PUB_DIR}
export QC_DIR=${QC_DIR}
export ONLINE_PLOT_DIR=${ONLINE_PLOT_DIR}
export USER=$USER
# Set up environment
source ./setup_env_cmip6.sh publication
# Run quality check
echo quality checking exp ${EXP_TO_PROCESS}...
python ./completion_check.py --multi
grep 'file name does not match expected' $JOB_OUTPUT
#python ./quality_check_multi.py --compliance --data --timeseries
chmod -R 755 ${OUT_DIR}
echo completed quality checking exp ${EXP_TO_PROCESS}
#EOF

################################################################

#/bin/chmod 775 ${OUT_DIR}/qc_script_${EXP_TO_PROCESS}.job.sh
#ls ${OUT_DIR}/qc_script_${EXP_TO_PROCESS}.job.sh
#qsub ${OUT_DIR}/qc_script_${EXP_TO_PROCESS}.job.sh
