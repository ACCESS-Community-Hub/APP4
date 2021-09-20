#!/bin/bash

################################################################
# CHOOSE OPTIONS
################################################################

export EXP_TO_PROCESS=lig127k
# If inline argument is passed
if [ ! -z $1 ]; then
  export EXP_TO_PROCESS=$1
fi
#
#
export PRINTERRORS=False
export PUBLISH=True
export PUB_DIR=/scratch/p66/CMIP6/APP_publishable/CMIP6
export QC_DIR=/g/data/p66/CMIP6/APP_QC/CMIP6
export ONLINE_PLOT_DIR=/g/data/p66/accessdev-web/$USER/CMIP6_QC
export MODE=cmip6
export PROJ=p66
export ADDPROJS=( p73 )

################################################################

# Set up environment
source ./subroutines/setup_env_cmip6.sh publication
echo "publishing = $PUBLISH"

################################################################
# CREATE JOB
################################################################
echo -e '\ncreating job...'

for addproj in ${ADDPROJS[@]}; do
  addstore="${addstore}+scratch/${addproj}+gdata/${addproj}"
done
NUM_ROWS=$( cat ${SUCCESS_LISTS}/${EXP_TO_PROCESS}_success.csv | wc -l )
if (($NUM_ROWS <= 48)); then
  NUM_CPUS=$NUM_ROWS
else
  NUM_CPUS=48
fi
#NUM_MEM=$(echo "${NUM_CPUS} * 32" | bc)
NUM_MEM=$(echo "${NUM_CPUS} * 2" | bc)
if ((${NUM_MEM} >= 1470)); then
  NUM_MEM=1470
fi
#
QUEUE=normal
echo "number of files to check: ${NUM_ROWS}"
echo "number of cpus to to be used: ${NUM_CPUS}"
echo "total amount of memory to be used: ${NUM_MEM}Gb"
echo "NCI queue to use: $QUEUE"

cat << EOF > ${OUT_DIR}/qc_job.sh
#!/bin/bash
#PBS -P ${PROJ}
#PBS -l walltime=12:00:00,ncpus=${NUM_CPUS},mem=${NUM_MEM}Gb,wd
#PBS -q ${QUEUE}
#PBS -l storage=scratch/${PROJ}+gdata/${PROJ}+gdata/hh5+gdata/access${addstore}
#PBS -j oe
#PBS -o ${OUT_DIR}/qc_results.ou
#PBS -N qc_${EXP_TO_PROCESS}
export EXP_TO_PROCESS=${EXP_TO_PROCESS}
export PUBLISH=${PUBLISH}
export PUB_DIR=${PUB_DIR}
export QC_DIR=${QC_DIR}
export ONLINE_PLOT_DIR=${ONLINE_PLOT_DIR}
export USER=$USER
export DEFAULT_MODE=$DEFAULT_MODE
export NCPUS=$NUM_CPUS
# Set up environment
source ./subroutines/setup_env_cmip6.sh publication
# Run checks
echo "completion checking exp ${EXP_TO_PROCESS}..."
python ./subroutines/completion_check.py --multi
echo "quality checking exp ${EXP_TO_PROCESS}..."
python ./subroutines/quality_check.py --compliance #--timeseries
exit
echo "setting up accessdev-web QC plots"
python ./subroutines/qcfigs_index.py
chmod -R 755 ${OUT_DIR}
chmod -Rf 755 ${ONLINE_PLOT_DIR}/${EXP_TO_PROCESS}
echo completed quality checking exp ${EXP_TO_PROCESS}
EOF

################################################################

/bin/chmod 775 ${OUT_DIR}/qc_job.sh
ls ${OUT_DIR}/qc_job.sh
qsub ${OUT_DIR}/qc_job.sh
