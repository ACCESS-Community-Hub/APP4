#!/bin/bash

################################################################
# CHOOSE OPTIONS
################################################################

export EXP_TO_PROCESS=bi889

export PRINTERRORS=False
export PUBLISH=True
export PUB_DIR=/g/data/p66/CMIP6/APP_publishable/CMIP6
export QC_DIR=/g/data/p66/CMIP6/APP_QC/CMIP6
export ONLINE_PLOT_DIR=/g/data/p66/accessdev-web/$USER/CMIP6_QC

# If inline argument is passed
if [ ! -z $1 ]; then
  export EXP_TO_PROCESS=$1
fi

################################################################

# Set up environment
source ./setup_env_cmip6.sh publication

if [ -d ${JOB_SCRIPTS}/qc ]; then
  rm -r ${JOB_SCRIPTS}/qc
  mkdir -p ${JOB_SCRIPTS}/qc
else
  mkdir -p ${JOB_SCRIPTS}/qc
fi

JOB_LIST_QC=${OUT_DIR}/qc_job_list_${EXP_TO_PROCESS}.txt
if [ -f ${JOB_LIST_QC} ]; then
  rm ${JOB_LIST_QC}
fi

echo "publishing = $PUBLISH"

################################################################
# CREATING JOB SCRIPTS
################################################################

mkdir -p ${JOB_SCRIPTS}/qc

IFS=,
[ ! -f ${MULTI_LIST} ] && { echo "${MULTI_LIST} file not found"; exit 99; }
while read table
do

cat << EOF > ${JOB_SCRIPTS}/qc/qc_${table%%.*}.job.sh
#!/bin/bash
export EXP_TO_PROCESS=${EXP_TO_PROCESS}
export TABLE_TO_PROCESS=${table%%.*}
export PRINTERRORS=${PRINTERRORS}
export PUBLISH=${PUBLISH}
export PUB_DIR=${PUB_DIR}
export QC_DIR=${QC_DIR}
export ONLINE_PLOT_DIR=${ONLINE_PLOT_DIR}
export USER=$USER
# Set up environment
source ./setup_env_cmip6.sh publication
# Run quality check
echo quality checking exp ${EXP_TO_PROCESS}, table ${table%%.*}...
python ./quality_check_multi.py --compliance --data --timeseries
echo completed quality checking exp ${EXP_TO_PROCESS}, table ${table%%.*}
EOF

/bin/chmod 755 ${JOB_SCRIPTS}/qc/qc_${table%%.*}.job.sh
ls ${JOB_SCRIPTS}/qc/qc_${table%%.*}.job.sh
echo ${JOB_SCRIPTS}/qc/qc_${table%%.*}.job.sh >> ${JOB_LIST_QC}

done < <(ls ${VARIABLE_MAPS})

################################################################
# START OF PARALLELISATION
################################################################
echo -e '\nstarting parallelisation...'
NUM_INPUTS=$(wc -l < ${JOB_LIST_QC})
NUM_CPUS=$(echo "${NUM_INPUTS} / 4 + 1" | bc)
if ((${NUM_CPUS} >= 48)); then
  NUM_CPUS=48
fi
#NUM_CPUS=1
#NUM_MEM=$(echo "${NUM_CPUS} * 1" | bc) # for tests
NUM_MEM=$(echo "${NUM_CPUS} * 16" | bc) # for real runs
if ((${NUM_MEM} >= 192)); then
  NUM_MEM=192
fi
echo number of tables to process: ${NUM_INPUTS}
echo number of cpus to to be used: ${NUM_CPUS}
echo total amount of memory to be used: ${NUM_MEM}Gb

cat << EOF > ${OUT_DIR}/qc_parallel_${EXP_TO_PROCESS}.job.sh
#!/bin/bash
#PBS -P p66
#PBS -l walltime=48:00:00,ncpus=${NUM_CPUS},mem=${NUM_MEM}Gb,wd
#PBS -q normal
#PBS -l storage=scratch/p66+gdata/p66+gdata/hh5+scratch/access
#PBS -j oe
#PBS -o ${OUT_DIR}/qc_results_${EXP_TO_PROCESS}.ou
#PBS -e ${OUT_DIR}/qc_results_${EXP_TO_PROCESS}.ou
#PBS -N qc_${EXP_TO_PROCESS}
export EXP_TO_PROCESS=${EXP_TO_PROCESS}
export PRINTERRORS=${PRINTERRORS}
export PUBLISH=${PUBLISH}
export PUB_DIR=${PUB_DIR}
export QC_DIR=${QC_DIR}
export ONLINE_PLOT_DIR=${ONLINE_PLOT_DIR}
export USER=$USER
JOB_LIST_QC=${JOB_LIST_QC}
# Set up environment
source ./setup_env_cmip6.sh publication
# Run quality check
echo completion checking exp ${EXP_TO_PROCESS}...
python ./completion_check.py --multi
echo ''
parallel -j \${PBS_NCPUS} pbsdsh -v -n {%} -- bash -l -c "'{}'" :::: \${JOB_LIST_QC}
python qcfigs_index.py
#chmod -Rf 755 ${MAIN_DIR}/*
chmod -R 755 ${OUT_DIR}
chmod -Rf 755 ${ONLINE_PLOT_DIR}/${EXP_TO_PROCESS}
echo completed quality checking exp ${EXP_TO_PROCESS}
EOF

################################################################

/bin/chmod 775 ${OUT_DIR}/qc_parallel_${EXP_TO_PROCESS}.job.sh
ls ${OUT_DIR}/qc_parallel_${EXP_TO_PROCESS}.job.sh
qsub ${OUT_DIR}/qc_parallel_${EXP_TO_PROCESS}.job.sh
