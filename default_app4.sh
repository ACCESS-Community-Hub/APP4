#!/bin/bash
# 
################################################################
# USE THIS FOR NON-CMIP6 EXPERIMENTS - "DEFAULT MODE"
# THE APP4 WILL MAKE DEFAULT DECISIONS - NOT FOR PUBLICATION
# 
# THIS SCRIPT WILL SAVE ALL CMORISED DATA AS PICONTROL
# SEE: $OUTPUT_LOC/APP_output/CMIP6/CMIP/CSIRO[-ARCCSS]/ACCESS-[CM2,ESM]/piControl/r1i1p1f1/
################################################################
#
# Details of experiment to process
#local name of experiment
export EXP_TO_PROCESS=ca200     # local name of experiment
export VERSION=CM2              # select one of: [CM2, ESM, OM2(TBC)]
export START_YEAR=1850          # internal year to begin CMORisation
export END_YEAR=1851            # internal year to end CMORisation (inclusive)
export PROJECT=p66
#
# Directory information
#input directory containing model output: atm/ocn/ice/
export HISTORY_DATA=/g/data/$PROJECT/$USER/archive/$EXP_TO_PROCESS/history
#output directory for all generated data (CMORISED files & logs)
export OUTPUT_LOC=/scratch/$PROJECT/$USER/CMIP6
#
# CMIP6 table/variable to process. Default is 'all'.
export TABLE_TO_PROCESS=all
export VARIABLE_TO_PROCESS=all
#
# subdaily selection options
export SUBDAILY=true            # select one of: [true, false, only]
#
# Variable input options
export PRIORITY_ONLY=false      #priority list is set in ./subroutines/setup_env_cmip6.sh
#
################################################################
# SETTING UP ENVIROMENT, VARIABLE MAPS, AND DATABASE
################################################################

# Set up environment
export DEFAULT_MODE=true
export RESTRICT_TO_INCOMPLETE=false
export FORCE_DREQ=true
source ./subroutines/setup_env_cmip6.sh

# Cleanup output_files
./subroutines/cleanup.sh $OUT_DIR

# Update 'version' in experiment json file to today or chosen date
export datevers=$(date '+%Y%m%d')
#export datevers=20201207
./input_files/json/update_json.sh

# Create variable maps
python ./subroutines/dreq_mapping.py --multi

# Create database
python ./subroutines/database_manager.py

# FOR TESTING
#python ./subroutines/app_wrapper.py
#exit
#

################################################################
# CREATE JOB
################################################################
echo -e '\ncreating job...'

NUM_ROWS=$( cat $OUT_DIR/database_count.txt )
if (($NUM_ROWS <= 48)); then
  NUM_CPUS=$NUM_ROWS
else
  NUM_CPUS=48
fi
if $SUBDAILY; then
  NUM_MEM=$(echo "${NUM_CPUS} * 24" | bc)
else
  NUM_MEM=$(echo "${NUM_CPUS} * 8" | bc)
fi
if ((${NUM_MEM} >= 1470)); then
  NUM_MEM=1470
fi
#
#NUM_CPUS=48
#NUM_MEM=1470
echo number of cpus to to be used: ${NUM_CPUS}
echo total amount of memory to be used: ${NUM_MEM}Gb

cat << EOF > $APP_JOB
#!/bin/bash
#PBS -P $PROJECT
#PBS -q hugemem
#PBS -l storage=scratch/$PROJECT+gdata/$PROJECT+gdata/hh5+gdata/access
#PBS -l ncpus=${NUM_CPUS},walltime=12:00:00,mem=${NUM_MEM}Gb,wd
#PBS -j oe
#PBS -o ${JOB_OUTPUT}
#PBS -e ${JOB_OUTPUT}
#PBS -N app_${EXP_TO_PROCESS}
module purge
# pre
export EXP_TO_PROCESS=${EXP_TO_PROCESS}
export DEFAULT_MODE=true
export OUTPUT_LOC=$OUTPUT_LOC
source ./subroutines/setup_env_cmip6.sh ${CMIP6_ENV}
# main
python ./subroutines/app_wrapper.py
# post
python ${OUT_DIR}/database_updater.py
sort ${SUCCESS_LISTS}/${EXP_TO_PROCESS}_success.csv \
    > ${SUCCESS_LISTS}/${EXP_TO_PROCESS}_success_sorted.csv
mv ${SUCCESS_LISTS}/${EXP_TO_PROCESS}_success_sorted.csv \
    ${SUCCESS_LISTS}/${EXP_TO_PROCESS}_success.csv
sort ${SUCCESS_LISTS}/${EXP_TO_PROCESS}_failed.csv \
    > ${SUCCESS_LISTS}/${EXP_TO_PROCESS}_failed_sorted.csv 2>/dev/null
mv ${SUCCESS_LISTS}/${EXP_TO_PROCESS}_failed_sorted.csv \
    ${SUCCESS_LISTS}/${EXP_TO_PROCESS}_failed.csv
echo "APP completed for exp ${EXP_TO_PROCESS}."
EOF

/bin/chmod 775 ${APP_JOB}
echo "app job script: ${APP_JOB}"
#qsub ${APP_JOB}
