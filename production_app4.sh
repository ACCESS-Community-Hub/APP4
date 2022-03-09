#!/bin/bash
set -a
################################################################
#
# This is the ACCESS Post-Processor, v4.1
# 24/01/2022
# 
# Developed by Chloe Mackallah, CSIRO Aspendale
# based on prior work by Peter Uhe and others at CSIRO
#
################################################################
#
# PRODUCTION MODE - USE THIS FOR PRODUCTION EXPERIMENTS (CMIP6, CCMI2022)
#
# see https://git.nci.org.au/cm2704/ACCESS-Archiver for related tools
#
################################################################
#
# USER OPTIONS

# Local experiment to process
EXP_TO_PROCESS=ca587
# If inline argument is passed from multiwrap_app4.sh
if [ ! -z $1 ]; then
  export EXP_TO_PROCESS=$1
fi

# Variables input options
#
TABLE_TO_PROCESS=Amon             # CMIP6 table to process. Default is 'all'
VARIABLE_TO_PROCESS=mc            # CMIP6 variable to process. Default is 'all'
SUBDAILY=true                    # subdaily selection options - select one of: [true, false, only]
FORCE_DREQ=true                  # use input_files/dreq/cmvme_all_piControl_3_3.csv
VAR_SUBSET=false                  # sub-set list of variables to process, as defined by 'VAR_SUBSET_LIST'
VAR_SUBSET_LIST=input_files/var_subset_lists/var_subset_ACS.csv

# Additional NCI information:
#
PROJ=p66                         # NCI project to charge compute and use in storage flags
ADDPROJS=( p73 )                 # additional NCI projects to be included in the storage flags
QUEUE=hugemem                    # NCI queue to use
MEM_PER_CPU=24                   # memory (GB) per CPU (recommended: 24 for daily/monthly; 48 for subdaily) 

# Select mode [cmip6, ccmi]
#
MODE=cmip6

#
#

################################################################
# SETTING UP ENVIROMENT, VARIABLE MAPS, AND DATABASE
################################################################

# Set up environment
source ./subroutines/setup_env.sh

# Cleanup output_files
./subroutines/cleanup.sh $OUT_DIR

# Update 'version' in experiment json file to today or chosen date
datevers=$(date '+%Y%m%d')
#datevers=20210802
./input_files/json/update_json.sh

# Create variable maps
python ./subroutines/dreq_mapping.py --multi
#exit 

# Create database
python ./subroutines/database_manager.py
#exit

# FOR TESTING
python ./subroutines/app_wrapper.py; exit
#

################################################################
# CREATE JOB
################################################################
echo -e '\ncreating job...'

for addproj in ${ADDPROJS[@]}; do
  addstore="${addstore}+scratch/${addproj}+gdata/${addproj}"
done
#
QUEUE=hugemem
#
NUM_ROWS=$( cat $OUT_DIR/database_count.txt )
if (($NUM_ROWS <= 24)); then
  NUM_CPUS=$NUM_ROWS
else
  NUM_CPUS=24
fi
NUM_MEM=$(echo "${NUM_CPUS} * ${MEM_PER_CPU}" | bc)
if ((${NUM_MEM} >= 1470)); then
  NUM_MEM=1470
fi
#
#NUM_CPUS=4
#NUM_MEM=192
echo "number of files to create: ${NUM_ROWS}"
echo "number of cpus to to be used: ${NUM_CPUS}"
echo "total amount of memory to be used: ${NUM_MEM}Gb"

cat << EOF > $APP_JOB
#!/bin/bash
#PBS -P ${PROJ}
#PBS -q ${QUEUE}
#PBS -l storage=scratch/${PROJ}+gdata/${PROJ}+gdata/hh5+gdata/access${addstore}
#PBS -l ncpus=${NUM_CPUS},walltime=48:00:00,mem=${NUM_MEM}Gb,wd
#PBS -j oe
#PBS -o ${JOB_OUTPUT}
#PBS -e ${JOB_OUTPUT}
#PBS -N prod_app4_${EXP_TO_PROCESS}
module purge
set -a
# pre
EXP_TO_PROCESS=${EXP_TO_PROCESS}
MODE=${MODE}
CDAT_ANONYMOUS_LOG=no
source ./subroutines/setup_env.sh
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
qsub ${APP_JOB}
echo ""
