#!/bin/bash
#
################################################################
# CHOOSE OPTIONS
################################################################

# Local experiment to process
export EXP_TO_PROCESS=ca587
#
# If inline argument is passed
if [ ! -z $1 ]; then
  export EXP_TO_PROCESS=$1
fi

# Variables input options
#
export TABLE_TO_PROCESS=Amon            # CMIP6 table to process. Default is 'all'
export VARIABLE_TO_PROCESS=all          # CMIP6 variable to process. Default is 'all'
export SUBDAILY=true                    # subdaily selection options - select one of: [true, false, only]
export PRIORITY_ONLY=false              # sub-set list of variables to process, as defined in setup_env_cmip6.sh
export FORCE_DREQ=false                 # use input_files/default_mode_cmor-tables/Tables/CMIP6_CV.json

# Additional NCI information:
#
export PROJ=p66                         # NCI project to charge compute and use in storage flags
export ADDPROJS=( p73 )                 # additional NCI projects to be included in the storage flags
export QUEUE=hugemem                    # NCI queue to use
export MEM_PER_CPU=24                   # memory (GB) per CPU (recommended: 24 for daily/monthly; 48 for subdaily) 

# Select mode [cmip6, ccmi]
#
export MODE=cmip6

#
#

################################################################
# SETTING UP ENVIROMENT, VARIABLE MAPS, AND DATABASE
################################################################

# Set up environment
source ./subroutines/setup_env_cmip6.sh

# Cleanup output_files
./subroutines/cleanup.sh $OUT_DIR

# Update 'version' in experiment json file to today or chosen date
export datevers=$(date '+%Y%m%d')
#export datevers=20210802
./input_files/json/update_json.sh

# Create variable maps
python ./subroutines/dreq_mapping.py --multi
#exit 

# Create database
python ./subroutines/database_manager.py
#exit

# FOR TESTING
#python ./subroutines/app_wrapper.py; exit
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
#PBS -N app_${EXP_TO_PROCESS}
module purge
# pre
export EXP_TO_PROCESS=${EXP_TO_PROCESS}
export MODE=${MODE}
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
qsub ${APP_JOB}
echo ""
