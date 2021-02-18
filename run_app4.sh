#!/bin/bash
#
################################################################
# CHOOSE OPTIONS
################################################################
#
# Local experiment to process
export EXP_TO_PROCESS=PI-01
#
# CMIP6 table/variable to process. Default is 'all'.
export TABLE_TO_PROCESS=Omon
export VARIABLE_TO_PROCESS=all
#
# subdaily selection options
export SUBDAILY=false    #[true,false,only]
#
# Variable input options
export FORCE_DREQ=false    #use piControl dreq
export PRIORITY_ONLY=true
#
# If inline argument is passed
if [ ! -z $1 ]; then
  export EXP_TO_PROCESS=$1
fi
#
# for Default mode (i.e. non-CMIP6)
export DEFAULT_MODE=false
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
NUM_MEM=$(echo "${NUM_CPUS} * 32" | bc)
if ((${NUM_MEM} >= 1470)); then
  NUM_MEM=1470
fi
#
#NUM_CPUS=48
#NUM_MEM=1470
echo "number of files to create: ${NUM_ROWS}"
echo "number of cpus to to be used: ${NUM_CPUS}"
echo "total amount of memory to be used: ${NUM_MEM}Gb"

cat << EOF > $APP_JOB
#!/bin/bash
#PBS -P p66
#PBS -q hugemem
#PBS -l storage=scratch/p66+gdata/p66+gdata/hh5+gdata/access
#PBS -l ncpus=${NUM_CPUS},walltime=12:00:00,mem=${NUM_MEM}Gb,wd
#PBS -j oe
#PBS -o ${JOB_OUTPUT}
#PBS -e ${JOB_OUTPUT}
#PBS -N app_${EXP_TO_PROCESS}
module purge
# pre
export EXP_TO_PROCESS=${EXP_TO_PROCESS}
export DEFAULT_MODE=${DEFAULT_MODE}
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
