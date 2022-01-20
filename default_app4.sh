#!/bin/bash
# 
################################################################
# USE THIS FOR NON-CMIP6 EXPERIMENTS - "DEFAULT MODE"
#
# THE APP4 WILL INSERT THE DETAILS DEFINED BELOW INTO THE CMIP6_CV.JSON FILE
# TO ENABLE NON-CMIP6 EXPERIMENTS TO BE CMORISED
#
# see https://git.nci.org.au/cm2704/ACCESS-Archiver for related tools
################################################################

# Details of local experiment to process:
# HISTORY_DATA must point to dir containing atm/ ocn/ ice/
#
export HISTORY_DATA=/g/data/p73/archive/non-CMIP/ACCESS-ESM1-5/HI-noluc-C-05/history
export EXP_TO_PROCESS=HI-noluc-C-05                 # local name of experiment
export VERSION=ESM                          # select one of: [CM2, ESM, OM2[-025]]
export START_YEAR=1850                      # internal year to begin CMORisation
export END_YEAR=2014                        # internal year to end CMORisation (inclusive)
export CONTACT=access_csiro@csiro.au        # please insert your contact email

# Standard experiment details:
#
export experiment_id=hist-noluc-C                 # standard experiment name; e.g. piControl
export activity_id=CNP-MIP                  # activity name; e.g. CMIP
export realization_index=1                  # "r1"[i1p1f1]; e.g. 1
export initialization_index=1               # [r1]"i1"[p1f1]; e.g. 1
export physics_index=1                      # [r1i1]"p1"[f1]; e.g. 1
export forcing_index=1                      # [r1i1p1]"f1"; e.g. 1
export source_type=AOGCM                    # see input_files/default_mode_cmor-tables/Tables/CMIP6_CV.json
export branch_time_in_child=0D0             # specifies the difference between the time units base and the first internal year; e.g. 365D0

# Parent experiment details:
# if parent=false, all parent fields are automatically set to "no parent". If true, defined values are used.
#
export parent=true 
export parent_experiment_id=piControl               # e.g. piControl-spinup
export parent_activity_id=CMIP                      # e.g. CMIP
export parent_time_units="days since 0101-01-01"    # e.g. "days since 0001-01-01"
export branch_time_in_parent=21915D0                # e.g. 0D0
export parent_variant_label=r1i1p1f1                # e.g. r1i1p1f1

# Variables to CMORise:
# CMIP6 table/variable to process. Default is 'all'.
export DREQ=input_files/dreq/esm/cmvme_c4.cd.cm.rf.sc_historical_1_2.csv   # default=input_files/dreq/cmvme_all_piControl_3_3.csv
export TABLE_TO_PROCESS=all             # CMIP6 table to process. Default is 'all'
export VARIABLE_TO_PROCESS=all          # CMIP6 variable to process. Default is 'all'
export SUBDAILY=false                    # subdaily selection options - select one of: [true, false, only]
export PRIORITY_ONLY=false              # sub-set list of variables to process, as defined in setup_env_cmip6.sh

# Additional NCI information:
# OUTPUT_LOC defines directory for all generated data (CMORISED files & logs)
#
export OUTPUT_LOC=/scratch/$PROJECT/$USER/APP4_output 
export PROJECT=$PROJECT                      # NCI project to charge compute
export ADDPROJS=( p73 p66 )                 # additional NCI projects to be included in the storage flags
export QUEUE=hugemem                    # NCI queue to use
export MEM_PER_CPU=24                   # memory (GB) per CPU (recommended: 24 for daily/monthly; 48 for subdaily) 

#
#
#
#
#
#

################################################################
# SETTING UP ENVIROMENT, VARIABLE MAPS, AND DATABASE
################################################################

# Set up environment
export MODE=default
source ./subroutines/setup_env_cmip6.sh

# Cleanup output_files
./subroutines/cleanup.sh $OUT_DIR

# Create json file which contains metadata info
python ./subroutines/default_json_editor.py
#exit

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
#NUM_CPUS=48
#NUM_MEM=1470
echo "number of files to create: ${NUM_ROWS}"
echo "number of cpus to to be used: ${NUM_CPUS}"
echo "total amount of memory to be used: ${NUM_MEM}Gb"

cat << EOF > $APP_JOB
#!/bin/bash
#PBS -P $PROJECT
#PBS -q $QUEUE
#PBS -l storage=scratch/$PROJECT+gdata/$PROJECT+gdata/hh5+gdata/access${addstore}
#PBS -l ncpus=${NUM_CPUS},walltime=24:00:00,mem=${NUM_MEM}Gb,wd
#PBS -j oe
#PBS -o ${JOB_OUTPUT}
#PBS -e ${JOB_OUTPUT}
#PBS -N app_${EXP_TO_PROCESS}
module purge
# pre
export EXP_TO_PROCESS=${EXP_TO_PROCESS}
export OUTPUT_LOC=$OUTPUT_LOC
export MODE=default
export CONTACT=$CONTACT
export CDAT_ANONYMOUS_LOG=no
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
