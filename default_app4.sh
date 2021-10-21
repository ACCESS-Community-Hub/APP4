#!/bin/bash
# 
################################################################
# USE THIS FOR NON-CMIP6 EXPERIMENTS - "DEFAULT MODE"
# THE APP4 WILL MAKE DEFAULT DECISIONS - NOT FOR CMIP6 PUBLICATION
# 
################################################################

# Details of local experiment to process:
# HISTORY_DATA is the input directory containing model output: atm/,ocn/,ice/.
# see https://git.nci.org.au/cm2704/ACCESS-Archiver for related tools
export EXP_TO_PROCESS=bj594             # local name of experiment
export VERSION=ESM                      # select one of: [CM2, ESM, OM2(TBC)]
export START_YEAR=1850                  # internal year to begin CMORisation
export END_YEAR=1851                    # internal year to end CMORisation (inclusive)
export PROJECT=p66                      # NCI project to charge compute
export CONTACT=access_csiro@csiro.au    # please insert your contact email
export HISTORY_DATA=/g/data/p73/archive/CMIP6/ACCESS-ESM1-5/HI-05/history

# Standard experiment details:
export experiment_id=piControl          # standard experiment name 
export activity_id=CMIP                 # activity name
export realization_index=1              # "r1"[i1p1f1]
export initialization_index=1           # [r1]"i1"[p1f1]
export physics_index=1                  # [r1i1]"p1"[f1]
export forcing_index=1                  # [r1i1p1]"f1"
export source_type=AOGCM                # see input_files/default_mode_cmor-tables/Tables/CMIP6_CV.json
export branch_time_in_child=0D0         # specifies the difference between the time units base and the first internal year

# Parent experiment details:
# if parent=false, all variables are set to "no parent"
export parent=true
export parent_experiment_id=piControl-spinup
export parent_activity_id=CMIP
export parent_mip_era=CMIP6
export parent_time_units="days since 0001-01-01"
export branch_time_in_parent=0D0
export parent_variant_label=r1i1p1f1

# Additional NCI information:
# output directory for all generated data (CMORISED files & logs)
export OUTPUT_LOC=/scratch/$PROJECT/$USER/APP4_output
# additional NCI projects to be included in the storage flags
export ADDPROJS=( p73 )

# Variables to CMORise:
# CMIP6 table/variable to process. Default is 'all'.
export TABLE_TO_PROCESS=Amon
export VARIABLE_TO_PROCESS=tas
# subdaily selection options
export SUBDAILY=false            # select one of: [true, false, only]
# Variable input options for specific list of required variables
export PRIORITY_ONLY=false      # priority list of variables is defined in setup_env_cmip6.sh



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
export FORCE_DREQ=true
source ./subroutines/setup_env_cmip6.sh

# Cleanup output_files
./subroutines/cleanup.sh $OUT_DIR

# Create json file which contains metadata info
./subroutines/create_json_default.sh

# Create variable maps
python ./subroutines/dreq_mapping.py --multi
#exit

# Create database
python ./subroutines/database_manager.py
#exit

# FOR TESTING
#python ./subroutines/app_wrapper.py
#exit
#

################################################################
# CREATE JOB
################################################################
echo -e '\ncreating job...'

for addproj in ${ADDPROJS[@]}; do
  addstore="${addstore}+scratch/${addproj}+gdata/${addproj}"
done
QUEUE=normal #hugemem
#
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
#PBS -q $QUEUE
#PBS -l storage=scratch/$PROJECT+gdata/$PROJECT+gdata/hh5+gdata/access${addstore}
#PBS -l ncpus=${NUM_CPUS},walltime=12:00:00,mem=${NUM_MEM}Gb,wd
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
