#!/bin/bash
#
################################################################
# CHOOSE OPTIONS
################################################################
#
# Local experiment to process
#CM2
#export EXP_TO_PROCESS=bi889           #CM2-piControl
#export EXP_TO_PROCESS=bj400         #CM2-amip    bj400,bj402,bj567,bj229
#export EXP_TO_PROCESS=bj594          #CM2-historical   bj594,bl655,bm652
#export EXP_TO_PROCESS=bn570        #CM2-abrupt-4xCO2   (+bj595)
#export EXP_TO_PROCESS=bk243        #CM2-1pctCO2
#export EXP_TO_PROCESS=bs267        #CM2-ssp126     bl686,br563,bs267
#export EXP_TO_PROCESS=bs266        #CM2-ssp245     bk882,br080,bs266
#export EXP_TO_PROCESS=bs215        #CM2-ssp370     bm038,br612,bs215
#export EXP_TO_PROCESS=bs118        #CM2-ssp585     bk786,bq888,bs118
#export EXP_TO_PROCESS=bo830        #CM2-faf-heat (FAFMIP)
#export EXP_TO_PROCESS=bo831        #CM2-faf-water (FAFMIP)
#export EXP_TO_PROCESS=bo832        #CM2-faf-stress (FAFMIP)
#export EXP_TO_PROCESS=bs500_fafall    #CM2-faf-all (FAFMIP) (Re-run)
#export EXP_TO_PROCESS=bo133        #CM2-faf-passiveheat (FAFMIP)
#export EXP_TO_PROCESS=bs364        #CM2-faf-heat-NA0pct (FAFMIP)
#export EXP_TO_PROCESS=br657        #CM2-faf-heat-NA50pct (FAFMIP)
#export EXP_TO_PROCESS=br931            #CM2-piClim-control (RFMIP)
#export EXP_TO_PROCESS=bs626            #CM2-piClim-4xCO2 (RFMIP)
#export EXP_TO_PROCESS=bs627            #CM2-piClim-ghg (RFMIP)
#export EXP_TO_PROCESS=bs628            #CM2-piClim-aer (RFMIP)
#export EXP_TO_PROCESS=bs629            #CM2-piClim-anthro (RFMIP)
#export EXP_TO_PROCESS=bu010         #CM2-hist-GHG (DAMIP) bu010,bu839,bu840
#export EXP_TO_PROCESS=bw966         #CM2-hist-aer (DAMIP) bw966,bx128,bx129
#export EXP_TO_PROCESS=by350         #CM2-hist-nat (DAMIP) by350,by438,by563
#export EXP_TO_PROCESS=omip2_cm2         #CM2-omip2 (OMIP)
#export EXP_TO_PROCESS=025deg_jra55_iaf_cycle1         #OM2-025-omip2 (OMIP)
#ESM1.5
export EXP_TO_PROCESS=PI-01         #ESM-piControl PI-01,PI-02(ext)
#export EXP_TO_PROCESS=PI-slice-HI-05  #ESM-piControl-30yrslice
#export EXP_TO_PROCESS=PI-EDC-01      #ESM-esm-piControl
#export EXP_TO_PROCESS=PI-slice-HIE03  #ESM-esm-piControl-30yrslice
#export EXP_TO_PROCESS=PI-1pct-01       #ESM-1pctCO2
#export EXP_TO_PROCESS=PI-4xco2-02       #ESM-abrupt-4xCO2  PI-4xco2-01,02
#export EXP_TO_PROCESS=AM-01        #ESM-amip    AM-01,02,03
#export EXP_TO_PROCESS=HI-25        #ESM-historical   HI-05..34
#export EXP_TO_PROCESS=HI-EDC-03      #ESM-esmhist     HI-EDC-03,04,06..13
#export EXP_TO_PROCESS=SSP-126-06      #ESM-ssp126     SSP-126-05..14
#export EXP_TO_PROCESS=SSP-245-34      #ESM-ssp245     SSP-245-05..34
#export EXP_TO_PROCESS=SSP-370-12      #ESM-ssp370     SSP-370-05..14
#export EXP_TO_PROCESS=SSP-585-06      #ESM-ssp585     SSP-585-05..14
#export EXP_TO_PROCESS=SSP-EDC-585-06  #ESM-esm-ssp585    SSP-EDC-585-03,04,06
#export EXP_TO_PROCESS=PI-1pct-bgc-01   #ESM-1pctCO2-bgc (C4MIP)
#export EXP_TO_PROCESS=PI-1pct-rad-01   #ESM-1pctCO2-rad (C4MIP)
#export EXP_TO_PROCESS=HI-bgc-02      #ESM-hist-bgc (C4MIP)
#export EXP_TO_PROCESS=SSP-585-bgc-02      #ESM-ssp585-bgc (C4MIP)
#export EXP_TO_PROCESS=PI-1pct-rev-01  #ESM-1pctCO2-cdr (CDRMIP)
#export EXP_TO_PROCESS=PI-rev-01      #ESM-1pctCO2-cdr(piC-levels) (CDRMIP)
#export EXP_TO_PROCESS=PI-EDC-pulse-01  #ESM-esm-pi-cdr-pulse (neg) (CDRMIP)
#export EXP_TO_PROCESS=PI-EDC-pulse-02  #ESM-esm-pi-CO2pulse (pos) (CDRMIP)
#export EXP_TO_PROCESS=PI-EDC-ZEC-01  #ESM-esm-1pct-brch-1000PgC (C4MIP)
#export EXP_TO_PROCESS=PI-EDC-ZEC-02  #ESM-esm-1pct-brch-750PgC (C4MIP)
#export EXP_TO_PROCESS=PI-EDC-ZEC-03  #ESM-esm-1pct-brch-2000PgC (C4MIP)
#export EXP_TO_PROCESS=lig127k        #ESM-lig127K (PMIP)
#export EXP_TO_PROCESS=AM-rf-01         #ESM-piClim-control (RFMIP)
#export EXP_TO_PROCESS=AM-rf-4xCO2-01   #ESM-piClim-4xCO2 (RFMIP)
#export EXP_TO_PROCESS=AM-rf-ghg-01     #ESM-piClim-ghg (RFMIP)
#export EXP_TO_PROCESS=AM-rf-aer-01     #ESM-piClim-aer (RFMIP)
#export EXP_TO_PROCESS=AM-rf-anth-01    #ESM-piClim-anthro (RFMIP)
#export EXP_TO_PROCESS=AM-rf-lu-01    #ESM-piClim-lu (RFMIP)
#export EXP_TO_PROCESS=HI-aer-01        #ESM-hist-aer (DAMIP)   HI-aer-01,02,03 (+SSP-245-*)
#export EXP_TO_PROCESS=HI-ghg-01        #ESM-hist-ghg (DAMIP)   HI-ghg-01,02,03 (+SSP-245-*)
#export EXP_TO_PROCESS=HI-nat-01        #ESM-hist-nat (DAMIP)   HI-nat-01,02,03 (+SSP-245-*)
#export EXP_TO_PROCESS=PI-CN-02         #ESM-CN-piC
#export EXP_TO_PROCESS=PI-1pct-CN-01    #ESM-CN-1pct
#export EXP_TO_PROCESS=PI-1pbgc-CN-01   #ESM-CN-1pct-bgc
#export EXP_TO_PROCESS=PI-1prad-CN-01   #ESM-CN-1pct-rad
#export EXP_TO_PROCESS=PI-C-02          #ESM-C-piC
#export EXP_TO_PROCESS=PI-1pct-C-01     #ESM-C-1pct
#export EXP_TO_PROCESS=PI-1pbgc-C-01    #ESM-C-1pct-bgc
#export EXP_TO_PROCESS=PI-1prad-C-01    #ESM-C-1pct-rad
#export EXP_TO_PROCESS=SSP-245-tyb-01    #ESM-ssp245-covid (CovidMIP) SSP-245-tyb-01..34
#export EXP_TO_PROCESS=SSP-245-stg-01    #ESM-ssp245-cov-strgreen (CovidMIP) SSP-245-stg-01..14
#export EXP_TO_PROCESS=SSP-245-mog-01    #ESM-ssp245-cov-modgreen (CovidMIP) SSP-245-mog-01..14
#export EXP_TO_PROCESS=SSP-245-fos-01    #ESM-ssp245-cov-fossil (CovidMIP) SSP-245-fos-01..14
#
# CMIP6 table/variable to process. Default is 'all'.
export TABLE_TO_PROCESS=all
export VARIABLE_TO_PROCESS=all
#
# Table selection options
export SUBDAILY=true
export DAYMON=true
export YEARLY=true
export CO2AMON=true
#
# Variable input options
export RESTRICT_TO_INCOMPLETE=false
export PRIORITY_ONLY=false
export FORCE_DREQ=false
#
# Select environment
export CMIP6_ENV=publication # publication or claire
#
# If inline argument is passed
if [ ! -z $1 ]; then
  export EXP_TO_PROCESS=$1
fi
#
################################################################
# SETTING UP ENVIROMENT, VARIABLE MAPS, AND DATABASE
################################################################

# Set up environment
source ./subroutines/setup_env_cmip6.sh ${CMIP6_ENV}

# Cleanup output_files
./subroutines/cleanup.sh $OUT_DIR

# Update 'version' in experiment json file to today or chosen date
export version=$(date '+%Y%m%d')
#export version=20201207
./input_files/json/update_versions.sh

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
if (($NUM_ROWS <= 24)); then
  NUM_CPUS=$NUM_ROWS
else
  NUM_CPUS=24
fi
NUM_MEM=$(echo "${NUM_CPUS} * 16" | bc)
#NUM_MEM=$NUM_CPUS
if ((${NUM_MEM} >= 192)); then
  NUM_MEM=192
fi
echo number of cpus to to be used: ${NUM_CPUS}
echo total amount of memory to be used: ${NUM_MEM}Gb

cat << EOF > $APP_JOB
#!/bin/bash
#PBS -P p66
#PBS -q normal
#PBS -l storage=scratch/p66+gdata/p66+gdata/hh5+gdata/access
#PBS -l ncpus=${NUM_CPUS},walltime=24:00:00,mem=${NUM_MEM}Gb,wd
#PBS -j oe
#PBS -o ${JOB_OUTPUT}
#PBS -e ${JOB_OUTPUT}
#PBS -N app_${EXP_TO_PROCESS}
export EXP_TO_PROCESS=${EXP_TO_PROCESS}
module purge
# pre
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
chmod -R 644 ${OUT_DIR}
echo "APP completed for exp ${EXP_TO_PROCESS}."
EOF

/bin/chmod 775 ${APP_JOB}
echo "app job script: ${APP_JOB}"
qsub ${APP_JOB}
