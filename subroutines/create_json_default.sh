#!/bin/bash
echo "creating json file"
#scriptpath="$( cd "$(dirname "$0")" ; pwd -P )"
#cd $scriptpath

json=${OUT_DIR}/${EXP_TO_PROCESS}.json

datevers=$(date '+%Y%m%d')

mkdir -p ${DATA_DIR}/CMORised_output

if ! $parent ; then
  parent_experiment_id="no parent"
  parent_activity_id="no parent"
  branch_time_in_parent="no parent"
  parent_mip_era="no parent"
  parent_time_units="no parent"
  parent_variant_label="no parent"
fi

if [[ $MODE == default ]]; then
  if [[ $VERSION == CM2 ]]; then
    :
  elif [[ $VERSION == ESM ]]; then
    echo """{
    \"_control_vocabulary_file\":     \"CMIP6_CV.json\",
    \"_cmip6_option\":                \"CMIP6\",
    \"activity_id\":                  \"${activity_id}\",
    \"outpath\":                      \"${DATA_DIR}/CMORised_output\",
    \"experiment_id\":                \"${experiment_id}\",
    \"parent_experiment_id\":         \"${parent_experiment_id}\",
    \"parent_activity_id\":           \"${parent_activity_id}\", 
    \"calendar\":                     \"proleptic_gregorian\",
    \"realization_index\":            \"${realization_index}\",
    \"initialization_index\":         \"${initialization_index}\",
    \"physics_index\":                \"${physics_index}\",
    \"forcing_index\":                \"${forcing_index}\",
    \"source_type\":                  \"${source_type}\",
    \"sub_experiment\":               \"none\",
    \"sub_experiment_id\":            \"none\",
    \"branch_time_in_child\":         \"${branch_time_in_child}\",
    \"branch_time_in_parent\":        \"${branch_time_in_parent}\",
    \"branch_method\":                \"standard\",
    \"parent_mip_era\":               \"${parent_mip_era}\",
    \"parent_time_units\":            \"${parent_time_units}\",
    \"parent_variant_label\":         \"${parent_variant_label}\",
    \"parent_source_id\":             \"ACCESS-ESM1-5\",
    \"grid\":                         \"native atmosphere N96 grid (145x192 latxlon)\",
    \"grid_label\":                   \"gn\",
    \"nominal_resolution\":           \"250 km\",
    \"tracking_prefix\":              \"hdl:21.14100\",
    \"institution_id\":               \"CSIRO\",
    \"institution\":                  \"Commonwealth Scientific and Industrial Research Organisation, Aspendale, Victoria 3195, Australia\",
    \"run_variant\":                  \"forcing: GHG, Oz, SA, Sl, Vl, BC, OC, (GHG = CO2, N2O, CH4, CFC11, CFC12, CFC113, HCFC22, HFC125, HFC134a)\",
    \"source_id\":                    \"ACCESS-ESM1-5\",
    \"source\":                       \"ACCESS-ESM1.5 (2019): aerosol: CLASSIC (v1.0), atmos: HadGAM2 (r1.1, N96; 192 x 145 longitude/latitude; 38 levels; top level 39255 m), atmosChem: none, land: CABLE2.4, landIce: none, ocean: ACCESS-OM2 (MOM5, tripolar primarily 1deg; 360 x 300 longitude/latitude; 50 levels; top grid cell 0-10 m), ocnBgchem: WOMBAT (same grid as ocean), seaIce: CICE4.1 (same grid as ocean)\",
    \"version\":                      \"v${datevers}\",
    \"output_path_template\":         \"<activity_id><institution_id><source_id><experiment_id><variant_label><table><variable_id><grid_label><version>\",
    \"output_file_template\":         \"<variable_id><table><source_id><experiment_id><variant_label><grid_label>\",
    \"license\":                      \"CMIP6 model data produced by CSIRO is licensed under a Creative Commons Attribution-ShareAlike 4.0 International License (https://creativecommons.org/licenses/). Consult https://pcmdi.llnl.gov/CMIP6/TermsOfUse for terms of use governing CMIP6 output, including citation requirements and proper acknowledgment.  Further information about this data, including some limitations, can be found via the further_info_url (recorded as a global attribute in this file). The data producers and data providers make no warranty, either express or implied, including, but not limited to, warranties of merchantability and fitness for a particular purpose. All liabilities arising from the supply of the information (including any liability arising in negligence) are excluded to the fullest extent permitted by law.\"
}
    """ > ${OUT_DIR}/${EXP_TO_PROCESS}.json
  else
    echo "default jsons only exist for CM2 and ESM thus far"
  fi
fi

#    \"_cmip6_option\":                \"CMIP6\",
