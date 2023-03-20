# This script takes the experiment information in custom-mode APP4 and edits it into:
#   input_files/custom_mode_cmor-tables/Tables/CMIP6_CV.json
#   input_files/json/default_[version].json
# This allows CMOR to be generalised for non-CMIP6 experiment-level vocabulary.
# porting to python3: paola.petrelli@utas.edu.au

import json, os
from collections import OrderedDict

#=os.environ.get('')
outdir = os.environ.get('OUT_DIR')
maindir = os.environ.get('MAIN_DIR')
version = os.environ.get('VERSION')
exptoprocess = os.environ.get('EXP_TO_PROCESS')
#
experiment_id = os.environ.get('experiment_id')
activity_id = os.environ.get('activity_id')
realization_index = os.environ.get('realization_index')
initialization_index = os.environ.get('initialization_index')
physics_index = os.environ.get('physics_index')
forcing_index = os.environ.get('forcing_index')
source_type = os.environ.get('source_type')
branch_time_in_child = os.environ.get('branch_time_in_child')
#
if os.environ.get('parent').lower() == 'true':
    parent_experiment_id = os.environ.get('parent_experiment_id')
    parent_activity_id = os.environ.get('parent_activity_id')
    parent_mip_era = 'CMIP6'
    parent_time_units = os.environ.get('parent_time_units')
    branch_time_in_parent = os.environ.get('branch_time_in_parent')
    parent_variant_label = os.environ.get('parent_variant_label')
else:
    parent_experiment_id = 'no parent'
    parent_activity_id = 'no parent'
    parent_mip_era = 'no parent'
    parent_time_units = 'no parent'
    branch_time_in_parent = 'no parent'
    parent_variant_label = 'no parent'

if version == 'ESM':
    json_exp = 'input_files/json/default_esm.json'
elif version == 'CM2':
    json_exp = 'input_files/json/default_cm2.json'
json_cv = 'input_files/custom_mode_cmor-tables/Tables/CMIP6_CV.json'

#
# Edit experiment-specific json
#
# PP might be more sensible just writing this directly as a custom config??
def edit_exp_json():
    with open(json_exp,'r') as f:
        json_exp_dict=json.load(f, object_pairs_hook=OrderedDict)
    f.close()
    
    json_exp_dict['outpath'] = maindir
    json_exp_dict['experiment_id'] = experiment_id
    json_exp_dict['activity_id'] = activity_id
    json_exp_dict['realization_index'] = realization_index
    json_exp_dict['initialization_index'] = initialization_index
    json_exp_dict['physics_index'] = physics_index
    json_exp_dict['forcing_index'] = forcing_index
    json_exp_dict['source_type'] = source_type
    json_exp_dict['branch_time_in_child'] = branch_time_in_child
    #
    json_exp_dict['parent_experiment_id'] = parent_experiment_id
    json_exp_dict['parent_activity_id'] = parent_activity_id
    json_exp_dict['parent_mip_era'] = parent_mip_era
    json_exp_dict['parent_time_units'] = parent_time_units
    json_exp_dict['branch_time_in_parent'] = branch_time_in_parent
    json_exp_dict['parent_variant_label'] = parent_variant_label
    
    with open('{}/{}.json'.format(outdir,exptoprocess),'w') as f:
        json.dump(json_exp_dict, f, indent=4, separators=(',', ': '))
    f.close
    return

#
# Edit 'custom' CMOR tables with experiment details
#
def edit_cv_json():
    with open(json_cv,'r') as f:
        json_cv_dict=json.load(f, object_pairs_hook=OrderedDict)
    f.close()
    
    if not activity_id in json_cv_dict['CV']['activity_id']: 
        print("activity_id '{activity_id}' not in CV, adding")
        json_cv_dict['CV']['activity_id'][activity_id] = activity_id
    
    if not experiment_id in json_cv_dict['CV']['experiment_id']: 
        print("experiment_id '{experiment_id}' not in CV, adding")
        json_cv_dict['CV']['experiment_id'][experiment_id] = OrderedDict({
        'activity_id': [activity_id],
        'additional_allowed_model_components': ['AER','CHEM','BGC'],
        'experiment': experiment_id,
        'experiment_id': experiment_id,
        'parent_activity_id': [parent_activity_id],
        'parent_experiment_id': [parent_experiment_id],
        'required_model_components': [source_type],
        'sub_experiment_id': ['none']
        })
    else:
        print("experiment_id '{experiment_id}' found, updating")
        json_cv_dict['CV']['experiment_id'][experiment_id] = OrderedDict({
        'activity_id': [activity_id],
        'additional_allowed_model_components': ['AER','CHEM','BGC'],
        'experiment': experiment_id,
        'experiment_id': experiment_id,
        'parent_activity_id': [parent_activity_id],
        'parent_experiment_id': [parent_experiment_id],
        'required_model_components': [source_type],
        'sub_experiment_id': ['none']
        })
    
    with open(json_cv,'w') as f:
        json.dump(json_cv_dict, f, indent=4, separators=(',', ': '))
    f.close
    return

def main():
    print("\nstarting custom_json_editor...")
    print("using experiment json template: {json_exp}")
    edit_exp_json()
    print("created experiment file: {outdir}/{exptoprocess}")
    print("editing CV json file: {json_cv}")
    edit_cv_json()
    print("custom_json_editor complete, metadata prepared for use in CMOR")

if __name__ == "__main__":
    main()
    
