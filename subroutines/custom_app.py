#!/usr/bin/env python
# Copyright 2023 ARC Centre of Excellence for Climate Extremes (CLEX)
# Author: Paola Petrelli <paola.petrelli@utas.edu.au> 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# contact: paola.petrelli@utas.edu.au
# last updated 16/04/2023

################################################################
#
# This is the ACCESS Post-Processor, v5.0
# 16/04/2023
# 
# Developed by Paola Petrelli, Sam Green CLEX
# based on prior work by Chloe Mackallah, Peter Uhe and others at CSIRO
#
################################################################
#
# CUSTOM MODE - USE THIS FOR NON-CMIP6 EXPERIMENTS (I.E. CUSTOM METADATA)
#
# THE APP4 WILL INSERT THE DETAILS DEFINED BELOW INTO THE CMIP6_CV.JSON FILE
# TO ENABLE NON-CMIP6 EXPERIMENTS TO BE CMORISED
#
#
# SETTING UP ENVIROMENT, VARIABLE MAPS, AND DATABASE
################################################################
# exit back to check_app4 script if being used
#if [[ $check_app4 == 'true' ]] ; then return ; fi

import os
import sys
import shutil
import glob
import yaml
import json


def read_yaml(fname):
    """
    """
    with open(fname, 'r') as yfile:
        data = yaml.safe_load(yfile)
    return data


def write_yaml(data, fname='exp_config.yaml'):
    """Write data to a yaml file

    Parameters
    ----------
    data : dict
        The file content as adictioanry 
    fname : str
        Yaml filename (default: exp_config.yaml)

    Returns
    -------
    """
    try:
        with open(fname, 'w') as f:
            yaml.dump(data, f)
    except:
        print(f"Check that {data} exists and it is an object compatible with json")
    return


def setup_env(config):
    """Substitue setup_env.sh
    """
    # how are these 2 used???
    #PP this could be done better!!
    app_dir = os.getcwd().split('/subroutines')[0]
    cdict = config['cmor']
    attrs = config['attrs']
    if cdict['outdir'] == 'default':
        cdict['outdir'] = f"/scratch/{cdict['project']}/{os.getenv('USER')}/APP4_output"
    cdict['main_dir'] = cdict['outdir']
    #PP not sure it ever get used
    #cdict['data_dir'] = cdict['datadir']
    cdict['output'] = f"{cdict['output_loc']}/APP_job_files/{cdict['exp']}"
    # we probably don't need this??? just transfer to custom_appp.yaml
    # dreq file is the only field that wasn't yet present!
    cdict['exps_table'] = f"${app_dir}/input_files/experiments.csv" 
    # Output subdirectories
    config['variable_maps'] = f"{config['out_dir']}/variable_maps"
    config['success_lists'] = f"{config['out_dir']}/success_lists"
    config['cmor_logs'] = f"{config['out_dir']}/cmor_logs"
    config['var_logs'] = f"{config['out_dir']}/variable_logs"
    config['app_logs'] = f"{config['out_dir']}/app_logs"
    # Output files
    config['app_job'] = f"{config['out_dir']}/app_job.sh"
    config['job_output'] =f"{config['out_dir']}/job_output.OU"
    config['database'] = f"{config['out_dir']}/database.db"
    return config

def define_tables():
    """
    """
    UM_realms = ['atmos','land','aerosol','atmosChem','landIce']
    MOM_realms = ['ocean','ocnBgchem']
    CICE_realms = ['seaIce']
    UM_tables = ['3hr','AERmon','AERday','CFmon',
        'Eday','Eyr','fx','6hrLev','Amon','E3hr','Efx',
        'LImon','day','6hrPlev','6hrPlevPt','CF3hr','E3hrPt','Emon',
        'Lmon','EdayZ','EmonZ','AmonZ','Aday','AdayZ','A10dayPt']
    MOM_tables = ['Oclim','Omon','Oday','Oyr','Ofx','Emon','Eyr','3hr']
    CICE_tables = ['SImon','SIday']
    CMIP_tables = UM_tables + MOM_tables + CICE_tables
    return 



def check_table():
    if tabletoprocess in CMIP_tables:
        pass
    elif tabletoprocess == 'all':
        pass
    else:
        sys.exit(f"table '{tabletoprocess}' not in CMIP_tables list. "+
                "Check spelling of table, or CMIP_tables list in '{os.path.basename(__file__)}'")


def check_output_directory(path):
    if len(glob.glob(f"{path}/*.csv")) == 0:
        print(f"variable map directory: '{path}'")
    else:
        for fname in glob.glob(f"{path}/*.csv"):
            os.remove(fname)
        print(f"variable maps deleted from directory '{path}'")

def check_path(path):
    if os.path.exists(path):
        print(f"found directory '{path}'")
    else:
        try:
            os.makedirs(path)
            print(f"created directory '{path}'")
        except OSError as e:
            sys.exit(f"failed to create directory '{path}';" +
                     "please create manually. \nexiting.")


def find_cmip_tables(dreq):
    """
    """
    tables=[]
    with open(dreq, 'r') as f:
        reader=csv.reader(f, delimiter='\t')
        for row in reader:
            if not row[0] in tables:
                if (row[0] != 'Notes') and (row[0] != 'MIP table') and (row[0] != '0'):
                    if (row[0].find('hr') != -1):
                        if subdaily:
                            tables.append(row[0])
                    else:
                        if daymonyr:
                            tables.append(row[0])
    f.close()
    return tables


def dreq_map():
    """
    """
    table_to_process = ctx.obj.get('table_to_process', 'all')
    var_to_process = ctx.obj.get('variable_to_process', 'all')
    subdaily = True
    daymonyr = True
    subd = ctx.obj.get('subdaily', 'false')
    if subd not in ['true', 'false', 'only']:
        raise(f"Invalid subdaily option: {subdaily}")
    if subd == 'only':
        daymonyr = False
    elif subd == 'false':
        subdaily = False
    varsub = ctx.obj.get('var_subset')
    priorityonly = False
    if ctx.obj['var_subset'].lower() == 'true':
        priorityonly = True
    forcedreq = False
    if ctx.obj['force_dreq'].lower() == 'true':
        forcedreq = True
    dreq_years = False
    if ctx.obj['dreq_years'].lower() == 'true':
        dreq_years = True
# Custom mode vars
    if mode.lower() == 'custom':
        def_hist_data = ctx.obj['history_data']
        access_version = ctx.obj['version']
        def_start = int(ctx.obj['start_year'])
        def_end = int(ctx.obj['end_year'])
        dreq = ctx.obj['dreq']
        if dreq == 'default':
            dreq = 'input_files/dreq/cmvme_all_piControl_3_3.csv'
        elif not dreq.endswith('.csv'):
            print('E: dreq not a csv file')
            raise
        else:
            print(f"dreq file: {dreq}")
        #PP should I check that is 8 long? ie.e with YYYYDDMM
        reference_date = def_start
        start_year = def_start
        end_year = def_end
        local_exp_dir = def_hist_data
    else:
        #PP we probably don't need this!!!
        print(exptoprocess)
        with open(experimentstable,'r') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                try:
                    row[0]
                except:
                    row='#'
                if row[0].startswith('#'):
                    pass
                elif row[0] == exptoprocess:
                    access_version = row[7]
                    if forcedreq:
                        dreq='input_files/dreq/cmvme_all_piControl_3_3.csv'
                    else:
                        dreq=row[3]
                    reference_date = row[4]
                    start_year = row[5]
                    end_year = row[6]
                    local_exp_dir = row[1]
        f.close()
    if 'OM2' in access_version:
        print('ACCESS-OM2, using master_map_om2.csv')
        master_map = master_map.replace('.csv','_om2.csv')
    elif mode == 'ccmi':
        print('CCMI2022, using master_map_ccmi2022.csv')
        master_map = master_map.replace('.csv','_ccmi2022.csv')
    #PP probaly no need of this either as we should enforce DRS
    if os.path.exists(f"{local_exp_dir}/atm/netCDF/link/"):
        atm_file_struc='/atm/netCDF/link/'
    else:
        atm_file_struc='/atm/netCDF/'
    #PP probably can scrap priority this can be obtained by using varlist
    priority_vars=[]
    try:
        with open(prioritylist,'r') as p:
            reader = csv.reader(p, delimiter=',')
            for row in reader:
                try:
                    row[0]
                except:
                    row='#'
                if row[0].startswith('#'):
                    pass
                else:
                    priority_vars.append([row[0],row[1]])
        p.close()
    except:
        if priorityonly:
            print(f"no priority list for local experiment '{exptoprocess}', processing all variables")

def cleanup(config):
    """Substitute cleanup.sh
    """
    # check if output path already exists
    outpath = config['out_dir']
    if os.path.exists(outpath):
        answer = input(f"Output directory 'outpath' exists.\n"+
                       "Delete and continue? [Y,n]\n")
        if answer == 'Y':
            try:
                shutil.rmtree(outpath)
            except OSError as e:
                raise(f"Error couldn't delete {outpath}: {e}")
        else:
            print("Exiting")
            sys.exit()
    # If outpath doesn't exists or we just deleted
    # Get a list of all the file paths that ends with .txt from in specified directory
    toremove = glob.glob("./*.pyc'")
    toremove.extend(glob.glob("./subroutines/*.pyc"))
    for fpath in toremove:
        try:
            os.remove(filePath)
        except OSerror as e:
            print(f"Error while deleting {fpath}: {e}")
    print("Preparing job_files directory...")
    # Creating output directories
    os.makedirs(config['success_lists'], exist_ok=True)
    os.mkdir(config['variable_maps'])
    os.mkdir(config['cmor_logs'])
    os.mkdir(config['var_logs'])
    os.mkdir(config['app_logs'])
    return


def define_template(config, flag, nrows, ncpus, nmem):
    """
    """
    template = f"""#!/bin/bash
#PBS -P {config['project']} 
#PBS -q {config['queue']}
#PBS -l {flag}
#PBS -l ncpus={ncpus},walltime=1:00:00,mem={nmem}GB,wd
#PBS -j oe
#PBS -o ${config['job_output']}
#PBS -e ${config['job_output']}
#PBS -N custom_app4_{config['exp']}

module use /g/data/hh5/public/modules
module use ~access/modules
# doesn't seem to use parallel
#module load parallel
module load conda
PATH=${{PATH}}:/scratch/v45/pxp581/conda/envs/CMOR/bin:/g/data/hh5/public/apps/miniconda3/bin
source activate CMOR

module list
python -V
# possibly I don't need to set this env vars
set -a
# pre
EXP_TO_PROCESS={config['exp']}
OUTPUT_LOC={config['output_loc']}
MODE={config['mode']}
CONTACT={config['contact']}
#source ./subroutines/setup_env.sh
# main
python ./subroutines/cli.py --debug wrapper
# post
python {config['out_dir']}/database_updater.py
sort {config['success_lists']}/{config['exp']}_success.csv \
    > {config['success_lists']}/{config['exp']}_success_sorted.csv
mv {config['success_lists']}/{config['exp']}_success_sorted.csv \
    {config['success_lists']}/{config['exp']}_success.csv
sort {config['success_lists']}/{config['exp']}_failed.csv \
    > {config['success_lists']}/{config['exp']}_failed_sorted.csv 2>/dev/null
mv {config['success_lists']}/{config['exp']}_failed_sorted.csv \
    {config['success_lists']}/{config['exp']}_failed.csv
echo 'APP completed for exp {config['exp']}.'"""
    return template


def write_job(config):
    """
    """
    # define storage flag
    flag = "storage=gdata/hh5+gdata/access"
    projects = config['addprojs'] + [config['project']]
    for proj in projects:
       flag += f"+scratch/{proj}+gdata/{proj}"
    # work out number of cpus based on number of files to process
    #NUM_ROWS=$( cat $OUT_DIR/database_count.txt )
    nrows = 2 
    if nrows <= 24:
        ncpus = nrows
    else:
        ncpus = 24
    nmem = ncpus * config['mem_per_cpu']
#NUM_MEM=$(echo "${NUM_CPUS} * ${MEM_PER_CPU}" | bc)
    if nmem >= 1470: 
        nmem = 1470
    print(f"number of files to create: {nrows}")
    print(f"number of cpus to to be used: {ncpus}")
    print(f"total amount of memory to be used: {nmem}GB")
    fpath = config['app_job']
    template = define_template(config, flag, nrows, ncpus, nmem)
    with open(fpath, 'w') as f:
        f.write(template)
    return fpath


def edit_cv_json():
    """Temporarily copied as it is in custom_json-editor
    """
    with open(json_cv, 'r') as f:
        json_cv_dict=json.load(f, object_pairs_hook=OrderedDict)
    f.close()

    if not activity_id in json_cv_dict['CV']['activity_id']:
        print(f"activity_id '{activity_id}' not in CV, adding")
        json_cv_dict['CV']['activity_id'][activity_id] = activity_id

    if not experiment_id in json_cv_dict['CV']['experiment_id']:
        print(f"experiment_id '{experiment_id}' not in CV, adding")
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
        print(f"experiment_id '{experiment_id}' found, updating")
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


def add_exp(version):
    """Do i need this? Probably not most of this has been added to custom_app.yaml,
       so all the info is together
    """
    if version == 'ESM':
        json_exp = 'input_files/json/default_esm.json'
    elif version == 'CM2':
        json_exp = 'input_files/json/default_cm2.json'
    # add content parent_id relaiz. etc to content of this file
    #read file first create function
    exp_dict = read_json()
    return


def main():
    # first read config passed by user
    config = read_yaml('custom_app.yaml')
    print(config)
    # then add setup_env to config
    config = setup_env(config)
    cmord = config['cmor']
    fname = f"{config['exp']}_config.yaml"
    print("Exporting config data to yaml file")
    write_yaml(config, fname)
    cleanup(config)
    json_cv = 'input_files/custom_mode_cmor-tables/Tables/CMIP6_CV.json'
    edit_cv_json(json_cv)
    # mapping
    write_job(config)
    print(f"app job script: {config['app_job']}")

    
"""


# Create variable maps
python ./subroutines/dreq_mapping.py --multi
#exit

# Create database
python ./subroutines/database_manager.py
#exit
"""

################################################################
# CREATE JOB
################################################################
#echo -e '\ncreating job...'


#/bin/chmod 775 ${APP_JOB}
#qsub ${APP_JOB}
if __name__ == "__main__":
    main()
