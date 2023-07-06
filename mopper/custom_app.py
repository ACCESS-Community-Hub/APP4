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
import calendar
import glob
import yaml
import json
import csv
import sqlite3
import subprocess
import ast
from collections import OrderedDict
from datetime import datetime, timedelta


def write_variable_map(outpath, table, matches):
    """Write variables mapping to json file
    """
    with open(f"{outpath}/{table}.json", 'w') as fjson:
        json.dump(matches, fjson, indent=2)
    fjson.close()


def define_timeshot(frequency, resample, cell_methods):
    """Returns timeshot based on frequency, cell_methods and resample.
    It also fixes and returns frequency for pressure levels and
    climatology data.
    If data will be resample timeshot is mean/max/min
    """
    if 'time:' in cell_methods:
        bits = cell_methods.split()
        timeshot = bits[bits.index('time:') + 1]
    else:
        timeshot = ''
    if 'Pt' in frequency:
        timeshot = 'point'
        frequency = str(frequency)[:-2]
    elif frequency == 'monC':
        timeshot = 'clim'
        frequency = 'mon'
    # if timeshot is maximum/minimum/sum then leave it unalterated
    # otherwise resampled values is mean
    # for maximum, minimum pass timeshot as the resample method
    if resample != '':
        if timeshot in ['mean', 'point', '']:
            timeshot = 'mean'
        elif timeshot in ['maximum', 'minimum']:
            timeshot = timeshot[:3]
    return timeshot, frequency


def find_matches(table, var, realm, frequency, varlist):
    """Finds variable matching constraints given by table and config
    settings and returns a dictionary with the variable specifications. 
    NB. if an exact match (cmor name, realm, frequency is not found) 
    will try to find same cmor name and realm but different frequency.

    Parameters
    ----------
    table : str
        Variable table 
    var : str
        Variable cmor/cmip style name to match
    realm : str
        Variable realm to match
    frequency : str
        Variable frequency to match
    varlist : list
        List of variables, each represented by a dictionary with mappings
        used to find a match to "var" passed 

    Returns
    -------
    match : dict
        Dictionary containing matched variable specifications
        or None if not matches
    """
    near_matches = []
    found = False
    match = None
    for v in varlist:
        if v['cmip_var'].startswith('#'):
            pass
        elif (v['cmip_var'] == var and v['realm'] == realm 
              and v['frequency'] == frequency):
            match = v
            found = True
        elif v['cmip_var'] == var and v['realm'] == realm:
            near_matches.append(v)
    if found is False:
        v = check_best_match(near_matches, frequency)
        if v is not None:
            match = v
            found = True
        else:
            print(f"could not find match for {table}-{var}-{frequency}")
    if found is True:
        #PP should we review this approach? shouldn't be anything with time is inst time_0 is mean etc?
        resample = match.get('resample', '')
        timeshot, frequency = define_timeshot(frequency, resample,
            match['cell_methods'])
        match['resample'] = resample
        match['timeshot'] = timeshot
        match['table'] = table
        match['frequency'] = frequency
        if match['realm'] == 'land':
            realmdir = 'atmos'
        else:
            realmdir = match['realm']
        match['file_structure'] = f"/{realmdir}/{match['filename']}*.nc"
        #match['file_structure'] = f"/atm/netCDF/{match['filename']}*.nc"
    return match


def check_best_match(varlist, frequency):
    """If variable is present in file at different frequencies,
    finds the one with higher frequency nearest to desired frequency.
    Adds frequency to variable resample field.

    Parameters
    ----------
    varlist : list
        Subset of variables with same realm and cmor name but different
        frequency
    frequency : str
        Variable frequency to match

    Returns
    -------
    var : dict
        Dictionary containing matched variable specifications
        or None if not matches
    """
    var = None
    found = False
    resample_order = ['10yr', 'yr', 'mon', '10day', '7day',
            'day', '12hr', '6hr', '3hr', '1hr', '30min', '10min']
    resample_frq = {'10yr': '10Y', 'yr': 'Y', 'mon': 'M', '10day': '10D',
                    '7day': '7D', 'day': 'D', '12hr': '12H', '6hr': '6H',
                    '3hr': '3H', '1hr': 'H', '10min': '10T'}
    freq_idx = resample_order.index(frequency)
    for frq in resample_order[freq_idx+1:]:
        for v in varlist:
            vfrq = v['frequency'].replace('Pt','').replace('C','')
            if vfrq == frq:
                v['resample'] = resample_frq[frequency]
                found = True
                var = v
                break
        if found:
            break
    return var


def read_yaml(fname):
    """Read yaml file
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
    """Sets up the configuration dictionary based on config file input

    Parameters
    ----------
    config : dict(dict)
        Dictionary including 'cmor' settings and attributes for experiment

    Returns
    -------
    config : dict(dict)
        Updated dictionary including 'cmor' settings and attributes for experiment
    """
    cdict = config['cmor']
    #output_loc and main are the same previously also outpath
    if cdict['maindir'] == 'default':
        cdict['maindir'] = f"/scratch/{cdict['project']}/{os.getenv('USER')}/APP5_output"
    #PP not sure it ever get used
    cdict['outpath'] = f"{cdict['maindir']}/APP_job_files/{cdict['exp']}"
    # just making sure that custom_py is not in subroutines
    # cdict['appdir'] = cdict['appdir'].replace('/subroutines','')
    cdict['master_map'] = f"{cdict['appdir']}/{cdict['master_map']}"
    cdict['tables_path'] = f"{cdict['appdir']}/{cdict['tables_path']}"
    # we probably don't need this??? just transfer to custom_app.yaml
    # dreq file is the only field that wasn't yet present!
    #cdict['exps_table'] = f"{cdict['appdir']}/data/experiments.csv" 
    # Output subdirectories
    cdict['variable_maps'] = f"{cdict['outpath']}/variable_maps"
    cdict['success_lists'] = f"{cdict['outpath']}/success_lists"
    cdict['cmor_logs'] = f"{cdict['outpath']}/cmor_logs"
    cdict['var_logs'] = f"{cdict['outpath']}/variable_logs"
    cdict['app_logs'] = f"{cdict['outpath']}/app_logs"
    # Output files
    cdict['app_job'] = f"{cdict['outpath']}/app_job.sh"
    cdict['job_output'] =f"{cdict['outpath']}/job_output.OU"
    cdict['database'] = f"{cdict['outpath']}/app5.db"
    # reference_date
    if cdict['reference_date'] == 'default':
        cdict['reference_date'] = f"{cdict['start_date'][:4]}-{cdict['start_date'][4:6]}-{cdict['start_date'][6:8]}"
        print(cdict['reference_date'])
    config['cmor'] = cdict
    # if parent False set parent attrs to 'no parent'
    print(config['attrs']['parent'])
    if config['attrs']['parent'] is False:
        p_attrs = [k for k in config['attrs'].keys() if 'parent' in k]
        for k in p_attrs:
            config['attrs'][k] = 'no parent'
    return config


#PP might not need this anymore
#PP currently used by check_tables 
# unless is need to process dreq
def define_tables():
    """
    """
    UM_tables = ['3hr','AERmon','AERday','CFmon',
        'Eday','Eyr','fx','6hrLev','Amon','E3hr','Efx',
        'LImon','day','6hrPlev','6hrPlevPt','CF3hr','E3hrPt','Emon',
        'Lmon','EdayZ','EmonZ','AmonZ','Aday','AdayZ','A10dayPt']
    MOM_tables = ['Oclim','Omon','Oday','Oyr','Ofx','Emon','Eyr','3hr']
    CICE_tables = ['SImon','SIday']
    CMIP_tables = UM_tables + MOM_tables + CICE_tables
    return CMIP_tables 

#PP not sure we really need this either
def check_table(tables):
    """Check if list of tables are defined in CMIP/custom tables
    """
    CMIP_tables = define_tables()
    if tables in CMIP_tables:
        pass
    elif tables == 'all':
        pass
    else:
        sys.exit(f"table '{tables}' not in CMIP_tables list. "+
                "Check spelling of table, or CMIP_tables list in '{os.path.basename(__file__)}'")
    return


def check_output_directory(path):
    """Check if mapping directory exists and remove pre-existing files 
    """
    if len(glob.glob(f"{path}/*.csv")) == 0:
        print(f"variable map directory: '{path}'")
    else:
        for fname in glob.glob(f"{path}/*.csv"):
            os.remove(fname)
        print(f"variable maps deleted from directory '{path}'")
    return


def check_path(path):
    """Check if path exists, if not creates it
    """
    if os.path.exists(path):
        print(f"found directory '{path}'")
    else:
        try:
            os.makedirs(path)
            print(f"created directory '{path}'")
        except OSError as e:
            sys.exit(f"failed to create directory '{path}';" +
                     "please create manually. \nexiting.")
    return


def find_custom_tables(cdict):
    """Returns list of tables files in custom table path
    """
    tables = []
    path = cdict['tables_path']
    tables = glob.glob(f"{path}/*_*.json")
    for f in table_files:
        f = f.replace(".json", "")
        #tables.append(f.split("_")[1])
        tables.append(f)
    return tables


#PP part of using dreq need to double check e verything
def find_cmip_tables(dreq):
    """

    Returns
    -------
    """
    tables=[]
    with open(dreq, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if not row[0] in tables:
                if (row[0] != 'Notes') and (row[0] != 'MIP table') and (row[0] != '0'):
                    if (row[0].find('hr') != -1):
                        if subdaily:
                            tables.append(f"CMIP6_{row[0]}")
                    else:
                        if daymonyr:
                            tables.append(f"CMIP6_{row[0]}")
    f.close()
    return tables


def check_file(fname):
    """Check if file exists, if not stop execution
    """
    if os.path.exists(fname):
        print(f"found file '{fname}'")
    else:
        sys.exit(f"file '{fname}' does not exist!")
    return


def check_output_directory(path):
    """Check if path contains older mapping files,if yes
    it removes them
    """
    if len(glob.glob(f"{path}/*.csv")) == 0:
        print(f"variable map directory: '{path}'")
    else:
        for fname in glob.glob(f"{path}/*.csv"):
            os.remove(fname)
        print(f"variable maps deleted from directory '{path}'")
    return


#PP part of dreq not needed otherwise
def reallocate_years(years, reference_date):
    """Reallocate years based on dreq years 
    Not sure what it does need to ask Chloe
    """
    reference_date = int(reference_date[:4])
    if reference_date < 1850:
        years = [year-1850+reference_date for year in years]
    else:
        pass
    return years


def fix_years(years, tstart, tend):
    """Update start and end date for experiment based on dreq
    constraints for years. It is called only if dreq and dreq_years are True

    Parameters
    ----------
    years : list
        List of years from dreq file
    tstart: str
        Date of experiment start as defined in config
    tend: str
        Date of experiment end as defined in config

    Returns
    -------
    tstart: str
        Updated date of experiment start
    tend: str
        Updated date of experiment end
    """
    if tstart >= years[0]:
        pass
    elif (tstart < years[0]) and (tend >= years[0]):
        tstart = years[0]
    else:
        tstart = None 
    if tend <= years[-1]:
        pass
    elif (tend > years[-1]) and (tstart <= years[-1]):
        tend = years[-1]
    else:
        tstart = None 
    return tstart, tend


def read_dreq_vars2(cdict, table_id, activity_id):
    """Reads dreq variables file and returns a list of variables included in
    activity_id and experiment_id, also return dreq_years list

    Parameters
    ----------
    cdict : dict
        Dictionary with post-processing config 
    table_id : str
        CMIP table id
    activity_id: str
        CMIP activity_id

    Returns
    -------
    dreq_variables : dict
        Dictionary where keys are cmor name of selected variables and
        values are corresponding dreq years
    """
    with open(cdict['dreq'], 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        dreq_variables = {} 
        for row in reader:
            if (row[0] == table_id) and (row[12] not in ['', 'CMOR Name']):
                cmorname = row[12]
                mips = row[28].split(',')
                if activity_id not in mips:
                    continue
                try:
                    #PP if years==rangeplu surely calling this function will fail
                    # in any cas eis really unclear what reallocate years does and why, it returns different years
                    # if ref date before 1850???
                    if 'range' in row[31]:
                        years = reallocate_years(
                                ast.literal_eval(row[31]), cdict['reference_date'])
                        years = f'"{years}"'
                    elif 'All' in row[31]:
                        years = 'all'
                    else:
                        try:
                            years = ast.literal_eval(row[31])
                            years = reallocate_years(years, cdict['reference_date'])
                            years = f'"{years}"'
                        except:
                            years = 'all'
                except:
                    years = 'all'
                dreq_variables[cmorname] = years
    f.close()
    return dreq_variables


def create_variable_map(cdict, table, masters, activity_id=None, 
                        selection=None):
    """Create a mapping file for this specific experiment based on 
    model ouptut mappings, variables listed in table/s passed by config.
    Called by var_map

    Parameters
    ----------

    Returns
    -------
    """
    matches = []
    fpath = f"{cdict['tables_path']}/{table}.json"
    table_id = table.split('_')[1]
    with open(fpath, 'r') as fj:
         vardict = json.load(fj)
    row_dict = vardict['variable_entry']
    all_vars = [v for v in row_dict.keys()]
    # work out which variables you want to process
    select = all_vars 
    if selection is not None:
        select = [v for v in all_vars if v in selection]
    elif cdict['variable_to_process'] != 'all':
        select = [cdict['variable_to_process']]
    elif cdict['force_dreq'] is True:
        dreq_years = read_dreq_vars2(cdict, table_id, activity_id)
        all_dreq = [v for v in dreq_years.keys()]
        select = set(select).intersection(all_dreq) 
    for var,row in row_dict.items():
        #var = row['cmor_name']
        if var not in select:
            continue
        frequency = row['frequency']
        realm = row['modeling_realm']
        years = 'all'
        if cdict['force_dreq'] and var in all_dreq:
            years = dreq_years[var]
        match = find_matches(table, var, realm, frequency, masters)
        if match is not None:
            match['years'] = years
            matches.append(match)
    if matches == []:
        print(f"{table}:  no matching variables found")
    else:
        write_variable_map(cdict['variable_maps'], table, matches)
    return


#PP this is where dreq process start it can probably be simplified
# if we can read dreq as any other variable list
# and change year start end according to experiment
def var_map(cdict, activity_id=None):
    """
    """
    tables = cdict.get('tables', 'all')
    #PP probably don't these two?
    subdaily = True
    daymonyr = True
    subd = cdict.get('subdaily', 'false')
    if subd not in ['true', 'false', 'only']:
        print(f"Invalid subdaily option: {subd}")
        sys.exit()
    if subd == 'only':
        daymonyr = False
    elif subd == 'false':
        subdaily = False
    subset = cdict.get('var_subset_list', '')
    if subset == '':
        priorityonly = False
    elif subset[-5:] != '.yaml':
        print(f"{subset} should be a yaml file")
        sys.exit()
    else:
        subset = f"{cdict['appdir']}/{subset}"
        check_file(subset)
        priorityonly = True
# Custom mode vars
    if cdict['mode'].lower() == 'custom':
        access_version = cdict['access_version']
        start_year = int(cdict['start_date'])
        end_year = int(cdict['end_date'])
    # probably no need to check this!!
    check_path(cdict['variable_maps'])
    if cdict['force_dreq'] is True:
        if cdict['dreq'] == 'default':
            cdict['dreq'] = 'data/dreq/cmvme_all_piControl_3_3.csv'
        check_file(cdict['dreq'])
    check_file(cdict['master_map'])
    with open(cdict['master_map'],'r') as f:
        reader = csv.DictReader(f)
        masters = list(reader)
    f.close()
    # this is removing .csv files from variable_maps, is it necessary???
    check_output_directory(cdict['variable_maps'])
    print(f"beginning creation of variable maps in directory '{cdict['variable_maps']}'")
    if priorityonly:
        selection = read_yaml(subset)
        tables = [t for t in selection.keys()] 
        for table in tables:
            print(f"\n{table}:")
            create_variable_map(cdict, table, masters,
                selection=selection[table])

    elif tables.lower() == 'all':
        print(f"no priority list for local experiment '{cdict['exp']}', processing all variables")
        if cdict['force_dreq'] == True:
            tables = find_cmip_tables(cdict['dreq'])
        else:
            tables = find_custom_tables(cdict)
        for table in tables:
            print(f"\n{table}:")
            create_variable_map(cdict, table, masters, activity_id)
    else:
        create_variable_map(cdict, tables, masters)
    return cdict


#PP still creating a file_master table what to store in it might change!
def master_setup(conn):
    """Sets up file_master table in database
    """
    cursor = conn.cursor()
    #cursor.execute('drop table if exists file_master')
    #Create the file_master table
    try:
        cursor.execute('''create table if not exists file_master(
            infile text,
            outpath text,
            file_name text,
            vin text,
            variable_id text,
            ctable text,
            frequency text,
            realm text,
            timeshot text,
            tstart integer,
            tend integer,
            status text,
            file_size real,
            local_exp_id text,
            calculation text,
            resample text,
            in_units text,
            positive text,
            cfname text,
            source_id text,
            access_version text,
            json_file_path text,
            reference_date text,
            version text,
            primary key(local_exp_id,variable_id,ctable,tstart,version))''')
    except Exception as e:
        print("Unable to create the APP file_master table.\n {e}")
        raise e
    conn.commit()
    return


def cleanup(config):
    """Prepare output directories and removes pre-existing ones
    """
    # check if output path already exists
    cdict = config['cmor']
    outpath = cdict['outpath']
    if os.path.exists(outpath):
        answer = input(f"Output directory '{outpath}' exists.\n"+
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
    for fpath in toremove:
        try:
            os.remove(filePath)
        except OSerror as e:
            print(f"Error while deleting {fpath}: {e}")
    print("Preparing job_files directory...")
    # Creating output directories
    os.makedirs(cdict['success_lists'], exist_ok=True)
    os.mkdir(cdict['variable_maps'])
    os.mkdir(cdict['cmor_logs'])
    os.mkdir(cdict['var_logs'])
    os.mkdir(cdict['app_logs'])
    # copy CV file to CMIP6_CV.json
    shutil.copyfile(f"{cdict['tables_path']}/{cdict['_control_vocabulary_file']}",
                    f"{cdict['outpath']}/CMIP6_CV.json")
    return


def define_template(cdict, flag, nrows):
    """Defines job file template
    not setting contact and I'm sure I don't need the other envs either!
    CONTACT={cdict['contact']}

    Parameters
    ----------
    cdict : dict
        Dictonary with cmor settings for experiment
    """
    template = f"""#!/bin/bash
#PBS -P {cdict['project']} 
#PBS -q {cdict['queue']}
#PBS -l {flag}
#PBS -l ncpus={cdict['ncpus']},walltime=1:00:00,mem={cdict['nmem']}GB,wd
#PBS -j oe
#PBS -o {cdict['job_output']}
#PBS -e {cdict['job_output']}
#PBS -N custom_app4_{cdict['exp']}

module use /g/data/hh5/public/modules
module use ~access/modules
module load conda
PATH=${{PATH}}:/g/data/ua8/Working/packages/envs/newcmor/bin:/g/data/hh5/public/apps/miniconda3/bin
source activate /g/data/ua8/Working/packages/envs/newcmor

module list
python -V
# main
cd {cdict['appdir']}
python cli.py --debug -i {cdict['exp']}_config.yaml wrapper 
# post
#python {cdict['outpath']}/database_updater.py
sort {cdict['success_lists']}/{cdict['exp']}_success.csv \
    > {cdict['success_lists']}/{cdict['exp']}_success_sorted.csv
mv {cdict['success_lists']}/{cdict['exp']}_success_sorted.csv \
    {cdict['success_lists']}/{cdict['exp']}_success.csv
sort {cdict['success_lists']}/{cdict['exp']}_failed.csv \
    > {cdict['success_lists']}/{cdict['exp']}_failed_sorted.csv 2>/dev/null
mv {cdict['success_lists']}/{cdict['exp']}_failed_sorted.csv \
    {cdict['success_lists']}/{cdict['exp']}_failed.csv
echo 'APP completed for exp {cdict['exp']}.'"""
    return template


def write_job(cdict, nrows):
    """
    """
    # define storage flag
    flag = "storage=gdata/hh5+gdata/access"
    projects = cdict['addprojs'] + [cdict['project']]
    for proj in projects:
       flag += f"+scratch/{proj}+gdata/{proj}"
    # work out number of cpus based on number of files to process
    if nrows <= 24:
        cdict['ncpus'] = nrows
    else:
        cdict['ncpus'] = 24
    cdict['nmem'] = cdict['ncpus'] * cdict['mem_per_cpu']
#NUM_MEM=$(echo "${NUM_CPUS} * ${MEM_PER_CPU}" | bc)
    if cdict['nmem'] >= 1470: 
        cdict['nmem'] = 1470
    print(f"number of files to create: {nrows}")
    print(f"number of cpus to be used: {cdict['ncpus']}")
    print(f"total amount of memory to be used: {cdict['nmem']}GB")
    fpath = cdict['app_job']
    template = define_template(cdict, flag, nrows)
    with open(fpath, 'w') as f:
        f.write(template)
    return cdict


def create_exp_json(config, json_cv):
    """Create a json file as expected by CMOR to describe the dataset
    and passed the main global attributes.

    Parameters
    ----------
    config : dict(dict)
        Dictionary with both cmor settings and attributes defined for experiment
    json_cv : str
        Path of CV json file to edit

    Returns
    -------
    fname : str
        Name of created experiment json file
    """
    # outpath empty, calendar not there
    # template outpath etc different use <> instead of {}
    cdict = config['cmor']
    attrs = config['attrs']
    # read required attributes from cv file
    with open(json_cv, 'r') as f:
        json_cv_dict=json.load(f, object_pairs_hook=OrderedDict)
    f.close()
    required = json_cv_dict['CV']['required_global_attributes']
    # add attributes for path and file template to required
    tmp_str = (cdict['path_template'].replace('}/{','/') 
               + cdict['file_template'].replace('}_{','/'))
    attrs_template = tmp_str.replace('}','').replace('{','').split('/') 
    required.extend( set(attrs_template))
    # these are probably needed by cmor
    required.extend(['_cmip6_option', '_control_vocabulary_file',
        '_AXIS_ENTRY_FILE', '_FORMULA_VAR_FILE', 'outpath'] )
    # create global attributes dict to save
    glob_attrs = {}
    attrs_keys = [k for k in attrs.keys()]
    for k in required:
        if k in attrs_keys:
            glob_attrs[k] = attrs[k]
        else:
            glob_attrs[k] = cdict.get(k, '')
    # temporary correction until CMIP6_CV file anme is not anymore hardcoded in CMOR
    glob_attrs['_control_vocabulary_file'] = f"{cdict['outpath']}/CMIP6_CV.json"
    # replace {} _ and / in output templates
    glob_attrs['output_path_template'] = cdict['path_template'].replace('{','<').replace('}','>').replace('/','')
    glob_attrs['output_file_template'] = cdict['file_template'].replace('}_{','><').replace('}','>').replace('{','<')
    #glob_attrs['table_id'] = cdict['table']
    if cdict['mode'] == 'cmip6':
        glob_attrs['experiment'] = attrs['experiment_id']
    else:
        glob_attrs['experiment'] = cdict.get('exp','')
    # write glob_attrs dict to json file
    fname = f"{cdict['outpath']}/{cdict['exp']}.json"
    # parent attrs don't seem to be included should I add them manually?
    # at least for mode = cmip6
    json_data = json.dumps(glob_attrs, indent = 4, sort_keys = True, default = str)
    with open(fname, 'w') as f:
        f.write(json_data)
    f.close()
    return fname


def edit_json_cv(json_cv, attrs):
    """Edit the CMIP6 CV json file to include extra activity_ids and
    experiment_ids, so they can be recognised by CMOR when following 
    CMIP6 standards.

    Parameters
    ----------
    json_cv : str
        Path of CV json file to edit
    attrs: dict
        Dictionary with attributes defined for experiment

    Returns
    -------
    """
    activity_id = attrs['activity_id']
    experiment_id = attrs['experiment_id']

    with open(json_cv, 'r') as f:
        json_cv_dict = json.load(f, object_pairs_hook=OrderedDict)
    f.close()

    if activity_id not in json_cv_dict['CV']['activity_id']:
        print(f"activity_id '{activity_id}' not in CV, adding")
        json_cv_dict['CV']['activity_id'][activity_id] = activity_id

    if experiment_id not in json_cv_dict['CV']['experiment_id']:
        print(f"experiment_id '{attrs['experiment_id']}' not in CV, adding")
        json_cv_dict['CV']['experiment_id'][experiment_id] = OrderedDict({
        'activity_id': [activity_id],
        'additional_allowed_model_components': ['AER','CHEM','BGC'],
        'experiment': experiment_id,
        'experiment_id': experiment_id,
        'parent_activity_id': [attrs['parent_activity_id']],
        'parent_experiment_id': [attrs['parent_experiment_id']],
        'required_model_components': [attrs['source_type']],
        'sub_experiment_id': ['none']
        })
    else:
        print(f"experiment_id '{experiment_id}' found, updating")
        json_cv_dict['CV']['experiment_id'][experiment_id] = OrderedDict({
        'activity_id': [activity_id],
        'additional_allowed_model_components': ['AER','CHEM','BGC'],
        'experiment': experiment_id,
        'experiment_id': experiment_id,
        'parent_activity_id': [attrs['parent_activity_id']],
        'parent_experiment_id': [attrs['parent_experiment_id']],
        'required_model_components': [attrs['source_type']],
        'sub_experiment_id': ['none']
        })
    with open(json_cv,'w') as f:
        json.dump(json_cv_dict, f, indent=4, separators=(',', ': '))
    f.close
    return


#PP I have the feeling that pupulate/ppulate_unlimtied etc might be joined into one?
def populate(conn, config):
    """Populate file_master db table, this will be used by app to
    process all files

    Parameters
    ----------
    conn : obj 
        DB connection object
    config : dict(dict) 
        Dictionary including 'cmor' settings and attributes for experiment

    Returns
    -------
    """
    #defaults
    #config['cmor']['status'] = 'unprocessed'
    #get experiment information
    opts = {}
    opts['status'] = 'unprocessed'
    opts['outpath'] = config['cmor']['outpath']
    config['attrs']['version'] = config['attrs'].get('version', datetime.today().strftime('%Y%m%d'))
    #Experiment Details:
    for k,v in config['attrs'].items():
        opts[k] = v
    opts['version'] = config['attrs'].get('version', datetime.today().strftime('%Y%m%d'))
    opts['local_exp_id'] = config['cmor']['exp'] 
    opts['local_exp_dir'] = config['cmor']['datadir']
    opts['reference_date'] = config['cmor']['reference_date']
    opts['exp_start'] = config['cmor']['start_date'] 
    opts['exp_end'] = config['cmor']['end_date']
    opts['access_version'] = config['cmor']['access_version']
    opts['json_file_path'] = config['cmor']['json_file_path'] 
    print(f"found local experiment: {opts['local_exp_id']}")
    cursor = conn.cursor()
    #monthly, daily unlimited except cable or moses specific diagnostics
    rows = []
    tables = glob.glob(f"{config['cmor']['variable_maps']}/*.json")
    for table in tables:
        with open(table, 'r') as fjson:
            data = json.load(fjson)
        rows.extend(data)
    populate_rows(rows, config['cmor'], opts, cursor)
    conn.commit()
    return


def add_row(values, cursor):
    """Add a row to the file_master database table
       one row specifies the information to produce one output cmip5 file

    Parameters
    ----------
    values : list
        Path of CV json file to edit
    cursor : obj 
        Dictionary with attributes defined for experiment
    Returns
    -------
    """
    try:
        cursor.execute('''insert into file_master
            (infile, outpath, file_name, vin, variable_id,
            ctable, frequency, realm, timeshot, tstart, tend,
            status, file_size, local_exp_id, calculation,
            resample, in_units, positive, cfname, source_id,
            access_version, json_file_path, reference_date, version)
        values
            (:infile, :outpath, :file_name, :vin, :variable_id,
            :table, :frequency, :realm, :timeshot, :tstart, :tend,
            :status, :file_size, :local_exp_id, :calculation,
            :resample, :in_units, :positive, :cfname, :source_id,
            :access_version, :json_file_path, :reference_date,
            :version)''',
            values)
    except sqlite3.IntegrityError as e:
        print(f"Row already exists:\n{e}")
    except Exception as e:
        print(f"Could not insert row for {values['file_name']}:\n{e}")
    return cursor.lastrowid


def check_calculation(opts, insize):
    """

    Returns
    -------
    """
    # transport/transects/tiles should reduce size
    # volume,any vertical sum
    # resample will affect frequency but that should be already taken into acocunt in mapping
    grid_size = insize
    return grid_size


#PP if this approach is ok I should move the interval definition out of here
# and as for everything else in yaml file
def computeFileSize(cdict, opts, grid_size, frequency):
    """Calculate an estimated output file size (in megabytes)
       and the interval to use to satisfy max_size decided by user

    Parameters
    ----------
    json_cv : str
        Path of CV json file to edit
    attrs: dict
        Dictionary with attributes defined for experiment

    Returns
    -------
    """
    nstep_day = {'10min': 1440, '1hr': 24, '3hr': 8, '6hr':4, 'day':1,
             '10day': 0.1, 'mon': 1/30, 'yr': 1/365, 'dec': 1/3650}
    max_size = cdict['max_size']
    # work out if grid-size might change because of calculation
    if opts['calculation'] != '':
        grid_size = check_calculation(opts, grid_size)
    size_tstep = int(grid_size)/(1024**2)

    # work out how long is the all span in days
    start = datetime.strptime(str(cdict['start_date']), '%Y%m%d').date()
    finish = datetime.strptime(str(cdict['end_date']), '%Y%m%d').date()
    delta = (finish - start).days + 1
    # calculate the size of various intervals depending on timestep frequency
    size = {}
    size['days=1'] = size_tstep * nstep_day[frequency]
    size[f'days={delta}'] = size['days=1'] * delta
    size['days=7'] = size['days=1'] * 7
    size['months=1'] = size['days=1'] * 30
    size['years=1'] = size['months=1'] * 12
    size['years=10'] = size['years=1'] * 10
    size['years=100'] = size['years=10'] * 10
    # Now evaluate intervals in order starting from all timeseries and then from longer to shorter
    if size[f'days={delta}'] <= max_size*1.1:
        interval = f'days={delta}' 
    else:
        for interval in ['years=100', 'years=10', 'years=1',
                         'months=1', 'days=7', 'days=1']:
            if max_size*0.3 <= size[interval] <= max_size*1.1:
                print(f'create files at {interval} interval')
                break
    return interval, size[interval]


#PP I super simplified this not sure there's much point in trying to double guess final name
# it might be enough to make sure dates are correct?
def build_filename(cdict, opts):
    """Builds name for file to be created based on template in config
    NB we are using and approximations for dates
    not including here exact hour

    Parameters
    ----------
    cdict : dict
        Dictonary with cmor settings for experiment
    opts : dict
        Dictionary with attributes for a specific variable

    Returns
    -------
    fname : str
        Name for file to be created
    """
    date = opts['version']
    tString = ''
    frequency = opts['frequency']
    if frequency != 'fx':
        #time values
        start = opts['tstart']
        fin = opts['tend']
        start = f"{start.strftime('%4Y%m%d')}"
        fin = f"{fin.strftime('%4Y%m%d')}"
        opts['date_range'] = f"{start}-{fin}"
    else:
        opts['date_range'] = ""
    #P use path_template and file_template instead
    template = (f"{cdict['outpath']}/{cdict['path_template']}"
               + f"{cdict['file_template']}")
    fname = template.format(**opts) 
    return fname


def populate_rows(rows, cdict, opts, cursor):
    """Populates file_master table, with values from config and mapping.
    Works out how many files to generate based on grid size. 

    Parameters
    ----------
    rows : list(dict)
        List of dictionaries where each item represents one file to create
    cdict : dict
        Dictonary with cmor settings for experiment
    opts : dict
        Dictionary with attributes of specific variable to update
    cursor : obj
        Cursor of db connection object

    Returns
    -------
    """
    tableToFreq = read_yaml(f"data/table2freq.yaml")
    for champ in rows:
        #from champions table:
        table_id = champ['table'].split('_')[1]
        frequency = tableToFreq[table_id]
        opts['frequency'] = frequency
        opts['realm'] = champ['realm']
        opts['table'] = champ['table']
        opts['table_id'] = table_id
        opts['variable_id'] = champ['cmip_var'] # cmip_var
        opts['vin'] = champ['input_vars'] # access_vars
        paths = champ['file_structure'].split() 
        opts['infile'] = ''
        for x in paths:
            opts['infile'] += f"{opts['local_exp_dir']}/{x} "
        opts['calculation'] = champ['calculation']
        opts['resample'] = champ['resample']
        opts['in_units'] = champ['units']
        opts['positive'] = champ['positive']
        opts['timeshot'] = champ['timeshot']
        opts['cfname'] = champ['standard_name']
        exp_start = opts['exp_start']
        exp_end = opts['exp_end']
        if champ['years'] != 'all' and cdict['dreq_years']:
            exp_start, exp_time = fix_years(champ['years'], exp_start, exp_end) 
            if exp_start is None:
                print("Years requested for variable are outside specified"
                     f"period: {table_id}, {var}, {match['tstart']}, {match['tend']}")
                continue
        time= datetime.strptime(str(exp_start), '%Y%m%d').date()
        finish = datetime.strptime(str(exp_end), '%Y%m%d').date()
        interval, opts['file_size'] = computeFileSize(cdict, opts, champ['size'], champ['frequency'])
        #loop over times
        while (time < finish):
            delta = eval(f"timedelta({interval})")
            newtime = min(time+delta, finish)
            opts['tstart'] = time
            opts['tend'] = newtime
            opts['file_name'] = build_filename(cdict, opts)
            rowid = add_row(opts, cursor)
            time = newtime
    return


def count_rows(conn, exp):
    """Returns number of files to process
    """
    cursor=conn.cursor()
    cursor.execute(f"select * from file_master where status=='unprocessed' and local_exp_id=='{exp}'")
    #cursor.execute(f"select * from file_master")
    rows = cursor.fetchall()
    print(f"Number of rows in file_master: {len(rows)}")
    return len(rows)


def sum_file_sizes(conn):
    """Returns estimate of total size of files to process
    """
    cursor=conn.cursor()
    cursor.execute('select file_size from file_master')
    sizeList=cursor.fetchall()
    size=0.0
    for s in sizeList:
        size += float(s[0])
    size = size/1024.
    return size


def main():
    """Main section: 
    * takes one argument the config yaml file with list of settings
      and attributes to add to files
    * set up paths and config dictionaries
    * updates CV json file if necessary
    * select variables and corresponding mappings based on table
      and constraints passed in config file
    * create/update database file_master table to list files to create
    * write job executable file and submit to queue 
    """
    config_file = sys.argv[1]
    # first read config passed by user
    config = read_yaml(config_file)
    # then add setup_env to config
    config = setup_env(config)
    cdict = config['cmor']
    cleanup(config)
    #json_cv = f"{cdict['outpath']}/{cdict['_control_vocabulary_file']}"
    json_cv = f"{cdict['outpath']}/CMIP6_CV.json"
    fname = create_exp_json(config, json_cv)
    cdict['json_file_path'] = fname
    if cdict['mode'] == 'cmip6':
        edit_json_cv(json_cv, config['attrs'])
        cdict = var_map(cdict, config['attrs']['activity_id'])
    else:
        cdict = var_map(cdict)
    #database_manager
    database = cdict['database']
    print(f"creating & using database: {database}")
    conn = sqlite3.connect(database)
    conn.text_factory = str
    #setup database tables
    master_setup(conn)
    populate(conn, config)
    #PP this can be totally done directly in cli.py, if it needs doing at all!
    #create_database_updater()
    nrows = count_rows(conn, cdict['exp'])
    tot_size = sum_file_sizes(conn)
    print(f"max total file size is: {tot_size} GB")
    #write app_job.sh
    config['cmor'] = write_job(cdict, nrows)
    print(f"app job script: {cdict['app_job']}")
    # write setting to yaml file to pass to wrapper
    fname = f"{cdict['exp']}_config.yaml"
    print("Exporting config data to yaml file")
    write_yaml(config, fname)
    #submint job
    os.chmod(cdict['app_job'], 775)
    #status = subprocess.run(f"qsub {cdict['app_job']}", shell=True)
    #if status.returncode != 0:
    #    print(f"{cdict['app_job']} submission failed, returned code is {status.returncode}.\n Try manually")
    

if __name__ == "__main__":
    main()
