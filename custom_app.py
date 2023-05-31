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
    """
    """
    with open(f"{outpath}/{table}.json", 'w') as fjson:
        json.dump(matches, fjson, indent=2)
    fjson.close()


#PP probably we don't need this anymore!
def determine_dimension(freq, dimensions, timeshot, realm, table, skip):
    """
    """
    UM_realms, MOM_realms, CICE_realms = define_realms()
    if skip:
        dimension = ''
    elif (freq == 'fx') or (dimensions.find('time') == -1):
        dimension = 'fx'
    elif (timeshot == 'clim') or (dimensions.find('time2') != -1):
        dimension = 'clim'
    elif len(dimensions.split()) == 1:
        dimension = 'scalar'
    elif dimensions.find('alev') != -1:
        if realm in UM_realms:
            dimension = '3Dalev'
        else:
            raise Exception('E: realm not identified')
    elif dimensions.find('plev') != -1:
        if realm in UM_realms:
            dimension = '3Datmos'
        else:
            raise Exception('E: realm not identified')
    elif dimensions.find('olev') != -1:
        if realm in MOM_realms:
            dimension = '3Docean'
        else:
            raise Exception('E: realm not identified')
    elif dimensions.find('sdepth') != -1:
        if realm in UM_realms:
            if dimensions.find('sdepth1') != -1:
                dimension = '2Datmos'
            else:
                dimension = '3Datmos'
        else:
            raise Exception('E: realm not identified')
    else:
        if realm in UM_realms:
            dimension = '2Datmos'
        elif realm in MOM_realms:
            dimension = '2Docean'
        elif realm in CICE_realms:
            dimension = '2Dseaice'
        else:
            raise Exception('E: no dimension identified')
    return dimension


def find_match2(table, var, realm, frequency, varlist):
    """
    """
    near_matches = []
    found = False
    match = None
    if 'Pt' in frequency:
        timeshot = 'inst'
        frequency = str(frequency)[:-2]
    elif frequency == 'monC':
        timeshot = 'clim'
        frequency = 'mon'
    else:
        timeshot = 'mean'
    for v in varlist:
        if v['cmip_var'].startswith('#'):
            pass
        elif (v['cmip_var'] == var and v['realm'] == realm 
              and v['frequency'] == frequency):
            match = v
            found = True
        elif v['cmip_var'] == var and v['realm'] == realm:
            near_matches.append(v)
    if not found:
        v = check_best_match(near_matches, frequency, table)
        if v is not None:
            match = v
            found = True
        else:
            print(f"could not find match for {table}-{var}-{frequency}")
    if found:
        match['timeshot'] = timeshot
        match['table'] = table
        #match['file_structure'] = f"/{match['realm']}/{match['filename']}*.nc"
        match['file_structure'] = f"/atm/netCDF/{match['filename']}*.nc"
    return match


def check_best_match(varlist, frequency, table):
    """If variable is present in file at different frequencies,
       find the one with higher frequency nearest to desired frequency.
    """
    var = None
    found = False
    resample_order = ['10yr', 'yr', 'mon', '10day', '7day',
            'day', '12hr', '6hr', '3hr', '1hr', '30min', '10min']
    freq_idx = resample_order.index(frequency)
    for frq in resample_order[freq_idx+1:]:
        for v in varlist:
            if v['frequency'] == frq:
                #v['frequency'] = frequency
                v['calculation'] += f",time_resample({frequency})"
                found = True
                var = v
                break
        if found:
            break
    return var


#PP not sure if we still need this for list of variables coming form dreq
def find_matches(cdict, table, cmorname, realm, freq, cfname,
                 years, dimensions):
    matches = [] 
    if 'Pt' in freq:
        timeshot = 'inst'
        freq = str(freq)[:-2]
    elif freq == 'monC':
        timeshot = 'clim'
        freq = 'mon'
    else:
        timeshot = 'mean'
    with open(cdict['master_map'],'r') as g:
        champ_reader = csv.DictReader(g)
        varlist = list(champ_reader)
    for v in var_list:
        if v['cmip_var'].startswith('#'):
            pass
        elif v['cmip_var'] == cmorname and v['version'] == cdict['access_version']:
            v['file_structure'] = f"/{v['realm']}/{v['filename']}*.nc"
            v['timeshot'] = timeshot
            v['years'] = years
            v['cfname'] = cfname
            matches.append(v)
    g.close()
    return matches


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
    """Substitute setup_env.sh
    """
    # how are these 2 used???
    #PP this could be done better!!
    cdict = config['cmor']
    #output_loc and main are the same previously also outdir
    if cdict['maindir'] == 'default':
        cdict['maindir'] = f"/scratch/{cdict['project']}/{os.getenv('USER')}/APP5_output"
    #PP not sure it ever get used
    cdict['outdir'] = f"{cdict['maindir']}/APP_job_files/{cdict['exp']}"
    # just making sure that custom_py is not in subroutines
    cdict['appdir'] = cdict['appdir'].replace('/subroutines','')
    cdict['master_map'] = f"{cdict['appdir']}/{cdict['master_map']}"
    cdict['grid_file'] = f"{cdict['appdir']}/{cdict['grid_file']}"
    # we probably don't need this??? just transfer to custom_app.yaml
    # dreq file is the only field that wasn't yet present!
    #cdict['exps_table'] = f"{cdict['appdir']}/input_files/experiments.csv" 
    # Output subdirectories
    cdict['variable_maps'] = f"{cdict['outdir']}/variable_maps"
    cdict['success_lists'] = f"{cdict['outdir']}/success_lists"
    cdict['cmor_logs'] = f"{cdict['outdir']}/cmor_logs"
    cdict['var_logs'] = f"{cdict['outdir']}/variable_logs"
    cdict['app_logs'] = f"{cdict['outdir']}/app_logs"
    # Output files
    cdict['app_job'] = f"{cdict['outdir']}/app_job.sh"
    cdict['job_output'] =f"{cdict['outdir']}/job_output.OU"
    cdict['database'] = f"{cdict['outdir']}/app5.db"
    # reference_date
    if cdict['reference_date'] == 'default':
        cdict['reference_date'] = cdict['start_date']
    config['cmor'] = cdict
    return config


#PP might not need this anymore
# unless is need to process dreq
def define_realms():
    """
    """
    UM_realms = ['atmos','land','aerosol','atmosChem','landIce']
    MOM_realms = ['ocean','ocnBgchem']
    CICE_realms = ['seaIce']
    return UM_realms, MOM_realms, CICE_realms


#PP might not need this anymore
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


def check_table(tables):
    """
    """
    CMIP_tables = define_tables()
    if tables in CMIP_tables:
        pass
    elif tables == 'all':
        pass
    else:
        sys.exit(f"table '{tables}' not in CMIP_tables list. "+
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


def find_custom_tables(cdict):
    """Assumes second part of table names is unique
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
    """
    tables=[]
    with open(dreq, 'r') as f:
        reader=csv.reader(f, delimiter='\t')
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
    if os.path.exists(fname):
        print(f"found file '{fname}'")
    else:
        sys.exit(f"file '{fname}' does not exist!")


def check_output_directory(path):
    if len(glob.glob(f"{path}/*.csv")) == 0:
        print(f"variable map directory: '{path}'")
    else:
        for fname in glob.glob(f"{path}/*.csv"):
            os.remove(fname)
        print(f"variable maps deleted from directory '{path}'")


#PP part of dreq not needed otherwise
def reallocate_years(years, reference_date):
    reference_date = int(reference_date[:4])
    if reference_date < 1850:
        years = [year-1850+reference_date for year in years]
    else:
        pass
    return years


def fix_years(years, tstart, tend):
    """Call this only if years != 'all'
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



def read_dreq_vars2(cdict, table, activity_id):
    """
    Modified so we only return a list of variables that fits table and activity/experiment-id + years
    then we check if these are present in masters as we would with any list and take into acocunt years if they are!
    MIP table       Priority        Long name       units   description     comment Variable Name   CF Standard Name        cell_methods    positive        type    dimensions      CMOR Name       modeling_realm  frequency       cell_measures   prov    provNote        rowIndex        UID     vid     stid    Structure Title valid_min       valid_max       ok_min_mean_abs ok_max_mean_abs MIPs (requesting)       MIPs (by experiment)    Number of Years Slice Type      Years   Grid
    """
    with open(cdict['dreq'], 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        dreq_variables = {} 
        for row in reader:
            if (row[0] == table) and (row[12] not in ['', 'CMOR Name']):
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


#PP part of dreq not needed otherwise
def read_dreq_vars(cdict, table):
    """
    MIP table       Priority        Long name       units   description     comment Variable Name   CF Standard Name        cell_methods    positive        type    dimensions      CMOR Name       modeling_realm  frequency       cell_measures   prov    provNote        rowIndex        UID     vid     stid    Structure Title valid_min       valid_max       ok_min_mean_abs ok_max_mean_abs MIPs (requesting)       MIPs (by experiment)    Number of Years Slice Type      Years   Grid
    """
    with open(cdict['dreq'], 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        dreq_variables = []
        for row in reader:
            try:
                if (row[0] == table) and (row[12] != ''):
                    dimensions = row[11]
                    cmorname = row[12]
                    freq = row[14]
                    cfname = row[7]
                    realms = row[13]
                    try:
                        realm = realms.split()[0]
                    except:
                        realm = 'uncertain'
                    try:
                        if 'range' in row[31]:
                            years = reallocate_years(
                                    eval(row[31]), cdict['reference_date'])
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
                    if (cdict['mode'] == 'custom') or not cdict['dreq_years']:
                        years = 'all'
                    if cdict['variable_to_process'].lower() == 'all':
                        dreq_variables.append([cmorname, realm, freq,
                                          cfname, years, dimensions])
                    else:
                        if cmorname == cdict['variable_to_process']:
                            dreq_variables.append([cmorname, realm,
                                    freq, cfname, years, dimensions])
            except:
                pass
    f.close()
    return dreq_variables


def create_variable_map(cdict, ftable, masters, activity_id=None):
    """
    """
    matches = []
    #PP open table json files and get variable list
    #fpath = glob.glob(f"{cdict['tables_path']}/*_{table}.json")
    fpath = f"{cdict['tables_path']}/{ftable}.json"
    table = ftable.split('_')[1]
    with open(fpath, 'r') as fj:
         vardict = json.load(fj)
    row_dict = vardict['variable_entry']
    all_vars = [v for v in row_dict.keys()]
    # work out which variables you want to process
    select = all_vars 
    if cdict['variable_to_process'] != 'all':
        select = [cdict['variable_to_process']]
    if cdict['force_dreq'] is True:
        dreq_years = read_dreq_vars2(cdict, table, activity_id)
        all_dreq = [v for v in dreq_years.keys()]
        select = set(select).intersection(all_dreq) 
    for name,row in row_dict.items():
        var = row['out_name']
        if var not in select:
                    continue
        frequency = row['frequency']
        realm = row['modeling_realm']
        years = 'all'
        if cdict['force_dreq'] and var in all_dreq:
            years = dreq_years[var]
        match = find_match2(ftable, var, realm, frequency, masters)
        if match is not None:
            match['years'] = years
            matches.append(match)
    if matches == []:
        print(f"{ftable}:  no matching variables found")
    else:
        write_variable_map(cdict['variable_maps'], ftable, matches)


#PP this is where dreq process start it can probably be simplified
# if we can read dreq as any other variable list
# and change year start end according to experiment
def dreq_map(cdict, activity_id=None):
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
    varsub = cdict.get('var_subset_list', '')
    if varsub == '':
        priorityonly = False
    elif varsub[-5:] != 'yaml':
        print(f"{varsub} should be a yaml file")
        sys.exit()
    else:
        check_file(f"{cdict['appdir']}/{varsub}")
        priorityonly = True
    cdict['priorityonly'] = priorityonly
# Custom mode vars
    if cdict['mode'].lower() == 'custom':
        access_version = cdict['access_version']
        start_year = int(cdict['start_date'])
        end_year = int(cdict['end_date'])
        #PP should I check that is 8 long? ie.e with YYYYDDMM
        cdict['reference_date'] = cdict['start_date']
    #
    priority_vars=[]
    # look slike yaml file would be better!!
    # change to yaml file with table as a dict and vars as list under each table
    if priorityonly:
        try:
            with open(prioritylist,'r') as p:
                reader = csv.reader(p, delimiter=',')
                priority_vars.append([row[0],row[1]])
        except Error as e:
            print(f"Invalid variable subset list {varsub}: {e}")
            sys.exit()
    else:
        print(f"no priority list for local experiment '{cdict['exp']}', processing all variables")
    #PP we don't really need this as we're doing a similar process in dreq_map???
    #check_table(cdict['tables'])
    # probably no need to check this!!
    check_path(cdict['variable_maps'])
    if cdict['force_dreq'] is True:
        if cdict['dreq'] == 'default':
            cdict['dreq'] = 'input_files/dreq/cmvme_all_piControl_3_3.csv'
        check_file(cdict['dreq'])
    check_file(cdict['master_map'])
    with open(cdict['master_map'],'r') as g:
        reader = csv.DictReader(g)
        masters = list(reader)
    g.close()
    # this is removing .csv files from variable_maps, is it necessary???
    check_output_directory(cdict['variable_maps'])
    print(f"beginning creation of variable maps in directory '{cdict['variable_maps']}'")
    if tables.lower() == 'all':
        if cdict['force_dreq'] == True:
            tables = find_cmip_tables(cdict['dreq'])
        else:
            tables = find_custom_tables(cdict)
        for table in tables:
            print(f"\n{table}:")
            create_variable_map(cdict, table, masters, activity_id)
    else:
        table = tables
        create_variable_map(cdict, table, masters, activity_id)
    return cdict


#PP still creating a file_master table what to store in it might change!
def master_setup(conn):
    cursor=conn.cursor()
    #cursor.execute('drop table if exists file_master')
    #Create the file_master table
    # PP this is a mix of attributes that could be taken from yaml file and file specific settings that needs to be derived from master file
    try:
        cursor.execute( '''create table if not exists file_master(
            infile text,
            outpath text,
            file_name text,
            vin text,
            variable_id text,
            cmip_table text,
            frequency text,
            timeshot text,
            tstart integer,
            tend integer,
            status text,
            file_size real,
            local_exp_id text,
            calculation text,
            in_units text,
            positive text,
            cfname text,
            source_id text,
            access_version text,
            json_file_path text,
            reference_date integer,
            version text,
            primary key(local_exp_id,variable_id,cmip_table,tstart,version))''')
    except Exception as e:
        print("Unable to create the APP file_master table.\n {e}")
        raise e
    conn.commit()


def cleanup(config):
    """Substitute cleanup.sh
    """
    # check if output path already exists
    cdict = config['cmor']
    outpath = cdict['outdir']
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
    toremove.extend(glob.glob("./subroutines/*.pyc"))
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
    return


def define_template(cdict, flag, nrows):
    """
    not setting contact and I'm sure I don't need the other envs either!
    CONTACT={cdict['contact']}
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
# possibly I don't need to set this env vars
set -a
# pre
EXP_TO_PROCESS={cdict['exp']}
OUTPUT_LOC={cdict['maindir']}
MODE={cdict['mode']}
# main
python {cdict['appdir']}/subroutines/cli.py --debug -i {cdict['exp']}_config.yaml wrapper 
# post
#python {cdict['outdir']}/database_updater.py
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
    print(f"number of cpus to to be used: {cdict['ncpus']}")
    print(f"total amount of memory to be used: {cdict['nmem']}GB")
    fpath = cdict['app_job']
    template = define_template(cdict, flag, nrows)
    with open(fpath, 'w') as f:
        f.write(template)
    return cdict


def create_cv_json(exp, appdir, attrs):
    """
    """
    fname = f"{appdir}/input_files/json/{exp}.json"
    json_data = json.dumps(attrs, indent = 4, sort_keys = True)
    with open(fname, 'w') as f:
        f.write(json_data)
    f.close()
    return fname


# PP not sure I need this unless users doesn't pass one but then you're stuck
# at passing all cmip args so i think is better to choose a standard and stick to it!
def edit_cv_json(json_cv, attrs):
    """Temporarily copied as it is in custom_json-editor
    """
    activity_id = attrs['activity_id']
    experiment_id = attrs['experiment_id']

    with open(json_cv, 'r') as f:
        json_cv_dict=json.load(f, object_pairs_hook=OrderedDict)
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

#PP I have the feeling that pupulate/ppulate_unlimtied etc might be joined into one?
def populate(conn, config):
    #defaults
    #config['cmor']['status'] = 'unprocessed'
    #get experiment information
    opts = {}
    opts['status'] = 'unprocessed'
    opts['outpath'] = config['cmor']['outdir']
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
    populate_unlimited(cursor, config['cmor'], opts)
    conn.commit()


#populate the database for variables that are requested for all times for all experiments
def populate_unlimited(cursor, cdict, opts):
    #monthly, daily unlimited except cable or moses specific diagnostics
    rows = []
    tables = glob.glob(f"{cdict['variable_maps']}/*.json")
    for table in tables:
        with open(table, 'r') as fjson:
            data = json.load(fjson)
        rows.extend(data)
    populateRows(rows, cdict, opts, cursor)


def addRow(values, cursor):
    """Add a row to the file_master database table
       one row specifies the information to produce one output cmip5 file
    """
    try:
        cursor.execute('''insert into file_master
            (infile, outpath, file_name, vin, variable_id,
            cmip_table, frequency, timeshot, tstart, tend,
            status, file_size, local_exp_id, calculation,
            in_units, positive, cfname, source_id,
            access_version, json_file_path,reference_date, version)
        values
            (:infile, :outpath, :file_name, :vin, :variable_id,
            :cmip_table, :frequency, :timeshot, :tstart, :tend,
            :status, :file_size, :local_exp_id, :calculation,
            :in_units, :positive, :cfname, :source_id,
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
def buildFileName(cdict, opts):
    """
    finish is the last day covered by file as datetime object
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
    template = f"{cdict['outdir']}/{cdict['path_template']}{cdict['file_template']}"
    file_name = template.format(**opts) 
    return file_name


def populateRows(rows, cdict, opts, cursor):
    tableToFreq = read_yaml(f"input_files/table2freq.yaml")
    for champ in rows:
        #from champions table:
        table = champ['table'].split('_')[1]
        frequency = tableToFreq[table]
        opts['frequency'] = frequency
        opts['cmip_table'] = champ['table']
        opts['variable_id'] = champ['cmip_var'] # cmip_var
        opts['vin'] = champ['input_vars'] # access_vars
        paths = champ['file_structure'].split() 
        opts['infile'] = ''
        for x in paths:
            opts['infile'] += f"{opts['local_exp_dir']}/{x} "
        opts['calculation'] = champ['calculation']
        opts['in_units'] = champ['units']
        opts['positive'] = champ['positive']
        opts['timeshot'] = champ['timeshot']
        #opts['var_notes'] = champ[11]
        opts['cfname'] = champ['standard_name']
        #dimension = champ['dimensions']
        exp_start = opts['exp_start']
        exp_end = opts['exp_end']
        if champ['years'] != 'all' and cdict['dreq_years']:
            exp_start, exp_time = fix_years(champ['years'], exp_start, exp_end) 
            if exp_start is None:
                print("Years requested for variable are outside specified"
                     f"period: {table}, {var}, {match['tstart']}, {match['tend']}")
                continue
        time= datetime.strptime(str(exp_start), '%Y%m%d').date()
        finish = datetime.strptime(str(exp_end), '%Y%m%d').date()
        interval, opts['file_size'] = computeFileSize(cdict, opts, champ['size'], champ['frequency'])
        #PP resume from here and chnage again the file creation!
        #TODO add in check that there is only one value
        #loop over times
        while (time < finish):
            delta = eval(f"timedelta({interval})")
            newtime = min(time+delta, finish)
            opts['tstart'] = time
            opts['tend'] = newtime
            opts['file_name'] = buildFileName(cdict, opts)
            rowid = addRow(opts, cursor)
            time = newtime


def count_rows(conn):
    """Return number of files to process
    """
    cursor=conn.cursor()
    #cursor.execute(f"select * from file_master where status=='unprocessed' and local_exp_id=='{exptoprocess}'")
    cursor.execute(f"select * from file_master")
    rows = cursor.fetchall()
    print(f"Number of rows in file_master: {len(rows)}")
    return len(rows)


def sum_file_sizes(conn):
    """Return estimate of total size of files to process
    """
    cursor=conn.cursor()
    cursor.execute('select file_size from file_master')
    sizeList=cursor.fetchall()
    size=0.0
    for s in sizeList:
        size += float(s[0])
    return size/1024.


def main():
    config_file = sys.argv[1]
    # first read config passed by user
    config = read_yaml(config_file)
    # then add setup_env to config
    config = setup_env(config)
    cdict = config['cmor']
    cleanup(config)
    json_cv = f"{cdict['tables_path']}/{cdict['_control_vocabulary_file']}"
    #PP do we ened this??
    #PP apparently we do need a file like that because of CMOR but maybe not all the same attributes
    #PP here I'm creating one on the fly with the attributes frm custo_app.yaml
    # then if necessary we can add edit-cv when needed for non custom runs
    #fname = create_cv_json(cdict['exp'], cdict['appdir'], config['attrs'])
    #PP do I need this in cli.py?
    fname = f"{cdict['appdir']}/input_files/json/cm000.json"
    cdict['json_file_path'] = fname
    #cdict['json_file_path'] = json_cv
    if cdict['mode'] == 'cmip6':
        print(json_cv)
        edit_cv_json(json_cv, config['attrs'])
        cdict = dreq_map(cdict, config['attrs']['activity_id'])
    else:
        cdict = dreq_map(cdict)
    #database_manager
    database = cdict['database']
    print(f"creating & using database: {database}")
    conn = sqlite3.connect(database)
    conn.text_factory = str
    #setup database tables
    master_setup(conn)
    populate(conn, config)
    print('past populate')
    #PP this can be totally done directly in cli.py, if it needs doing at all!
    #create_database_updater()
    nrows = count_rows(conn)
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
