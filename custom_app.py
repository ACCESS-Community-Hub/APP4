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
from collections import OrderedDict
from datetime import datetime, timedelta


def write_variable_map(outpath, table, matches):
    with open(f"{outpath}/{table}.csv", 'w') as fcsv:
        fwriter = csv.writer(fcsv, delimiter=',')
        fwriter.writerow(f"#cmip table: {table},,,,,,,,,,,,".split(","))
        header2 = ("#cmipvar,definable,access_vars,file_structure," +
                   "calculation,units,axes_modifier,positive," +
                   "timeshot,years,varnotes,cfname,dimension")
        fwriter.writerow(header2.split(","))
        for line in matches:
            print(f"  {line}")
            fwriter.writerow(f"{line}".split(","))
    fcsv.close()


def determine_dimension(freq, dimensions, timeshot, realm, table, skip):
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
        champ_reader = DictReader(g)
        var_list = list(dict_reader)
    for v in var_list
        if v['cmip_var'].startswith('#'):
            pass
        elif v['cmip_var'] == cmorname and v['version'] == cdict['access_version']:
            v['file_structure'] = f"/{v['realm']}/{v['finput']}*.nc"
            v['timeshot'] = timeshot
            v['years'] = years
            v['cfname'] = cfname
            matches.append(v)
    g.close()
    return matches


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
    config['cmor'] = cdict
    return config


def define_realms():
    """
    """
    UM_realms = ['atmos','land','aerosol','atmosChem','landIce']
    MOM_realms = ['ocean','ocnBgchem']
    CICE_realms = ['seaIce']
    return UM_realms, MOM_realms, CICE_realms


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


def check_table(table_to_process):
    """
    """
    CMIP_tables = define_tables()
    if table_to_process in CMIP_tables:
        pass
    elif table_to_process == 'all':
        pass
    else:
        sys.exit(f"table '{table_to_process}' not in CMIP_tables list. "+
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


def reallocate_years(years, reference_date):
    reference_date = int(reference_date)
    if reference_date < 1850:
        years = [year-1850+reference_date for year in years]
    else:
        pass
    return years

def read_dreq_vars(cdict, table):
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
    #print(dreq_variables)
    return dreq_variables


def create_variable_map(cdict, table):
    dreq_variables = read_dreq_vars(cdict, table)
    for cmorname, realm, freq, cfname, years, dimensions in dreq_variables:
        matches = find_matches(cdict, table, cmorname,
                 realm, freq, cfname, years, dimensions)
    if matches == []:
        print(f"{table}:  no ACCESS variables found")
    else:
        write_variable_map(cdict['variable_maps'], table, matches)
        

def dreq_map(cdict):
    """
    """
    table_to_process = cdict.get('table_to_process', 'all')
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
        dreq = cdict['dreq']
        if dreq == 'default':
            dreq = f"{cdict['appdir']}/input_files/dreq/cmvme_all_piControl_3_3.csv"
        elif not dreq.endswith('.csv'):
            print(f"E: {dreq} not a csv file")
            raise
        else:
            print(f"dreq file: {dreq}")
        #PP should I check that is 8 long? ie.e with YYYYDDMM
        cdict['reference_date'] = cdict['start_date']
        local_exp_dir = cdict['datadir'] 
    else:
        #PP we probably don't need this!!!
        with open(experimentstable,'r') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                try:
                    row[0]
                except:
                    row = '#'
                if row[0].startswith('#'):
                    pass
                elif row[0] == exp:
                    access_version = row[7]
                    if cdict['force_dreq']:
                        dreq = f"{cdict['appdir']}/input_files/dreq/cmvme_all_piControl_3_3.csv"
                    else:
                        dreq = row[3]
                    reference_date = row[4]
                    start_year = row[5]
                    end_year = row[6]
                    local_exp_dir = row[1]
        f.close()
    # update dreq file
    cdict['dreq'] = dreq
    if 'OM2' in access_version:
        cdict['master_map'] = cdict['master_map'].replace('.csv','_om2.csv')
    elif cdict['mode'] == 'ccmi':
        cdict['master_map'] = cdict['master_map'].replace('.csv','_ccmi2022.csv')
    print(f"ACCESS-{access_version}, using {cdict['master_map']}")
    #PP add here custom option
    #else:
    #
    #PP probaly no need of this either as we should enforce DRS
    if os.path.exists(f"{local_exp_dir}/atm/netCDF/link/"):
        atm_file_struc='/atm/netCDF/link/'
    else:
        atm_file_struc='/atm/netCDF/'
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
    check_table(cdict['table_to_process'])
    # probably no need to check this!!
    check_path(cdict['variable_maps'])
    check_file(dreq)
    check_file(cdict['master_map'])
    # this is removing .csv files from variable_maps, is it necessary???
    check_output_directory(cdict['variable_maps'])
    print(f"beginning creation of variable maps in directory '{cdict['variable_maps']}'")
    if table_to_process.lower() == 'all':
        tables = find_cmip_tables(dreq)
        for table in tables:
            print(f"\n{table}:")
            create_variable_map(cdict, table)
    else:
        table = table_to_process
        create_variable_map(cdict, table)
    return cdict


def master_setup(conn):
    cursor=conn.cursor()
    #cursor.execute('drop table if exists file_master')
    #Create the file_master table
    try:
        cursor.execute( '''create table if not exists file_master(
            experiment_id text,
            realization_idx integer,
            initialization_idx integer,
            physics_idx integer,
            forcing_idx integer,
            infile text,
            outpath text,
            file_name text,
            vin text,
            variable_id text,
            cmip_table text,
            frequency text,
            tstart integer,
            tend integer,
            status text,
            file_size real,
            local_exp_id text,
            calculation text,
            in_units text,
            positive text,
            timeshot text,
            years text,
            var_notes text,
            cfname text,
            activity_id text,
            institution_id text,
            source_id text,
            grid_label text,
            access_version text,
            json_file_path text,
            reference_date integer,
            version text,
            primary key(local_exp_id,experiment_id,variable_id,cmip_table,realization_idx,initialization_idx,physics_idx,forcing_idx,tstart))''')
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


def write_job(cdict):
    """
    """
    # define storage flag
    flag = "storage=gdata/hh5+gdata/access"
    projects = cdict['addprojs'] + [cdict['project']]
    for proj in projects:
       flag += f"+scratch/{proj}+gdata/{proj}"
    # work out number of cpus based on number of files to process
    #PP need to fix this still
    #NUM_ROWS=$( cat $OUT_DIR/database_count.txt )
    nrows = 2 
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


def champions_setup(conn, champions_dir):
    cursor = conn.cursor()
    cursor.execute('drop table if exists champions')
    try:
        cursor.execute( '''create table if not exists champions(
            cmip_table text,
            cmip_variable text,
            definable text,
            access_variable text,
            file_structure text,
            calculation text,
            in_units text,
            axes_modifier text,
            positive text,
            timeshot text,
            years text,
            notes text,
            cfname text,
            dimension text,
            primary key (cmip_variable,cmip_table))''')
    except:
        print("Unable to create the champions table.")
    cursor.execute('delete from champions')
    files = os.listdir(champions_dir)
    for table in files:
        if os.path.isdir(f"{champions_dir}/{table}"):
            continue
        if not table.startswith('.'): #hidden file, directory
            try:
                #PP added delimiter "," as separator
                #PP possibly if we should use csv instead of a basic file writer in dreq_mapping.py
                fcsv = open(f"{champions_dir}/{table}", 'r')
                freader = csv.reader(fcsv, delimiter=',')
                for line in freader:
                    if line[0][0] != '#':
                        row = ['N/A']*15
                        for i, item in enumerate(line):
                            row[i+1] = item
                        #cmip_table
                        row[0] = table[:-4]  #champions file with .csv removed
                        try:
                            cursor.execute('insert into champions values (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ',
                                    row[0:14])
                        except Exception as e:
                            print(f"error inserting line into champions file: {e}\n{row}")
                conn.commit()
            except Exception as e:
                print(e, table)
                raise
    conn.commit()


def populate(conn, config):
    cursor = conn.cursor()
    #defaults

    #config['cmor']['status'] = 'unprocessed'
    #get experiment information
    opts = {}
    opts['status'] = 'unprocessed'
    opts['outpath'] = config['cmor']['outdir']
    config['attrs']['version'] = config['attrs'].get('version', datetime.today().strftime('%Y%m%d'))
    #Experiment Details:
    opts['experiment_id'] = config['attrs']['experiment_id'] 
    opts['realization_idx'] = config['attrs']['realization_index']
    opts['initialization_idx'] = config['attrs']['initialization_index']
    opts['physics_idx'] = config['attrs']['physics_index']
    opts['forcing_idx'] = config['attrs']['forcing_index']
    opts['activity_id'] = config['attrs']['activity_id']
    opts['institution_id'] = config['attrs']['institution_id']
    opts['source_id'] = config['attrs']['source_id']
    opts['grid_label'] = config['attrs']['grid_label']
    opts['version'] = config['attrs'].get('version', datetime.today().strftime('%Y%m%d'))
    opts['local_exp_id'] = config['cmor']['exp'] 
    opts['local_exp_dir'] = config['cmor']['datadir']
    opts['reference_date'] = config['cmor']['reference_date']
    opts['exp_start'] = config['cmor']['start_date'] 
    opts['exp_end'] = config['cmor']['end_date']
    opts['access_version'] = config['cmor']['access_version']
    opts['json_file_path'] = config['cmor']['json_file_path'] 
    opts['cmip_exp_id'] = config['attrs']['experiment_id'] 
    print(f"found local experiment: {opts['local_exp_id']}")
    populate_unlimited(cursor, config['cmor'], opts)
    conn.commit()


#populate the database for variables that are requested for all times for all experiments
def populate_unlimited(cursor, cdict, opts):
    #monthly, daily unlimited except cable or moses specific diagnostics
    cursor.execute("select * from champions")
    rows = cursor.fetchall()
    #populateRows(cursor.fetchall(), opts, cursor)
    populateRows(rows, cdict, opts, cursor)


def addRow(values, cursor):
    """Add a row to the file_master database table
       one row specifies the information to produce one output cmip5 file
    """
    try:
        cursor.execute('''insert into file_master
            (experiment_id,realization_idx,initialization_idx,
            physics_idx,forcing_idx,infile,outpath,file_name,vin,
            variable_id,cmip_table,frequency,tstart,tend,status,
            file_size,local_exp_id,calculation,axes_modifier,
            in_units,positive,timeshot,years,var_notes,cfname,
            activity_id,institution_id,source_id,grid_label,
            access_version,json_file_path,reference_date,version)
        values
            (:experiment_id,:realization_idx,:initialization_idx,
            :physics_idx,:forcing_idx,:infile,:outpath,:file_name,:vin,
            :variable_id,:cmip_table,:frequency,:tstart,:tend,:status,
            :file_size,:local_exp_id,:calculation,:axes_modifier,
            :in_units,:positive,:timeshot,:years,:var_notes,:cfname,
            :activity_id,:institution_id,:source_id,:grid_label,
            :access_version,:json_file_path,:reference_date,:version)''',
            values)
    except sqlite3.IntegrityError as e:
        print(f"Row already exists:\n{e}")
    except Exception as e:
        print(f"Could not insert row for {values['file_name']}:\n{e}")
    return cursor.lastrowid


def computeFileSize(grid_points, frequency, length):
    """Calculate an estimated output file size (in megabytes)
    """
    #additional amount to take into account space the grid and metadata take up
    length = float(length)
    # base size is for fixed data
    #    return grid_points*4/1024/1024.0
    size = grid_points*4.0/1024.0**2
    if frequency == 'yr':
        size = size*length/365
        #return ((length/365)*grid_points*4))/1024.0^2
    elif frequency == 'mon':
        size = size * length * 12/365
        #return length/365*12*grid_points*4/1024/1024.0
    elif frequency == 'day':
        size = size * length
        #return length*grid_points*4/1024/1024.0
    elif frequency == 'monClim':
        size = size * 12
        #return 12*grid_points*4/1024/1024.0
    elif frequency == '6hr':
        size = size * length * 4
        #return length*grid_points*4/1024/1024.0*4
    elif frequency == '3hr':
        size = size * length * 8
        #return length*grid_points*4/1024/1024.0*8
    elif frequency == '1hr':
        size = size * length * 24
    elif frequency == '10min':
        size = size * length * 144
    else:
        size = -1
    return size


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
        #PP I think this is actually not a requirement of CMIP6 so except for fx which has not date range
        # just using tstart-tend as they are should be sufficient and also more correct
        # so removing the year and yer-month only options
        start = f"{start.strftime('%4Y%m%d')}"
        if opts['timeshot'] in ['mean','clim']:
            fin = f"{fin.strftime('%4Y%m%d')}"
            if frequency == '6hr':
                start = f"{start}0300"
                fin = f"{fin}2100"
            elif frequency == '3hr':
                #add minutes to string
                start = f"{start}0130"
                fin = f"{fin}2230"
            #hack to fix time for variables where only a single value is used (time invariant)
            #PP how is this different from fx??
            if opts['axes_modifier'].find('firsttime') != -1:
                fin = start
        elif opts['timeshot'] == 'inst':
            # add extra day to fin
            #snapshot time bounds:
            if frequency == '10day':
                #add month to final date
                fin = fin + timedelta(months=1)
                start = f"{start.strftime('%4Y%m')}11"
                fin = f"{fin.strftime('%4Y%m')}01"
            #add day to final date
            fin = (fin + timedelta(days=1)).strftime('%4Y%m%d')
            if frequency == '6hr':
                start = f"{start}0600"
                fin = f"{fin}0000"
            elif frequency == '3hr':
                start = f"{start}0300"
                fin = f"{fin}0000"
            else:
                raise Exception('Error creating file name for snapshot data: 6hr or 3hr data expected')
        else:
            raise Exception('Error creating file name: no timeshot in champions table')
                #add time elements into one string
        opts['date_range'] = f"{start}-{fin}"
        if opts['timeshot'] == 'clim':
            opts['date_range'] += '-clim'
    # PP probably need to adapt this with python3 string??
    #P use path_template and file_template instead
    template = f"{cdict['outdir']}/{cdict['path_template']}{cdict['file_template']}"
    file_name = template.format(**opts) 
    return file_name


def populateRows(rows, cdict, opts, cursor):
    tableToFreq = read_yaml(f"input_files/table2freq.yaml")
    #CREATE TABLE champions( cmip_table cmip_variable definable text, access_variable text, file_structure text, calculation text, in_units text, axes_modifier text, positive text, timeshot text, years text, notes text, cfname text, dimension text, primary key (cmip_variable,cmip_table))
    for champ in rows:
        #defaults
        #from champions table:
        frequency = tableToFreq[cdict['cmip_table']] #cmip table
        opts['frequency'] = frequency
        opts['cmip_table'] = cdict['cmip_table']
        opts['variable_id'] = champ[1] # cmip_var
        opts['vin'] = champ[3] # access_vars
        paths = file_structure.split() 
        opts['infile'] = ''
        for x in paths:
            opts['infile'] += f"{opts['local_exp_dir']}/{x} "
        opts['calculation'] = champ[5]
        opts['in_units'] = champ[6]
        opts['positive'] = champ[8]
        opts['timeshot'] = champ[9]
        opts['years'] = champ[10]
        opts['var_notes'] = champ[11]
        opts['cfname'] = champ[12]
        dimension = champ[13]
        time = datetime.strptime(opts['exp_start'], '%Y%m%d').date()
        finish = datetime.strptime(opts['exp_end'], '%Y%m%d').date()
        cursor.execute(f"select max_file_years,gridpoints from grids where dimensions=='{dimension}' and frequency=='{frequency}'")
        #TODO add in check that there is only one value
        try:
            if opts['variable_id'] == 'co2':
                stepyears = 10
                gridpoints = 528960
            else:
                stepyears, gridpoints=cursor.fetchone()
        except:
            print("error: no grid specification for")
            print(f"frequency: {frequency}")
            print(dimension)
            print(opts['variable_id'])
            raise
        #loop over times
        print(time, finish)
        while (time <= finish):
            stepdays = stepyears*365
            if calendar.isleap(time.year):
                stepdays = stepyears*366
            newtime = min(time+timedelta(days=stepdays-1), finish)
            #newtime = min(time+stepyears-1,finish)
            #stepDays = (datetime(newtime+1,1,1)-datetime(time,1,1)).days
            opts['tstart'] = time
            opts['tend'] = newtime
            opts['file_size'] = computeFileSize(gridpoints,frequency,stepdays)
            opts['file_name'] = buildFileName(cdict, opts)
            rowid = addRow(opts, cursor)
            time = newtime + timedelta(days=stepdays)


def count_rows(conn):
    cursor=conn.cursor()
    #cursor.execute(f"select * from file_master where status=='unprocessed' and local_exp_id=='{exptoprocess}'")
    cursor.execute(f"select * from file_master")
    rows = cursor.fetchall()
    print(f"Number of rowsin file_master: {len(rows)}")
    #for row in rows:
    #    print(row)
    #P we really don't need this!!!
    # should we just print it in log??
    # database_count=f"{outdir}/database_count.txt"
    # with open(database_count,'w') as dbc:
    #    dbc.write(str(len(rows)))
    #dbc.close()

def sum_file_sizes(conn):
    cursor=conn.cursor()
    cursor.execute('select file_size from file_master')
    sizeList=cursor.fetchall()
    size=0.0
    for s in sizeList:
        size += float(s[0])
    return size/1024.


def main():
    # first read config passed by user
    config = read_yaml('custom_app.yaml')
    # then add setup_env to config
    config = setup_env(config)
    cdict = config['cmor']
    cleanup(config)
    json_cv = f"{cdict['cmip_tables']}/{cdict['_control_vocabulary_file']}"
    edit_cv_json(json_cv, config['attrs'])
    #PP do we ened this??
    #PP apparently we do need a file like that because of CMOR but maybe not all the same attributes
    #PP here I'm creating one on the fly with the attributes frm custo_app.yaml
    # then if necessary we can add edit-cv when needed for non custom runs
    #fname = create_cv_json(cdict['exp'], cdict['appdir'], config['attrs'])
    fname = f"{cdict['appdir']}/input_files/json/cm000.json"
    cdict['json_file_path'] = fname
    #cdict['json_file_path'] = json_cv
    #if cdict['mode'] == 'cmip6':
    # mapping
    cdict = dreq_map(cdict)
    #database_manager
    database = cdict['database']
    print(f"creating & using database: {database}")
    conn = sqlite3.connect(database)
    conn.text_factory = str
    #setup database tables
    master_setup(conn)
    champions_setup(conn, cdict['variable_maps'])
    populate(conn, config)
    print('past populate')
    #PP this can be totally done directly in cli.py, if it needs doing at all!
    #create_database_updater()
    count_rows(conn)
    tot_size = sum_file_sizes(conn)
    print(f"max total file size is: {tot_size} GB")
    #write app_job.sh
    config['cmor'] = write_job(cdict)
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
