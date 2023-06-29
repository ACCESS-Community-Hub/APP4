#!/usr/bin/env python
# Copyright 2023 ARC Centre of Excellence for Climate Extremes (CLEX)
# Author: Paola Petrelli <paola.petrelli@utas.edu.au> for CLEX
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
# Crawl etccdi netcdf directories and update ETCCDI db with new files found
# contact: paola.petrelli@utas.edu.au
# last updated 19/04/2023
#  adding file pattern to mapping

import click
import sqlite3
import logging
import sys
import os
import csv
import glob
import json
import stat
import xarray as xr
import math
from datetime import datetime, date
from collections import Counter


def config_log(debug):
    """Configures log file"""
    # start a logger
    logger = logging.getLogger('db_log')
    # set a formatter to manage the output format of our handler
    formatter = logging.Formatter('%(asctime)s; %(message)s',"%Y-%m-%d %H:%M:%S")
    # set the level for the logger, has to be logging.LEVEL not a string
    level = logging.INFO
    flevel = logging.WARNING
    if debug:
        level = logging.DEBUG
        flevel = logging.DEBUG
    logger.setLevel(level)

    # add a handler to send WARNING level messages to console
    # or DEBUG level if debug is on
    clog = logging.StreamHandler()
    clog.setLevel(level)
    logger.addHandler(clog)

    # add a handler to send INFO level messages to file
    # the messagges will be appended to the same file
    # create a new log file every month
    day = date.today().strftime("%Y%m%d")
    logname = '/g/data/ua8/Working/packages/APP4/logs/dbapp4_log_' + day + '.txt'
    flog = logging.FileHandler(logname)
    try:
        os.chmod(logname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO);
    except OSError:
        pass
    flog.setLevel(flevel)
    flog.setFormatter(formatter)
    logger.addHandler(flog)
    # return the logger object
    return logger


def db_connect(db, db_log):
    """Connects to ACCESS mapping sqlite database"""
    conn = sqlite3.connect(db, timeout=10, isolation_level=None)
    if conn.total_changes == 0:
        db_log.info("Opened database successfully")
    return conn 


def mapping_sql():
    """Returns sql to define mapping table

    Returns
    -------
    sql : str
        SQL style string defining mapping table
    """
    sql = ("""CREATE TABLE IF NOT EXISTS mapping (
                cmip_var TEXT,
                input_vars TEXT,
                calculation TEXT,
                units TEXT,
	        dimensions TEXT,
                frequency TEXT,
                realm TEXT,
                cell_methods TEXT,
                positive TEXT,
                model TEXT,
                notes TEXT,
                origin TEXT,
                PRIMARY KEY (cmip_var, input_vars, frequency, realm, model)
                ) WITHOUT ROWID;""")
    return sql


def cmorvar_sql():
    """Returns sql definition of cmorvar table

    Returns
    -------  
    sql : str
        SQL style string defining cmorvar table
    """
    sql = ("""CREATE TABLE IF NOT EXISTS cmorvar (
                name TEXT PRIMARY KEY,
                frequency TEXT,
                modeling_realm TEXT,
                standard_name TEXT,
                units TEXT,
                cell_methods TEXT,
                cell_measures  TEXT,
                long_name TEXT,
                comment TEXT,
                dimensions TEXT,
                out_name TEXT,
                type TEXT,
                positive TEXT,
                valid_min TEXT,
                valid_max TEXT,
                flag_values TEXT,
                flag_meanings TEXT,
                ok_min_mean_abs TEXT,
                ok_max_mean_abs TEXT);""")
    return sql


def map_update_sql():
    """Returns sql needed to update mapping table

    Returns
    -------
    sql : str
        SQL style string updating mapping table
    """
    sql = """INSERT OR IGNORE INTO mapping (cmip_var,
        input_vars, calculation, units, dimensions,
        frequency, realm, cell_methods, positive, model,
        notes, origin) values (?,?,?,?,?,?,?,?,?,?,?,?)"""
    return sql


def cmor_update_sql():
    """Returns sql needed to update cmorvar table

    Returns
    -------
    sql : str
        SQL style string updating cmorvar table
    """
    sql = """INSERT OR IGNORE INTO cmorvar (name, frequency,
        modeling_realm, standard_name, units, cell_methods,
        cell_measures, long_name, comment, dimensions, out_name,
        type, positive, valid_min, valid_max, flag_values,
        flag_meanings, ok_min_mean_abs, ok_max_mean_abs) values 
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    return sql


def create_table(conn, sql, db_log):
    """Creates table if database is empty

    Parameters
    ----------
    conn : connection object
    sql : str
        SQL style string defining table to create
    db_log: logger obj
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Exception as e:
        db_log.error(e)
    return


def update_db(conn, table, rows_list, db_log):
    """Adds to table new variables definitions

    Parameters
    ----------
    conn : connection object
    table : str
        Name of database table to use
    rows_list : list
        List of str represneting rows to add to table
    db_log: logger obj
    """
    # insert into db
    if table == 'cmorvar':
        sql = cmor_update_sql()
    elif table == 'mapping':
        sql = map_update_sql()
    else:
        db_log.error("Provide an insert sql statement for table: {table}")
    if len(rows_list) > 0:
        db_log.info('Updating db ...')
        with conn:
            c = conn.cursor()
            db_log.debug(sql)
            c.executemany(sql, rows_list)
            c.execute('select total_changes()')
            db_log.info(f"Rows modified: {c.fetchall()[0][0]}")
    db_log.info('--- Done ---')
    return


def query(conn, sql, tup, first=True):
    """Execute generic sql query
    """
    with conn:
        c = conn.cursor()
        c.execute(sql, tup)
        if first:
            result = c.fetchone()
        else:
            result = [ x for x in c.fetchall() ]
        #columns = [description[0] for description in c.description]
        return result


def get_columns(conn, table):
    """Gets list of columns form db table
    """
    table_data = conn.execute(f'PRAGMA table_info({table});').fetchall()
    columns = [x[1] for x in table_data]
    return columns


def get_cmipname(conn, varname, db_log):
    """Queries mapping table for cmip name given variable name as output
       by the model
    """
    sql = f"SELECT cmip_var FROM mapping WHERE input_vars='{varname}' and calculation=''" 
    results = query(conn, sql,(), first=False)
    cmip_name = list(set(x[0] for x in results)) 
    if len(cmip_name) > 1:
        db_log.warning(f"Found more than 1 definition for {varname}:\n" +
                       f"{cmip_name}")
    elif len(cmip_name) == 0:
        cmip_name = ['']
    return cmip_name[0]

def cmor_table_header(name, realm, frequency):
    """
    """
    today = date.today()
    interval = {'dec': "3650.0", 'yr': "365.0", 'mon': "30.0",
                'day': "1.0", '6hr': "0.25", '3hr': "0.125",
                '1hr': "0.041667", '10min': "0.006944", 'fx': "0.0"}
    header = {
        "data_specs_version": "01.00.33",
        "cmor_version": "3.5",
        "table_id": f"Table {name}",
        "realm": realm,
        "table_date": today.strftime("%d %B %Y"),
        "missing_value": "1e20",
        "int_missing_value": "-999",
        "product": "model-output",
        "approx_interval": interval[frequency],
        "generic_levels": "",
        "mip_era": "",
        "Conventions": "CF-1.7 ACDD1.3"
    }
    return header


def write_cmor_table(var_list, name, db_log):
    """
    """
    realms = [v[2] for v in var_list]
    setr = set(realms)
    if len(setr) > 1:
        realm = Counter(realms).most_common(1)[0][0]
        db_log.info(f"More than one realms found for variables: {setr}")
        db_log.info(f"Using: {realm}")
    else:
        realm = realms[0]
    freqs = [v[1] for v in var_list]
    setf = set(freqs)
    if len(setf) > 1:
        frequency = Counter(freqs).most_common(1)[0][0]
        db_log.info(f"More than one freqs found for variables: {setf}")
        db_log.info(f"Using: {frequency}")
    else:
        frequency = freqs[0]
    header = cmor_table_header(name, realm, frequency)
    out = {"Header": header, "variable_entry": []}
    keys = ["frequency", "modeling_realm",
            "standard_name", "units",
            "cell_methods", "cell_measures",
            "long_name", "comment", "dimensions",
            "out_name", "type", "positive",
            "valid_min", "valid_max",
            "ok_min_mean_abs", "ok_max_mean_abs"] 
    var_dict = {}
    for v in var_list:
        var_dict[v[0]] = dict(zip(keys, v[1:]))
    out["variable_entry"] = var_dict
    jfile = f"CMOR_{name}.json"
    with open(jfile, 'w') as f:
        json.dump(out, f, indent=4)
    return


def delete_record(db, table, col, val, db_log):
    """This doesn't work, i just copied some code from another repo
    where I had this option to keep part of it
    """
    # connect to db
    conn = db_connect(db, db_log)

    # Set up query
    sql = f'SELECT {col} FROM {table} WHERE {col}="{val}"'
    print(sql)
    xl = query(conn, sql, ())
    print(f'Selected records in db: {xl}')
    # Delete from db
    if len(xl) > 0:
        confirm = input('Confirm deletion from database: Y/N   ')
        if confirm == 'Y':
            print('Updating db ...')
            for fname in xl:
                with conn:
                    c = conn.cursor()
                    sql = f'DELETE from file where filename="{fname}" AND location="{location}"'
                    c.execute(sql)
                    c.execute('select total_changes()')
                    db_log.info(f"Rows modified: {c.fetchall()[0][0]}")
    return


def list_files(indir, match, db_log):
    """Returns list of files matching input directory and match"""
    files = glob.glob(f"{indir}/{match}")
    db_log.debug(f"{indir}/{match}")
    return files


def build_umfrq(time_axs, ds, db_log):
    """
    """
    umfrq = {}
    int2frq = {'dec': 3652.0, 'yr': 365.0, 'mon': 30.0,
               'day': 1.0, '6hr': 0.25, '3hr': 0.125,
               '1hr': 0.041667, '10min': 0.006944}
    for t in time_axs:
        if len(ds[t]) > 1:
            interval = (ds[t][1]-ds[t][0]).values
            interval_file = (ds[t][-1] -ds[t][0]).values
            for k,v in int2frq.items():
                if math.isclose(interval, v, rel_tol=0.05):
                    umfrq[t] = k
                    break
        else:
            umfrq[t] = 'file'
    # use other time_axis info to work out frq of time axis with 1 step
    print(f"umfrq in function {umfrq}")
    for t,frq in umfrq.items():
        if frq == 'file':
           for k,v in int2frq.items():
               if math.isclose(interval_file, v, rel_tol=0.05):
                   umfrq[t] = k
                   break
    return umfrq


def get_frequency(realm, fbits, fname, ds, db_log):
    """Return frequency based on realm and filename
    For UM files checks if more than one time axis is present and if so
    returns dictionary with frquency: variable list
    """
    frequency = 'NA'
    if realm == 'atmos':
        frequency = fbits[-1].replace(".nc", "")
        time_axs = [d for d in ds.dims if 'time' in d]
        time_axs_len = set(len(ds[d]) for d in time_axs)
        if len(time_axs_len) == 1:
            umfrq = {}
        else:
            umfrq = build_umfrq(time_axs, ds, db_log)
    elif realm == 'ocean':
        # if I found scalar or monthly in any of fbits 
        if any(x in fname for x in ['scalar', 'monthly']):
            frequency = 'mon'
        elif 'daily' in fname:
            frequency = 'day'
    elif realm == 'ice':
        if '_m.' in fname:
            frequency = 'mon'
        elif '_d.' in fname:
            frequency = 'day'
    db_log.debug(f"Frequency: {frequency}")
    return frequency, umfrq


def write_varlist(conn, indir, startdate, db_log):
    """Based on model output files create a variable list and save it
       to a csv file. Main attributes needed to map output are provided
       for each variable
    """
    #PP temporarily remove .nc as ocean files sometimes have pattern.nc-datestamp
    #sdate = f"*{startdate}*.nc"
    sdate = f"*{startdate}*"
    files = list_files(indir, sdate, db_log)
    db_log.debug(f"Found files: {files}")
    patterns = []
    for fpath in files:
        # get first two items of filename <exp>_<group>
        fname = fpath.split("/")[-1]
        db_log.debug(f"Filename: {fname}")
        fbits = fname.split("_")
        # we rebuild file pattern until up to startdate
        fpattern = "_".join(fbits[:2]).split(startdate)[0]
        # adding this in case we have a mix of yyyy/yyyymn date stamps 
        # as then a user would have to pass yyyy only and would get 12 files for some of the patterns
        if fpattern in patterns:
            continue
        patterns.append(fpattern)
        pattern_list = list_files(indir, f"{fpattern}*", db_log)
        nfiles = len(pattern_list) 
        db_log.debug(f"File pattern: {fpattern}")
        fcsv = open(f"{fpattern}.csv", 'w')
        fwriter = csv.writer(fcsv, delimiter=',')
        fwriter.writerow(["name", "cmip_var", "units", "dimensions",
                          "frequency", "realm", "cell_methods", "dtype",
                          "size", "nsteps", "file_name", "long_name",
                          "standard_name"])
        # get attributes for the file variables
        try:
            realm = [x for x in ['/atmos/', '/ocean/', '/ice/'] if x in fpath][0]
        except:
            realm = [x for x in ['/atm/', '/ocn/', '/ice/'] if x in fpath][0]
        realm = realm[1:-1]
        if realm == 'atm':
            realm = 'atmos'
        db_log.debug(realm)
        ds = xr.open_dataset(fpath, decode_times=False)
        coords = [c for c in ds.coords] + ['latitude_longitude']
        frequency, umfrq = get_frequency(realm, fbits, fname, ds, db_log)
        multiple_frq = False
        if umfrq != {}:
            multiple_frq = True
        for vname in ds.variables:
            if vname not in coords and all(x not in vname for x in ['_bnds','_bounds']):
                v = ds[vname]
                # get size in bytes of grid for 1 timestep and number of timesteps
                vsize = v[0,:].nbytes
                nsteps = nfiles * v.shape[0]
                # assign specific frequency if more than one is available
                if multiple_frq:
                    if 'time' in v.dims[0]:
                        frequency = umfrq[v.dims[0]]
                    else:
                        frequency = 'NA'
                        db_log.info(f"Could not detect frequency for variable: {v}")
                # try to retrieve cmip name
                cmip_var = get_cmipname(conn, vname, db_log)
                attrs = v.attrs
                line = [v.name, cmip_var, attrs.get('units', ""),
                        " ".join(v.dims), frequency, realm, 
                        attrs.get('cell_methods', ""), 
                        v.dtype, vsize, nsteps, fpattern,
                        attrs.get('long_name', ""), 
                        attrs.get('standard_name', "")]
                fwriter.writerow(line)
        fcsv.close()
        db_log.info(f"Variable list for {fpattern} successfully written")
    return


def read_map_app4(fname):
    """Reads APP4 style mapping """
    # old order
    #cmip_var,definable,input_vars,calculation,units,axes_mod,positive,ACCESS_ver[CM2/ESM/both],realm,notes
    var_list = []
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            # if row commented skip
            if row[0][0] == "#":
                continue
            else:
                version = row[7].replace('ESM', 'ESM1.5')
                newrow = [row[0], row[2], row[3], row[4], '', '',
                          row[8], '', row[6], version, row[9], 'app4']
                # if version both append two rows one for ESM1.5 one for CM2
                if version == 'both':
                    newrow[9] = 'CM2'
                    var_list.append(newrow)
                    newrow[9] = 'ESM1.5'
                var_list.append(newrow)
    return var_list


def read_map(fname, alias):
    """Reads complete mapping csv file and extract info necessary to create new records
       for the mapping table in access.db
    Fields from file:
    cmip_var, input_vars, calculation, units, dimensions, frequency,
    realm, cell_methods, positive, access_ver, vtype, size, nsteps,
    filename, long_name, standard_name
    Fields in table:
    cmip_var, input_vars, calculation, units, dimensions, frequency,
    realm, cell_methods, positive, model, notes, origin 
    NB model and access_ver are often the same but access_ver should eventually use a CV
    """
    var_list = []
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            # if row commented skip
            if row[0][0] == "#":
                continue
            else:
                if row[15] != '':
                    notes = row[15]
                else:
                    notes = row[14]
                if alias == '':
                    alias = row[13]
                var_list.append(row[:10] + [notes, alias])
    return var_list


def parse_vars(conn, rows, db_log):
    """Returns records of variables to include in template master file,
    a list of all stash variables + frequency available in model output
    and a list of variables already defined in db
    """
    vars_list = []
    stash_vars = []
    already = []
    # get list of variables already in db
    # eventually we should be strict for the moment we might want to capture as much as possible
    #sql = f"SELECT * FROM mapping where access_ver='{access_version}'"
    sql = f"SELECT cmip_var,input_vars, frequency, realm FROM mapping"
    results = query(conn, sql,(), first=False)
    # create a list of dict of {(input_vars, realm): cmip-var} from mapping
    existing_vars = {(x[1], x[3]): x[0] for x in results}
    db_log.debug(f"Variables already in db: {existing_vars}")
    for row in rows:
        # if row commented skip
        if row[0][0] == "#" or row[0] == 'name':
            continue
        # build tuple with cmip_var,input_vars, frequency, realm
        else:
            #varid = (row[1],row[0],row[4],row[5], access_version)
            varid = (row[0],row[5])
        if varid in existing_vars.keys():
            # if row doesn't have existing cmip name add it
            if row[1] not in existing_vars.values():
                row[1] = existing_vars[varid] 
            already.append(row[1])
            db_log.debug(f"{row[1]} already defined in db")
        vars_list.append(row)
        stash_vars.append(f"{row[0]}-{row[4]}")
    db_log.info(f"Already cmip var: {[x for x in already]}")
    return vars_list, stash_vars, already


def potential_vars(conn, rows, stash_vars, db_log):
    """
    """
    pot_vars = set()
    pot_varnames = set()
    for row in rows:
        sql = f'SELECT * FROM mapping WHERE input_vars like "%{row[0]}%"'
        results = query(conn, sql,(), first=False)
        for r in results:
            # if we are calculating something
            if r[2] != '':
                allinput = r[1].split(" ")
                if all(f"{x}-{row[4]}" in stash_vars for x in allinput):
                    # add var type, size, nsteps and filename info
                    line = list(r) + row[7:11]
                    # add dimensions and frequency from the file
                    line[4] = row[3]
                    line[5] = row[4]
                    # This was checking all the input_vars dims etc so maybe not useful
                    #if line[4:6] != r[4:6]:
                    #    db_log.info(f"Mapping frequency and dims for var: {row[1]}, are different")
                    #    db_log.info(f"{r[4:6]}")
                    pot_vars.add(tuple(line))
                    pot_varnames.add(r[0])
    return pot_vars, pot_varnames


def write_map_template(vars_list, different, pot_vars, alias, version, db_log):
    """Write mapping csv file template based on list of variables to define 

    Input varlist file order:
    name, cmip_var, units, dimensions, frequency, realm, cell_methods, vtype,
    size, nsteps, filename, long_name, standard_name
    Mapping db order:
    cmip_var, input_vars, calculation, units, dimensions, frequency, realm,
    cell_methods, positive, model, notes, origin 
        for pot vars + vtype, size, nsteps, filename
    Final template order:
    cmip_var, input_vars, calculation, units, dimensions, frequency, realm,
    cell_methods, positive, access_ver, vtype, size, nsteps, filename,
    long_name, standard_name
    """
    with open(f"master_{alias}.csv", 'w') as fcsv:
        fwriter = csv.writer(fcsv, delimiter=',')
        header = ['cmip_var', 'input_vars', 'calculation', 'units',
                  'dimensions', 'frequency', 'realm', 'cell_methods',
                  'positive', 'access_ver', 'vtype', 'size', 'nsteps',
                  'filename', 'long_name', 'standard_name'] 
        fwriter.writerow(header)
        # add to template variables that can be defined
        # if cmip_var (1) empty then use original var name
        for var in vars_list[1:]:
            if var[1] == '':
                var[1] = var[0]
            # add double quotes to calculation in case it contains ","
            if "," in var[2]:
                var[2] = f'"{var[2]}"'
            #line = [var[1], var[0], ''] + var[2:7] + [ '', version] + var[7:]
            line = [var[1], var[0], None] + var[2:7] + [ None, version] + var[7:]
            fwriter.writerow(line)
        fwriter.writerow(["# these are variables that can be potentially calculated. Use with caution!",
                          '','','','','','','','','','','','','','',''])
        for var in pot_vars:
            line = list(var[:9]) 
            line = line + [version] + list(var[12:16]) + [ None, None]
            fwriter.writerow(line)
        # add variables which presents more than one to calculate them
        fwriter.writerow(["# these are variables that are defined with different inputs",
            '','','','','','','','','','','','','','',''])
        for var in different:
            fwriter.writerow([var])
        # add variables which can only be calculated: be careful that calculation is correct
        fcsv.close()
