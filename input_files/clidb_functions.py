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
from datetime import datetime


def config_log(debug):
    """Configures log file"""
    # start a logger
    logger = logging.getLogger('db_log')
    # set a formatter to manage the output format of our handler
    formatter = logging.Formatter('%(asctime)s; %(message)s',"%Y-%m-%d %H:%M:%S")
    # set the level for the logger, has to be logging.LEVEL not a string
    # until we do so cleflog doesn't have a level and inherits the root logger level:WARNING
    level = logging.WARNING
    if debug:
        level = logging.DEBUG
    logger.setLevel(level)

    # add a handler to send WARNING level messages to console
    # or DEBUG level if debug is on
    clog = logging.StreamHandler()
    clog.setLevel(level)
    logger.addHandler(clog)

    # add a handler to send INFO level messages to file
    # the messagges will be appended to the same file
    # create a new log file every month
    day = datetime.now().strftime("%Y%m%d")
    logname = '/g/data/ua8/Working/packages/APP4/logs/dbapp4_log_' + day + '.txt'
    flog = logging.FileHandler(logname)
    try:
        os.chmod(logname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO);
    except OSError:
        pass
    flog.setLevel(logging.INFO)
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
	        dimensions TEXT,
                units TEXT,
                frequency TEXT,
                realm TEXT,
                positive TEXT,
                model TEXT,
                notes TEXT,
                origin TEXT,
                PRIMARY KEY (cmip_var, frequency, realm, model)
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
        input_vars, calculation, dimensions, units,
        frequency, realm, positive, model,
        notes, origin) values (?,?,?,?,?,?,?,?,?,?,?)"""
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
        columns = [description[0] for description in c.description]
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
    sql = f"SELECT name FROM mapping WHERE input_vars='{varname}' and calculation=''" 
    results = query(conn, sql,(), first=False)
    cmip_name = list(set(x[0] for x in results)) 
    if len(cmip_name) > 1:
        db_log.warning(f"Found more than 1 definition for {varname}:\n" +
                       f"{cmip_name}")
    elif len(cmip_name) == 0:
        cmip_name = ['']
    return cmip_name[0]


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
    dims = {} 
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
        #realm = [x for x in ['/atmos/', '/ocean/', '/ice/'] if x in fpath][0]
        realm = [x for x in ['/atm/', '/ocn/', '/ice/'] if x in fpath][0]
        realm = realm[1:-1]
        db_log.debug(realm)
        frequency = 'NA'
        if realm == 'atm':
            frequency = fbits[-1].replace(".nc","")
        elif realm == 'ocn':
            print(fbits)
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
        fcsv = open(f"{fpattern}.csv", 'w')
        fwriter = csv.writer(fcsv, delimiter=',')
        fwriter.writerow(["name", "cmip_var", "units", "dimensions",
                          "frequency", "realm", "type", "size", "nfiles",
                          "file_name", "long_name", "standard_name"])
        ds = xr.open_dataset(fpath, use_cftime=True)
        coords = [c for c in ds.coords] + ['latitude_longitude']
        for vname in ds.variables:
            if vname not in coords and all(x not in vname for x in ['_bnds','_bounds']):
                v = ds[vname]
                # try to retrieve cmip name
                cmip_var = get_cmipname(conn, vname, db_log)
                attrs = v.attrs
                line = [v.name, cmip_var, attrs.get('units', ""),
                        " ".join(v.dims), frequency, realm, v.dtype,
                        v.nbytes, nfiles, fpattern,
                        attrs.get('long_name', ""), 
                        attrs.get('standard_name', "")]
                fwriter.writerow(line)
        fcsv.close()
        db_log.info(f"Variable list for {fpattern} successfully written")
    return


def read_map_app4(fname):
    """Reads APP4 style mapping """
    # new order
    # cmip_var, input_vars, calculation, dimensions, units,frequency, realm, positive, model,
    # origin, notes
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
                version = row[7].replace('CM2', 'CM2.0')
                newrow = [row[0], row[2], row[3], '', row[4], '',
                          row[8], row[6], version, 'app4', row[9]]
                # if version both append two rows one for ESM1.5 one for CM2.0
                if version == 'both':
                    newrow[8] = 'CM2.0'
                    var_list.append(newrow)
                    newrow[8] = 'ESM1.5'
                var_list.append(newrow)
    return var_list


def read_map(fname, alias):
    """Reads complete mapping csv file and extract info necessary to create new records
       for the mapping table in access.db
    Fields from file:
    cmip_var,input_vars,calculation,dimensions,units,frequency,realm,positive,access_ver,long_name,standard_name,filename
    Fields in table:
    cmip_var,input_vars,calculation,dimensions,units,frequency,realm,positive, model, origin, notes
    """
    var_list = []
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            # if row commented skip
            if row[0][0] == "#":
                continue
            else:
                if row[10] != '':
                    notes = row[10]
                else:
                    notes = row[9]
                if alias == '':
                    alias = row[11]
                var_list.append(row[:9] + [alias, notes])
    return var_list


def write_map_template(vars_list, different, pot_vars, columns, alias, version, db_log):
    """Write mapping csv file template based on list of variables to define 
    """
    with open(f"master_{alias}.csv", 'w') as fcsv:
        fwriter = csv.writer(fcsv, delimiter=',')
        #fwriter.writerow(columns[1:])
        header = ['cmip_var', 'input_vars', 'calculation', 'units',
                  'dimensions', 'frequency', 'realm', 'positive',
                  'access_ver', 'long_name', 'standard_name', 'filename'] 
        #fwriter.writerow(columns[1:]+['dimensions', 'frequency', 'realm', 'standard_name', 'long_name', 'filename'])
        fwriter.writerow(header)
        # add to template variables that can be defined
        for var in vars_list[1:]:
            if var[1] == '':
                var[1] = var[0]
            line = [var[1], var[0], '', var[2], '', version] + var[3:]
            fwriter.writerow(line)
        # add variables which presents more than one to calculate them
        fwriter.writerow(["# these are variables that are defined with different inputs",'','','','','','','','',''])
        for var in different:
            if var[1] == '':
                var[1] = var[0]
            #line = [var[1], var[0], '', var[2], '', version] + var[3:]
            #fwriter.writerow(line)
        # add variables which can only be calculated: be careful that calculation is correct
        fwriter.writerow(["# these are variables that can be potentially calculated. Use with caution!",
                          '','','','','','','','',''])
        for var in pot_vars:
            print(var)
            line = [var[1], var[0], '', var[2], '', version] + list(var[3:])
            fwriter.writerow(line)
        fcsv.close()
