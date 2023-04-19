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

def dbapp_catch():
    debug_logger = logging.getLogger('app_debug')
    debug_logger.setLevel(logging.CRITICAL)
    try:
        dbapp()
    except Exception as e:
        click.echo('ERROR: %s'%e)
        debug_logger.exception(e)
        sys.exit(1)

def config_log(debug):
    ''' configure log file to keep track of users queries '''
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
    """ connect to mapping sqlite db
    """
    conn = sqlite3.connect(db, timeout=10, isolation_level=None)
    if conn.total_changes == 0:
        db_log.info("Opened database successfully")
    return conn 


def variable_sql():
    """Return sql to define variable table
        :conn  connection object
    """
    sql = ("""CREATE TABLE IF NOT EXISTS variable(name TEXT PRIMARY KEY,
                cmip_var TEXT,
                definable TEXT,
                input_vars TEXT,
                calculation TEXT,
                units TEXT,
                axes_modifier TEXT,
                positive TEXT,
                access_ver TEXT,
                realm TEXT,
                notes TEXT)""")
    return sql


def cmorvar_sql():
    """Return sql definition of cmorvar table
    """
    sql = ("""CREATE TABLE IF NOT EXISTS cmorvar(
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
                ok_max_mean_abs TEXT)""")
    return sql


def map_update_sql():
    """Return sql needed to update mapping variable table
    """
    sql = """INSERT OR IGNORE INTO variable (name, cmip_var,
        definable, input_vars, calculation, units,
        axes_modifier, positive, access_ver, realm,
        notes) values (?,?,?,?,?,?,?,?,?,?,?)"""
    return sql


def cmor_update_sql():
    """Return sql needed to update cmor variable table
    """
    #sql = """INSERT OR IGNORE INTO cmorvar (name, frequency,
    #    modeling_realm, standard_name, units, cell_methods,
    #    cell_measures, long_name, comment, dimensions, out_name,
    #    type, positive, valid_min, valid_max, ok_min_mean_abs,
    #    ok_max_mean_abs) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    # Unfortunately there's one variable!!! with two extra flag fields
    sql = """INSERT OR IGNORE INTO cmorvar (name, frequency,
        modeling_realm, standard_name, units, cell_methods,
        cell_measures, long_name, comment, dimensions, out_name,
        type, positive, valid_min, valid_max, flag_values,
        flag_meanings, ok_min_mean_abs, ok_max_mean_abs) values 
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    return sql


def create_table(conn, sql, db_log):
    """ create  table if database doesn't exists or empty
        :conn  connection object
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Exception as e:
        db_log.error(e)
    return


def update_db(conn, table, rows_list, db_log):
    """Add to table new variables definitions
    """
    # insert into db
    if table == 'cmorvar':
        sql = cmor_update_sql()
    elif table == 'variable':
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
    """ generic query
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
    """Get list of columns form db table
    """
    table_data = conn.execute(f'PRAGMA table_info({table});').fetchall()
    columns = [x[1] for x in table_data]
    return columns


def get_cmipname(conn, varname, db_log):
    """Query mapping table for cmip name given variable name as output
       by the model
    """
    sql = f"SELECT name FROM variable WHERE input_vars='{varname}' and calculation=''" 
    results = query(conn, sql,(), first=False)
    cmip_name = list(set(x[0] for x in results)) 
    if len(cmip_name) > 1:
        db_log.warning(f"Found more than 1 definition for {varname}:\n" +
                       f"{cmip_name}")
    elif len(cmip_name) == 0:
        cmip_name = ['']
    return cmip_name[0]


def delete_record(db, idx, db_log):
    # connect to db
    conn = db_connect(db, db_log)
    #mn, yr, var = tuple(['%'] if not x else x for x in [mn, yr, var] ) 

    # Set up query
    for e in exp:
        for m in mod:
            fname, location = set_query(idx, prod, tstep, exp=e,model=m)
            sql = f'SELECT filename FROM file WHERE file.location="{location}"'
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
    """
    """
    files = glob.glob(f"{indir}/{match}")
    db_log.debug(f"{indir}/{match}")
    return files


def write_varlist(conn, indir, startdate, db_log):
    """Based on model output files create a variable list and save it
       to a csv file. Main attributes needed to map output are provided
       for each variable
    """
    sdate = f"*{startdate}*.nc"
    files = list_files(indir, sdate, db_log)
    db_log.debug(f"{files}")
    patterns = []
    for fpath in files:
        # get first two items of filename <exp>_<group>
        fname = fpath.split("/")[-1]
        db_log.debug(f"Filename: {fname}")
        fbits = fname.split("_")
        frequency = fbits[-1].replace(".nc","")
        db_log.debug(f"Frequency: {frequency}")
        # we rebuild file pattern until up to startdate
        fpattern = "_".join(fbits[:2]).split(startdate)[0]
        # adding this in case we have a mix of yyyy/yyyymn date stamps 
        # as then a user would have to pass yyyy only and would get 12 files for some of the patterns
        if fpattern in patterns:
            continue
        patterns.append(fpattern)
        db_log.debug(f"File pattern: {fpattern}")
        fcsv = open(f"{fpattern}.csv", 'w')
        fwriter = csv.writer(fcsv, delimiter=',')
        fwriter.writerow(["name", "cmip_name", "units", "dimensions",
                          "frequency", "long_name", "standard_name",
                          "file_name"])
        ds = xr.open_dataset(fpath, use_cftime=True)
        coords = [c for c in ds.coords] + ['latitude_longitude']
        for vname in ds.variables:
            if vname not in coords and '_bnds' not in vname:
                v = ds[vname]
                # try to retrieve cmip name
                cmip_name = get_cmipname(conn, vname, db_log)
                attrs = v.attrs
                line = [v.name, cmip_name, attrs.get('units', ""),
                        " ".join(v.dims), frequency,
                        attrs.get('long_name', ""), 
                        attrs.get('standard_name', ""), fpattern]
                fwriter.writerow(line)
        fcsv.close()
        db_log.info(f"Variable list for {fpattern} successfully written")
    return


def write_map_template(vars_list, different, columns, alias, db_log):
    """Write mapping csv file template based on list of variables to define 
    """
    with open(f"master_{alias}.csv", 'w') as fcsv:
        fwriter = csv.writer(fcsv, delimiter=',')
        #fwriter.writerow(columns[1:])
        fwriter.writerow(columns[1:]+['dimensions', 'frequency', 'standard_name', 'long_name', 'filename'])
        for var in vars_list[1:]:
            if var[1] == '':
                var[1] = var[0]
            line = [var[1],'yes',var[0],'',var[2],'','',alias] + var[3:]
            fwriter.writerow(line)
        fwriter.writerow(["# these are variables that are defined with different inputs",'','','','','','','','',''])
        for var in different:
            if var[1] == '':
                var[1] = var[0]
            line = [var[1],'yes',var[0],'',var[2],'','',alias,var[4],var[5]]
            fwriter.writerow(line)
        fcsv.close()


def db_args(f):
    """Define database APP4 click arguments
    """
    #potentially we can load vocabularies to check that arguments passed are sensible
    #vocab = load_vocabularies('CMIP5')
    vocab = {'axes_mod': ['dropX' ,'dropY', 'dropZ', 'dropTiles',
                  'monClim', 'time1', 'day2mon', 'basin', 'oline',
                  'siline', 'vegtype', 'landUse', 'topsoil', 'dropLev',
                  'switchlevs', 'surfaceLevel', 'mod2plev19',
                  'tMonOverride', 'firsttime', 'time_integral',
                  'mon2yr', 'depth100', 'yrpoint', 'monsecs',
                   'dropT', 'gridlat']}
    constraints = [
        click.option('--fname', '-f', type=str, required=True,
            help='Input file: used to update database (map/mcor), or to pass output model variables (list)'),
        click.option('--dbname', type=str, required=False,
            help='Database name if not passed default depends on subcommand: '+
                 'mapping.db for map and cmor.db for cmor'),
        click.option('--alias', '-a', type=str, required=False, default=None,
            help='Table alias to use when updating cmor var table or creating map template with list' +
                 ' to keep track of variable definition origin. If none passed uses input filename')]
    for c in reversed(constraints):
        f = c(f)
    return f


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
@click.pass_context
def dbapp(ctx, debug):
    ctx.obj={}
    # set up a default value for flow if none selected for logging
    ctx.obj['log'] = config_log(debug)


@dbapp.command(name='cmor')
@db_args
@click.pass_context
def update_cmor(ctx, dbname, fname, alias):
    """Open database and create/update populating with rows
       cmor table json file passed as input
    """
    db_log = ctx.obj['log']
    if alias is None:
        alias = fname.split("/")[-1]
        alias = alias.replace('.json', '')
    db_log.info(f"Adding {alias} to variable name to track origin")
    if dbname is None:
        dbname = 'cmor.db'
    # connect to db, this will create one if not existing
    conn = db_connect(dbname, db_log)
    # create table if not existing
    table_sql = cmorvar_sql()
    create_table(conn, table_sql, db_log)
    # get list of variables already in db
    sql = 'SELECT name FROM cmorvar'
    results = query(conn, sql,(), first=False)
    existing_vars = [x[0] for x in results]
    db_log.debug(f"Variables already in db: {existing_vars}")

    # read list of vars from file
    with open(fname, 'r') as fj:
         vardict = json.load(fj)
    row_dict = vardict['variable_entry']
    vars_list = []
    for name,row in row_dict.items():
    # alter the name so it reflects also its origin
        name = f"{name}-{alias}" 
        # check if row already exists in db and skip
        if name in existing_vars: 
            db_log.info(f"{name} already in db")
            continue
        else:
            values = [x for x in row.values()]
            # check if flag attrs present if not add them
            if 'flag_values' not in row.keys():
                values = values[:-2] + ['',''] + values[-2:]
            vars_list.append(tuple([name] + values))
    db_log.debug(f"Variables list: {vars_list}")
    # check that all tuples have len == 19
    for r in vars_list:
        if len(r) != 19:
            db_log.error(r)
            sys.exit()
    update_db(conn, 'cmorvar', vars_list, db_log)


@dbapp.command(name='template')
@db_args
@click.pass_context
def list_var(ctx, dbname, fname, alias):
    """Open database and check if variables passed as input are present in
       mapping database. Then attemot to create a template file with specific 
       mapping based on model output itself
    """
    db_log = ctx.obj['log']
    if dbname is None:
        dbname = 'mapping.db'
    if alias is None:
        alias = fname.split(".")[0]
    # connect to db, this will create one if not existing
    conn = db_connect(dbname, db_log)
    # get list of variables already in db
    sql = 'SELECT name FROM variable'
    results = query(conn, sql,(), first=False)
    existing_vars = [x[0] for x in results]
    db_log.debug(f"Variables already in db: {existing_vars}")
    # Now run query from stash name point of view to detect what can be calculated
    sql = 'SELECT input_vars FROM variable'
    results2 = query(conn, sql,(), first=False)
    stash_vars = [x[0] for x in results2]
    stash_vars = set(stash_vars)
    # read list of vars from file
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        vars_list = []
        pot_vars = []
        already = []
        for row in reader:
            # if row commented skip
            if row[0][0] == "#" or row[0] == 'name':
                continue 
            # check if row already exists in db and skip
            elif row[0] in existing_vars: 
                already.append(row[0])
                db_log.info(f"{row[0]} already defined in db")
            else:
                vars_list.append((row))
            if row[1] in stash_vars:
                sql = f'SELECT cmip_var,input_vars FROM variable WHERE input_vars like "%{row[1]}%"'
                results3 = query(conn, sql,(), first=False)
                for r in results3:
                    allinput = r[1].split(" ")
                    if all(x in stash_vars for x in allinput):
                        pot_vars.append(r[0]) 
    db_log.info(f"Missing cmip var: {[x[0] for x in vars_list]}")
    db_log.info(f"Already cmip var: {[x[0] for x in already]}")
    # at the moment we don't distiguish yet between different definitions of the variables (i.e. different frequency etc)
    db_log.info(f"Definable cmip var: {set(pot_vars)}")
    # if variable in already defined are not in definable list, than add them to separate list
    # it's possible that most of the definition exists but clearly they need to be calculated differently
    different = set(already) - set(pot_vars)# - set(already)
    if len(different) > 0:
        db_log.warning(f"Variables already defined but with different calculation: {different}")
    # prepare template
    # get names of current variable table
    columns = get_columns(conn, 'variable')
    write_map_template(vars_list, different, columns, alias, db_log)


@dbapp.command(name='map')
@db_args
@click.pass_context
def update_map(ctx, dbname, fname, alias):
    """Open database and create/update populating with rows
       mapping file passed as input
       alias is not used so far
    """
    db_log = ctx.obj['log']
    if dbname is None:
        dbname = 'mapping.db'
    # connect to db, this will create one if not existing
    conn = db_connect(dbname, db_log)
    # create table if not existing
    table_sql = variable_sql()
    create_table(conn, table_sql, db_log)
    # get list of variables already in db
    sql = 'SELECT name FROM variable'
    results = query(conn, sql,(), first=False)
    existing_vars = [x[0] for x in results]
    db_log.info(f"Variables already in db: {existing_vars}")
    # read list of vars from file
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')

        vars_list = []
        for row in reader:
            # if row commented skip
            if row[0][0] == "#":
                continue 
            # check if row already exists in db and skip
            elif row[0] in existing_vars: 
                db_log.info(f"{row[0]} already in db")
                continue
            else:
                vars_list.append(tuple([row[0]] + row))
        # check that all tuples have len == 11
        for r in vars_list:
            if len(r) != 11:
                db_log.error(r)
                sys.exit()
    update_db(conn, 'variable', vars_list, db_log)


@dbapp.command(name='varlist')
@click.option('--indir', '-i', type=str, required=True,
    help='Converted model output directory')
@click.option('--startdate', '-d', type=str, required=True,
    help='Start date of model run as YYYYMMDD')
@click.option('--dbname', type=str, required=False,
    help='Database name if not passed default to mapping.db ')
@click.pass_context
def model_vars(ctx, indir, startdate, dbname):
    """Read variables from model output
       opens one file for each kind, save variable list as csv file
       alias is not used so far
    """
    db_log = ctx.obj['log']
    if dbname is None:
        dbname = 'mapping.db'
    # connect to db, this will create one if not existing
    conn = db_connect(dbname, db_log)
    write_varlist(conn, indir, startdate, db_log)

if __name__ == "__main__":
    dbapp()
