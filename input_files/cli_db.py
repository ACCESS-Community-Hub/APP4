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
# last updated 07/05/2023

import click
import sqlite3
import logging
import sys
import csv
import json

from clidb_functions import *


def dbapp_catch():
    """
    """
    debug_logger = logging.getLogger('app_debug')
    debug_logger.setLevel(logging.CRITICAL)
    try:
        dbapp()
    except Exception as e:
        click.echo('ERROR: %s'%e)
        debug_logger.exception(e)
        sys.exit(1)


def db_args(f):
    """Define database APP4 click arguments
    """
    #potentially we can load vocabularies to check that arguments passed are sensible
    #vocab = load_vocabularies('CMIP5')
    vocab = {}
    constraints = [
        click.option('--fname', '-f', type=str, required=True,
            help='Input file: used to update db table (mapping/cmor),' +
                 'or to pass output model variables (list)'),
        click.option('--dbname', type=str, required=False, default='access.db',
            help='Database name if not passed default is access.db'),
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
    """Main group command, initialises log and context object
    """
    ctx.obj={}
    # set up a default value for flow if none selected for logging
    ctx.obj['log'] = config_log(debug)


@dbapp.command(name='cmor')
@db_args
@click.pass_context
def update_cmor(ctx, dbname, fname, alias):
    """Open/create database and create/update cmorvar table. Table is 
    populated with data passed via inout json file.


    Parameters
    ----------
    ctx : obj
        Click context object
    dbname: str
        Database name (default is access.db)
    fname: str
        Name of json input file with records to add
    alias: str
        Indicates origin of records to add, if None json filename
        base is used instead
        

    Returns
    -------
    """

    db_log = ctx.obj['log']
    if alias is None:
        alias = fname.split("/")[-1]
        alias = alias.replace('.json', '')
    db_log.info(f"Adding {alias} to variable name to track origin")
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
    return


@dbapp.command(name='template')
@db_args
@click.option('--access_version', '-v', required=True,
    type=click.Choice(['ESM1.5', 'CM2.0', 'AUS2200']), show_default=True,
    help='ACCESS version currently only CM2.0, ESM1.5, AUS2200')
@click.pass_context
def list_var(ctx, dbname, fname, alias, access_version):
    """Open database and check if variables passed as input are present in
       mapping database. Then attempt to create a template file with specific 
       mapping based on model output itself


    Parameters
    ----------
    ctx : obj
        Click context object
    dbname: str
        Database name (default is access.db)
    fname: str
        Name of csv input file with records to add
    alias: str
        Indicates origin of records to add, if None csv filename
        base is used instead
    access_version: str
        Version of ACCESS model used to generate variables


    Returns
    -------

    """
    db_log = ctx.obj['log']
    if alias is None:
        alias = fname.split(".")[0]
    # connect to db, check first if db exists or exit 

    conn = db_connect(dbname, db_log)
    # get list of variables already in db
    # eventually we should be strict for the moment we might want to capture as much as possible
    #sql = f"SELECT * FROM mapping where access_ver='{access_version}'"
    sql = f"SELECT * FROM mapping"
    results = query(conn, sql,(), first=False)
    # build tuple with cmip_var,input_vars, frequency, realm
    existing_vars = [(x[0],x[1],x[5],x[6]) for x in results]
    #db_log.debug(f"Variables already in db: {existing_vars}")
    # Now run query from stash name point of view to detect what can be calculated
    # seelcting only input_vars with more than one variable
    sql = "SELECT input_vars FROM mapping WHERE input_vars LIKE '% %'"
    results2 = query(conn, sql,(), first=False)
    stash_vars = [x[0] for x in results2]
    stash_vars = set(y for x in stash_vars for y in x.split())
    #stash_vars = set(stash_vars)
    print(stash_vars)
    # read list of vars from file
    with open(fname, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        vars_list = []
        pot_vars = []
        pot_varnames = []
        already = []
        for row in reader:
            # if row commented skip
            if row[0][0] == "#" or row[0] == 'name':
                continue 
            # build tuple with cmip_var,input_vars, frequency, realm
            else:
                #varid = (row[1],row[0],row[4],row[5], access_version)
                varid = (row[1],row[0],row[4],row[5])
            if varid in existing_vars: 
                already.append(row[0])
                db_log.info(f"{row[0]} already defined in db")
            #else:
            vars_list.append((row))
            if row[0] in stash_vars:
                print('var in stah_vars', row[0])
                sql = f'SELECT * FROM mapping WHERE input_vars like "%{row[0]}%"'
                results3 = query(conn, sql,(), first=False)
                print('query out',results3)
                for r in results3:
                    allinput = r[1].split(" ")
                    if len(allinput > 1):
                        print('allinput',allinput)
                        if all(x in stash_vars for x in allinput):
                            pot_vars.append(r) 
                            pot_varnames.append(r[1]) 
    db_log.info(f"Missing cmip var: {[x[0] for x in vars_list]}")
    db_log.info(f"Already cmip var: {[x for x in already]}")
    # at the moment we don't distiguish yet between different definitions of the variables (i.e. different frequency etc)
    db_log.info(f"Definable cmip var: {set(pot_varnames)}")
    # if variable in already defined are not in definable list, than add them to separate list
    # it's possible that most of the definition exists but clearly they need to be calculated differently
    different = set(already) - set(pot_vars)# - set(already)
    if len(different) > 0:
        db_log.warning(f"Variables already defined but with different calculation: {different}")
    # prepare template
    # get names of current variable table
    columns = get_columns(conn, 'mapping')
    write_map_template(vars_list, different, pot_vars, columns, alias, access_version, db_log)


@dbapp.command(name='map')
@db_args
@click.pass_context
def update_map(ctx, dbname, fname, alias):
    """Open database and create/update populating with rows
       mapping file passed as input
       alias indicates origin: if old style use 'app4'
    """
    db_log = ctx.obj['log']
    # connect to db, this will create one if not existing
    conn = db_connect(dbname, db_log)
    # create table if not existing
    table_sql = mapping_sql()
    create_table(conn, table_sql, db_log)
    # get list of variables already in db
    sql = 'SELECT cmip_var FROM mapping'
    results = query(conn, sql,(), first=False)
    existing_vars = [x[0] for x in results]
    db_log.info(f"Variables already in db: {existing_vars}")
    # read list of vars from file
    if alias == 'app4':
        var_list = read_map_app4(fname)
    else:
        var_list = read_map(fname)
    # update mapping table
    update_db(conn, 'mapping', var_list, db_log)
    return


@dbapp.command(name='varlist')
@click.option('--indir', '-i', type=str, required=True,
    help='Converted model output directory')
@click.option('--startdate', '-d', type=str, required=True,
    help='Start date of model run as YYYYMMDD')
@click.option('--dbname', type=str, required=False, default='access.db',
    help='Database name if not passed default to access.db ')
@click.pass_context
def model_vars(ctx, indir, startdate, dbname):
    """Read variables from model output
       opens one file for each kind, save variable list as csv file
       alias is not used so far
    """
    db_log = ctx.obj['log']
    # connect to db, this will create one if not existing
    conn = db_connect(dbname, db_log)
    write_varlist(conn, indir, startdate, db_log)

if __name__ == "__main__":
    dbapp()
