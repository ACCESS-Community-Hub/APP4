# This is the ACCESS Post Processor.
#
# Originally written for CMIP5 by Peter Uhe
#
# Adapted for CMIP6 by Chloe Mackallah
# Version 4 March 2022
#

'''
Changes to script

17/03/23:
SG - Updated print statements and exceptions to work with python3.
SG- Added spaces and formatted script to read better.

20/03/23:
SG - Changed cdms2 to Xarray.

21/03/23:
PP - Changed cdtime to datetime. NB this is likely a bad way of doing this, but I need to remove cdtime to do further testing
PP - datetime assumes Gregorian calendar

18/04/23
PP - complete restructure: now cli.py with cli_functions.py include functionality of both app.py and app_wrapper.py
     to run 
     python cli.py wrapper
     I'm not yet sure if click is best used here, currently not using the args either (except for debug) but I'm leaving them in just in case
     Still using pool, app_bulk() contains most of the all app() function, however I generate many "subfunctions" mostly in cli_functions.py to avoid having a huge one. What stayed here is the cmor settings and writing
     using xarray to open all files, passing sometimes dataset sometime variables this is surely not consistent yet with app_functions
     
'''


import os,sys
import warnings
import logging
import time as timetime
import traceback
import multiprocessing as mp
import csv
import yaml
import ast
import calendar
import click
import sqlite3
import numpy as np
import xarray as xr
import cmor
from itertools import repeat
from functools import partial
from app_functions import *
from cli_functions import *
from cli_functions import _preselect 

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

#
#main function to post-process files
#
def app_catch():
    debug_logger = logging.getLogger('app_debug')
    debug_logger.setLevel(logging.CRITICAL)
    try:
        app()
    except Exception as e:
        click.echo('ERROR: %s'%e)
        debug_logger.exception(e)
        sys.exit(1)


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--infile', '-i', type=str, required=True, 
                help='Input yaml file with experiment information')
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
@click.pass_context
def app(ctx, infile, debug):
    """Wrapper setup
    """
    with open(infile, 'r') as yfile:
        cfg = yaml.safe_load(yfile)

    ctx.obj = cfg['cmor']
    ctx.obj['attrs'] = cfg['attrs']
    # set up main app4 log
    ctx.obj['log'] = config_log(debug, ctx.obj['app_logs'])
    app_log = ctx.obj['log']
    app_log.info("\nstarting app_wrapper...")

    app_log.info(f"local experiment being processed: {ctx.obj['exp']}")
    app_log.info(f"cmip6 table being processed: {ctx.obj['tables']}")
    app_log.info(f"cmip6 variable being processed: {ctx.obj['variable_to_process']}")



@app.command(name='wrapper')
@click.pass_context
def app_wrapper(ctx):
    """Main method to select and process variables
    """
    #open database    
    conn=sqlite3.connect(ctx.obj['database'], timeout=200.0)
    print(ctx.obj['database'])
    conn.text_factory = str
    cursor = conn.cursor()

    #process only one file per mp process
    cursor.execute("select *,ROWID  from file_master where " +
        f"status=='unprocessed' and local_exp_id=='{ctx.obj['exp']}'")
    #fetch rows
    try:
       rows = cursor.fetchall()
    except:
       print("no more rows to process")
    conn.commit()
    #process rows
    print(f"number of rows: {len(rows)}")
    results = pool_handler(rows, ctx.obj['ncpus'])
    print("app_wrapper finished!\n")
    #summarise what was processed:
    print("RESULTS:")
    for r in results:
        print(r)


@click.pass_context
def app_bulk(ctx, app_log):
    start_time = timetime.time()
    print("starting main app function...")
    default_cal = "gregorian"
    #
    cmor.setup(inpath=ctx.obj['tables_path'],
        netcdf_file_action = cmor.CMOR_REPLACE_4,
        set_verbosity = cmor.CMOR_NORMAL,
        exit_control = cmor.CMOR_NORMAL,
        #exit_control=cmor.CMOR_EXIT_ON_MAJOR,
        logfile = f"{ctx.obj['cmor_logs']}/log", create_subdirectories=1)
    #
    #Define the dataset.
    #
    cmor.dataset_json(ctx.obj['json_file_path'])
    #
    for k,v in ctx.obj['attrs'].items():
        cmor.set_cur_dataset_attribute(k, v)
        
    #
    #Load the CMIP tables into memory.
    #
    tables = []
    tables.append(cmor.load_table(f"{ctx.obj['tables_path']}/{ctx.obj['grids']}"))
    tables.append(cmor.load_table(f"{ctx.obj['tables_path']}/{ctx.obj['table']}.json"))
    sys.stdout.flush()
    #
    #PP This now checks that input variables are available from listed paths if not stop execution
    # if they are all avai;able re-write infile as a sequence corresponding to invars
    all_files, ctx = find_files(app_log)

    sys.stdout.flush()
    # PP FUNCTION END return all_files, extra_files
    app_log.info(f"access files from: {os.path.basename(all_files[0][0])}" +
                 f"to {os.path.basename(all_files[0][-1])}")
    app_log.info(f"first file: {all_files[0][0]}")
    #
    #PP create time_axis function
    # PP in my opinion this can be fully skipped, but as a start I will move it to a function
    ds = xr.open_dataset(all_files[0][0], decode_times=False)
    time_dimension, inref_time = get_time_dim(ds, app_log)
    print(time_dimension)
    sys.stdout.flush()
    #
    #Now find all the ACCESS files in the desired time range (and neglect files outside this range).
    # First try to do so based on timestamp on file, if this fails
    # open files and read time axis
    try:
        inrange_files = check_timestamp(all_files[0], app_log) 
    except:
        inrange_files = check_in_range(all_files[0], time_dimension, app_log) 
    sys.stdout.flush()
    #check if the requested range is covered
    if inrange_files == []:
        app_log.warning("no data exists in the requested time range")
        return 0
    # as we want to pass time as float value we don't decode time when openin files
    # if we need to decode times can be done if needed by calculation`

    # preprocessing to select only variables we need to avoid
    # concatenation issues with multiple coordinates
    preselect = partial(_preselect, varlist=ctx.obj['vin'])
    dsin = xr.open_mfdataset(inrange_files, preprocess=preselect,
                             parallel=True, decode_times=False)
    #dsin = xr.open_mfdataset(inrange_files, parallel=True, use_cftime=True)
    sys.stdout.flush()
    invar = dsin[ctx.obj['vin'][0]]
    #First try and get the units of the variable.
    #
    in_units, in_missing, positive = get_attrs(invar, app_log) 

    #PP swapped around the order: calculate first and then worry about cmor
    app_log.info("writing data, and calculating if needed...")
    app_log.info(f"calculation: {ctx.obj['calculation']}")
    sys.stdout.flush()
    #
    #PP start from standard case and add modification when possible to cover other cases 
    if 'A10dayPt' in ctx.obj['table']:
        app_log.info('ONLY 1st, 11th, 21st days to be used')
        dsin = dsin.where(dsin[time_dimension].dt.day.isin([1, 11, 21]),
                          drop=True)
    
    # Perform the calculation:
    try:
        out_var = normal_case(dsin, time_dimension, in_missing, app_log)
        app_log.info("Calculation completed!")
    except Exception as e:
        app_log.error(f"E: Unable to run calculation because: {e}")
    # Now define axis, variable etc before writing to CMOR


    #calculate time integral of the first variable (possibly adding a second variable to each time)
    # PP I removed all the extra special calculations
    # adding axis etc after calculation will need to extract cmor bit from calc_... etc
    app_log.info("defining axes...")
    # get axis of each dimension
    print(out_var)
    sys.stdout.flush()
    t_axis, z_axis, j_axis, i_axis, p_axis, e_axis= get_axis_dim(out_var, app_log)
    # should we just calculate at end??
    n_grid_pnts = 1
    cmor.set_table(tables[1])
    axis_ids = []
    if t_axis is not None:
        cmor_tName = get_cmorname('t')
        t_bounds = get_bounds(dsin, t_axis, cmor_tName, app_log)
        ctx, t_axis_val, t_bounds = get_time(t_axis, t_bounds,
            inref_time, cmor_tName, app_log)
        t_axis_id = cmor.axis(table_entry=cmor_tName,
            units=ctx.obj['reference_date'],
            length=len(t_axis),
            coord_vals=t_axis_val,
            cell_bounds=t_bounds,
            interval=None)
        axis_ids.append(t_axis_id)
    if z_axis is not None:
        cmor_zName = get_cmorname('z')
        print(cmor_zName)
        z_bounds = get_bounds(dsin, z_axis, cmor_zName, app_log)
        z_axis_id = cmor.axis(table_entry=cmor_zName,
            units=z_axis.units,
            length=len(z_axis),
            coord_vals=z_axis.values,
            cell_bounds=z_bounds,
            interval=None)
        axis_ids.append(z_axis_id)
        #set up additional hybrid coordinate information
        if cmor_zName in ['hybrid_height', 'hybrid_height_half']:
            zfactor_b_id, zfactor_orog_id = hybrid_axis(lev_name, app_log)
    if j_axis is None or i_axis.ndim == 2:
           #cmor.set_table(tables[0])
           j_axis_id = cmor.axis(table=tables[0],
               table_entry='j_index',
               units='1',
               coord_vals=np.arange(len(dim_values)))
           axis_ids.append(j_axis_id)
       #             n_grid_pts=len(dim_values)
    else:
        cmor_jName = get_cmorname('j')
        print(cmor_jName)
        j_bounds = get_bounds(dsin, j_axis, cmor_jName, app_log)
        j_axis_id = cmor.axis(table_entry=cmor_jName,
            units=j_axis.units,
            length=len(j_axis),
            coord_vals=j_axis.values,
            cell_bounds=j_bounds,
            interval=None)
        axis_ids.append(j_axis_id)
    #    n_grid_pts = n_grid_pts * len(j_axis)
    if i_axis is None or i_axis.ndim == 2:
        setgrid = True
        i_axis_id = cmor.axis(table=tables[0],
             table_entry='i_index',
             units='1',
             coord_vals=np.arange(len(i_axis)))
        axis_ids.append(i_axis_id)
    else:
        setgrid = False
        cmor_iName = get_cmorname('i')
        print(cmor_iName)
        i_bounds = get_bounds(dsin, i_axis, cmor_iName, app_log)
        i_axis_id = cmor.axis(table_entry=cmor_iName,
            units=i_axis.units,
            length=len(i_axis),
            coord_vals=np.mod(i_axis.values,360),
            cell_bounds=i_bounds,
            interval=None)
        axis_ids.append(i_axis_id)
        #n_grid_pts = n_grid_pts * len(j_axis)
    if p_axis is not None:
        cmor_pName, p_vals, p_len = pseudo_axis(p_axis) 
        p_axis_id = cmor.axis(table_entry=cmor_pName,
            units='',
            length=p_len,
            coord_vals=p_vals)
        axis_ids.append(p_axis_id)
    if e_axis is not None:
        e_axis_id = create_axis(axm, tables[1], app_log) 
        axis_ids.append(e_axis_id)
    print(axis_ids)
    sys.stdout.flush()

    #If we are on a non-cartesian grid, Define the spatial grid
    if setgrid:
        grid_id = define_grid(i_axis_id, i_axis, j_axis_id, j_axis, tables[0], app_log)
    #PP need to find a different way to make this happens
    #create oline, siline, basin axis
    #for axm in ['oline', 'siline', 'basin']:
    #    if axm in ctx.obj['axes_modifier']:
    #        axis_id = create_axis(axm, tables[1], app_log)
    #        axis_ids.append(axis_id)

    #
    #Define the CMOR variable.
    #
    app_log.info(f"cmor axis variables: {axis_ids}")
    #
    #Define the CMOR variable, taking account of possible direction information.
    #
    app_log.info("defining cmor variable...")
    try:    
        #set positive value from input variable attribute
        #PP potentially check somewhere that variable_id is in table
        cmor.set_table(tables[1])
        var_id = ctx.obj['variable_id'].replace('_','-')
        sys.stdout.flush()
        variable_id = cmor.variable(table_entry=var_id,
                    units=in_units,
                    axis_ids=axis_ids,
                    data_type='f',
                    missing_value=in_missing,
                    positive=positive)
    except Exception as e:
        app_log.error(f"E: Unable to define the CMOR variable {e}")
        raise
    app_log.info('writing...')
    # ntimes passed is optional but we might need it if time dimension is not time
    status = None
    if time_dimension != None:
        app_log.info(f"Variable shape is {out_var.shape}")
        status = cmor.write(variable_id, out_var.values,
                ntimes_passed=out_var[time_dimension].size)
    else:
        status = cmor.write(variable_id, out_var.values, ntimes_passed=0)
    if status != 0:
        app_log.error(f"E: Unable to write the CMOR variable to file\n"
                      + f"See cmor log, status: {status}")
    #
    #Close the CMOR file.
    #
    app_log.info(f"finished writing @ {timetime.time()-start_time}")
    path = cmor.close(variable_id, file_name=True)
    return path

#
#function to process set of rows in the database
#if override is true, write over files that already exist
#otherwise they will be skipped
#
#PP not sure if better passing dreq_years with context or as argument
@click.pass_context
def process_row(ctx, row):
    app_log = ctx.obj['log']
    #set version number
    #set location of cmor tables
    cmip_table_path = ctx.obj['tables_path']
    
    row['vin'] = row['vin'].split()
    # check that calculation is defined if more than one variable is passed as input
    if len(row['vin'])>1 and row['calculation'] == '':
        app_log.error("error: multiple input variables are given without a description of the calculation")
        return -1
    row['notes'] = f"Local exp ID: {row['local_exp_id']}; Variable: {row['variable_id']} ({row['vin']})"
    row['exp_description'] = ctx.obj['attrs']['exp_description']
    #
    print("\n#---------------#---------------#---------------#---------------#\nprocessing row with details:\n")
    for k,v in row.items():
        ctx.obj[k] = v
        print(f"{k}= {v}")
    #
    sys.stdout.flush()
    try:
        #Do the processing:
        #
        expected_file = row['file_name']
        successlists = ctx.obj['success_lists']
        var_msg = f"{row['table']},{row['variable_id']},{row['tstart']},{row['tend']}"
        #if file doesn't already exist (and we're not overriding), run the app
        if ctx.obj['override'] or not os.path.exists(expected_file):
            #
            #version_number = f"v{version}"
            #process the file,
            ret = app_bulk(app_log)
            try:
                os.chmod(ret,0o644)
            except:
                pass
            print("\nreturning to app_wrapper...")
            #
            #check different return codes from the APP. 
            #
            if ret == 0:
                msg = f"\ndata incomplete for variable: {row['variable_id']}\n"
                #PP temporarily commenting this
                #with open(ctx.obj['database_updater'],'a+') as dbu:
                #    dbu.write(f"setStatus('data_Unavailable',{rowid})\n")
                #dbu.close()
            elif ret == -1:
                msg = "\nreturn status from the APP shows an error\n"
                #with open(['database_updater'],'a+') as dbu:
                #    dbu.write(f"setStatus('unknown_return_code',{rowid})\n")
                #dbu.close()
            else:
                insuccesslist = 0
                with open(f"{successlists}/{ctx.obj['exp']}_success.csv",'a+') as c:
                    reader = csv.reader(c, delimiter=',')
                    for line in reader:
                        if (line[0] == row['table'] and line[1] == row['variable_id'] and
                            line[2] == row['tstart'] and line[3] == row['tend']):
                            insuccesslist = 1
                        else: 
                            pass
                    if insuccesslist == 0:
                        c.write(f"{var_msg},{ret}\n")
                        print(f"added \'{var_msg},...\'" +
                              f"to {successlists}/{ctx.obj['exp']}_success.csv")
                    else:
                        pass
                c.close()
                #Assume processing has been successful
                #Check if output file matches what we expect
                #
                print(f"output file:   {ret}")
                if ret == expected_file:
                    print(f"expected and cmor file paths match")
                    msg = f"\nsuccessfully processed variable: {var_msg}\n"
                    #modify file permissions to globally readable
                    #oos.chmod(ret, 0o493)
                    #PP temporarily commenting this
                    #with open(ctx.obj['database_updater'],'a+') as dbu:
                    #    dbu.write(f"setStatus('processed',{rowid})\n")
                    #dbu.close()
                    #plot variable
                    #try:
                    #    if plot:
                    #        plotVar(outpath,ret,table,vcmip,source_id,experiment_id)
                    #except: 
                    #    msg = f"{msg},plot_fail: "
                    #    traceback.print_exc()
                else :
                    print(f"expected file: {expected_file}")
                    print("expected and cmor file paths do not match")
                    msg = f"\nproduced but file name does not match expected {var_msg}\n"
                    #PP temporarily commenting this
                    #with open(ctx.obj['database_updater'],'a+') as dbu:
                    #    dbu.write(f"setStatus('file_mismatch',{rowid})\n")
                    #dbu.close()
        else :
            #
            #we are not processing because the file already exists.     
            #
            msg = f"\nskipping because file already exists for variable: {var_msg}\n"
            print(f"file: {expected_file}")
            #PP temporarily commenting this
            #with open(ctx.obj['database_updater'],'a+') as dbu:
            #    dbu.write(f"setStatus('processed',{rowid})\n")
            #dbu.close()
    except Exception as e: #something has gone wrong in the processing
        print(e)
        traceback.print_exc()
        infailedlist = 0
        with open(f"{successlists}/{ctx.obj['exp']}_failed.csv",'a+') as c:
            reader = csv.reader(c, delimiter=',')
            for line in reader:
                if (line[0] == row['variable_id'] and line[1] == row['table']
                    and line[2] == row['tstart'] and line[3] == row['tend']):
                    infailedlist = 1
                else:
                    pass
            if infailedlist == 0:
                c.write(f"{var_msg}\n")
                print(f"added '{var_msg}' to {successlists}/{ctx.obj['exp']}_failed.csv")
            else:
                pass
        c.close()
        msg = f"\ncould not process file for variable: {var_msg}\n"
        #PP temporarily commenting this
        #with open(ctx.obj['database_updater'],'a+') as dbu:
        #    dbu.write(f"setStatus('processing_failed',{rowid})\n")
        #dbu.close()
    print(msg)
    return msg


@click.pass_context
def process_experiment(ctx, row):
    record = {}
    header = ['infile', 'outpath', 'file_name', 'vin', 'variable_id',
              'table', 'frequency', 'realm', 'timeshot', 'tstart',
              'tend', 'status', 'file_size', 'local_exp_id',
              'calculation', 'resample', 'in_units', 'positive',
              'cfname', 'source_id', 'access_version', 'json_file_path',
              'reference_date', 'version']  
    for i,val in enumerate(header):
        record[val] = row[i]
    table = record['table'].split('_')[1]
    varlogfile = (f"{ctx.obj['var_logs']}/varlog_{table}"
                 + f"_{record['variable_id']}_{record['tstart']}-"
                 + f"{record['tend']}.txt")
    sys.stdout = open(varlogfile, 'w')
    sys.stderr = open(varlogfile, 'w')
    print(f"process: {mp.Process()}")
    t1=timetime.time()
    print(f"start time: {timetime.time()-t1}")
    print(f"processing row:")
    msg = process_row(record)
    print(f"end time: {timetime.time()-t1}")
    return msg


@click.pass_context
def pool_handler(ctx, rows, ncpus):
    p = mp.Pool(ncpus)
    #args = zip(rows, repeat(ctx.obj['var_logs']))
    #results = p.imap_unordered(process_experiment,((row) for row in rows))
    results = p.imap_unordered(process_experiment, rows)
    p.close()
    p.join()
    return results


if __name__ == "__main__":
    app()
