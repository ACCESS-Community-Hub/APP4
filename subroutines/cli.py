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
     I'm not yet sure if click is bets use here, currently not using the args either (except for debug) but I'm leaving them in just in case
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
import ast
import calendar
import click
import sqlite3
import numpy as np
import xarray as xr
import cmor
from app_functions import *
from cli_functions import *

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
cmorlogs = os.environ.get('CMOR_LOGS')

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


def app_args(f):
    """Define APP4 click arguments
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
        click.option('--mode', default='cmip6', show_default=True,
            type=click.Choice(['CMIP6', 'CCMI2022', 'custom']),
            help='CMIP6, CCMI2022, or custom mode'),
        click.option('--infile', '-i', type=str, 
            default='/short/p73/tph548/test_data/test_data.nc',
                    help='Input file to process', show_default=True),
        click.option('--tstart', type=int,
            help='Start time for data to process (units: years)'),
        click.option('--tend', type=int,
            help='End time for data to process (units: years)'),
        click.option('--vin', multiple=True,
            help='Name of the input variable to process'),
        click.option('--vcmip', default='tos',
            help='Name of the CMIP-5 variable', show_default=True),
        click.option('--cmip_table_path', default='./cmip-cmor-tables/Tables',
            help='Path to the directory where the CMIP tables are stored',
            show_default=True),
        click.option('--frequency', default='mon',
            help='requested frequency of variable', show_default=True),
        click.option('--cmip_table', default='CMIP6_Amon',
            help='Name of CMIP table to load', show_default=True),
#        click.option('--version_number', default='unassigned',
#            help='ACCESS version number', show_default=True),
        click.option('--in_units',
            help='Units of input variable, by default read from the units attribute'),
        click.option('--calculation', default='',
            help='Calculation deriving the data values for the cmip '+
                 'variable from the input variables', show_default=True),
        click.option('--axes_modifier', default=[''], multiple=True,
            type=click.Choice(vocab['axes_mod']),
            help='string defining commands to modify axes: possible values: \
    dropX ,dropY, dropZ, dropTiles (remove axis),\
    monClim (monthly climatological averages), time1 (time snapshots),\
    day2mon (convert time from daily to monthly,\
    basin (add axes for basins),\
    oline (add axis for ocean lines)', show_default=True),
        click.option('--positive', default='', type=click.Choice(['up', 'down']),
            help='string defining whether the variable has the positive attribute'),
        click.option('--notes', default='',
            help='notes to be inserted directly into the netcdf file metadata'),
        click.option('--json_file_path', default='./input_file/access_cmip6.json',
            help='Path to cmor json file', show_default=True),
        click.option('--timeshot', default='mean',
            type=click.Choice(['mean', 'inst', 'clim']),
            help='mean, inst, or clim - specified by the frequency column of the data request'),
        click.option('--access_version', default='CM2', type=click.Choice(['ESM', 'CM2']),
            help='ACCESS version currently only CM2 or ESM', show_default=True),
        click.option('--reference_date', default='0001', show_default=True,
            help='The internally-consistent date(year) that the experiment began'),
        click.option('--exp_description', default='cmip6 standard experiment',
            help='Description of the experiment setup', show_default=True)]
    for c in reversed(constraints):
        f = c(f)
    return f


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
@click.pass_context
def app(ctx, debug):
    """Wrapper setup
    """
    ctx.obj={}
    # set up paths
    ctx.obj['successlists'] = os.environ.get('SUCCESS_LISTS')
    ctx.obj['out_dir'] = os.environ.get('OUT_DIR')
    ctx.obj['varlogs'] = os.environ.get('VAR_LOGS')
    log_path = os.environ.get('APP_LOGS')
    # set up main app4 log
    ctx.obj['log'] = config_log(debug, log_path)
    app_log = ctx.obj['log']
    app_log.info("\nstarting app_wrapper...")

    ctx.obj['exp'] = os.environ.get('EXP_TO_PROCESS')
    ctx.obj['table'] = os.environ.get('TABLE_TO_PROCESS')
    ctx.obj['var'] = os.environ.get('VARIABLE_TO_PROCESS')
    app_log.info(f"local experiment being processed: {ctx.obj['exp']}")
    app_log.info(f"cmip6 table being processed: {ctx.obj['table']}")
    app_log.info(f"cmip6 variable being processed: {ctx.obj['var']}")
    try:
        ctx.obj['ncpus'] = int(os.environ.get('NCPUS'))
    except:
        ctx.obj['ncpus'] = 1
    ctx.obj['database_updater'] = f"{ctx.obj['out_dir']}/database_updater.py"
    if os.environ.get('MODE').lower() == 'custom':
        ctx.obj['mode'] = 'custom'
    elif os.environ.get('MODE').lower() == 'ccmi':
        ctx.obj['mode'] = 'ccmi'
    else: 
        ctx.obj['mode'] = 'cmip6'

    #options

    if os.environ.get('OVERRIDEFILES').lower() in ['true','yes']:
        ctx.obj['overRideFiles'] = True
    else:
        ctx.obj['overRideFiles'] = False
    #if os.environ.get('PLOT').lower() == 'true': plot=True
    #else: plot=False
    if os.environ.get('DREQ_YEARS').lower() == 'true':
        ctx.obj['dreq_years'] = True
    else:
        ctx.obj['dreq_years'] = False
    print(f"dreq years = {ctx.obj['dreq_years']}")



@app.command(name='wrapper')
@click.pass_context
def app_wrapper(ctx):
    """Main method to select and process variables
    """
    #open database    
    database = os.environ.get('DATABASE')
    print(database)
    if not database:
        #default database
        database = f"{ctx.obj['out_dir']}/database.db"
    conn=sqlite3.connect(database, timeout=200.0)
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
    #PP all this first part doesn't make sense to me, in the original
    # it first calls the arg parser and set a opts dictionary then call the app() function with opts as input
    # here opts and option_dictionary would be the same??
    # otherwise where's opts coming from?
    #check the options passed to the function:    
    #len0 = len(opts)
    #opts.update(option_dictionary) 
    #overwrite default parameters with new parameters
    #len1 = len(opts)
    #if(len0 != len1): 
        #new parameters don't match old ones 
    #    raise ValueError('Error: {} input parameters don\'t match valid variable names'.format(str(len1-len0)))
    #
    #cdtime.DefaultCalendar = cdtime.GregorianCalendar
    default_cal = "gregorian"
    #
    cmor.setup(inpath=ctx.obj['cmip_table_path'],
        netcdf_file_action = cmor.CMOR_REPLACE_4,
        set_verbosity = cmor.CMOR_NORMAL,
        exit_control = cmor.CMOR_NORMAL,
        #exit_control=cmor.CMOR_EXIT_ON_MAJOR,
        logfile = f"{cmorlogs}/log", create_subdirectories=1)
    #
    #Define the dataset.
    #
    cmor.dataset_json(ctx.obj['json_file_path'])
    #
    #Write a global variable called version_number which is used for CSIRO purposes.
    #
    #try:
    #    cmor.set_cur_dataset_attribute('version_number', ctx.obj['version_number'])
    #except:
    #    print("E: Unable to add a global attribute called version_number")
    #    raise Exception('E: Unable to add a global attribute called version_number')
    #
    #Write a global variable called notes which is used for CSIRO purposes.
    #
    cmor.set_cur_dataset_attribute('notes', ctx.obj['notes'])
    #except:
    #    print 'E: Unable to add a global attribute called notes'
    #    raise Exception('E: Unable to add a global attribute called notes')
    cmor.set_cur_dataset_attribute('exp_description', ctx.obj['exp_description'])
    cmor.set_cur_dataset_attribute('contact', os.environ.get('CONTACT'))
    #
    #Load the CMIP tables into memory.
    #
    tables = []
    tables.append(cmor.load_table(f"{ctx.obj['cmip_table_path']}/CMIP6_grids.json"))
    tables.append(cmor.load_table(f"{ctx.obj['cmip_table_path']}/{ctx.obj['cmip_table']}.json"))
    #
    #PP SEPARATE FUNCTION
    all_files, extra_files = find_files(app_log)

    # PP FUNCTION END return all_files, extra_files
    app_log.info(f"access files from: {os.path.basename(all_files[0])}" +
                 f"to {os.path.basename(all_files[-1])}")
    app_log.info(f"first file: {all_files[0]}")
    #
    #PP FUNCTION check var in files
    #find file with first element of input variable (vin) 
    ds = check_var_in_file(all_files, ctx.obj['vin'][0], app_log)
    #
    #PP create time_axis function
    # PP in my opinion this can be fully skipped, but as a start I will move it to a function
    time_dimension = get_time_dim(ds, app_log)
    #
    #Now find all the ACCESS files in the desired time range (and neglect files outside this range).
    #PP start function
    # this could be potentially simplified??
    inrange_files = check_in_range(all_files, time_dimension, app_log) 
    #check if the requested range is covered
    if inrange_files == []:
        app_log.warning("no data exists in the requested time range")
        return 0
    #
    #
    #Load the first ACCESS NetCDF data file, and get the required information about the dimensions and so on.
    #
    #access_file = netCDF4.Dataset(inrange_files[0], 'r')
    # using parallel require dask so temporarily leaving it out
    #dsin = xr.open_mfdataset(inrange_files, parallel=True, use_cftime=True)
    #print(inrange_files)
    #print(ctx.obj['vin'][0])
    dsin = xr.open_mfdataset(inrange_files, parallel=True, use_cftime=True)
    #print(dsin)
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
    #PP in some case s
    if 'A10dayPt' in ctx.obj['cmip_table']:
        app_log.info('ONLY 1st, 11th, 21st days to be used')
        dsin = dsin.where(dsin[time_dimension].dt.day.isin([1, 11, 21]), drop=True)
    try:
        out_var = normal_case(dsin, time_dimension, in_missing, app_log)
    except Exception as e:
        app_log.error(f"E: Unable to retrieve/calculate variable {e}")
    # Now define axis, variable etc before writing to CMOR


    #calculate time integral of the first variable (possibly adding a second variable to each time)
    # PP I removed all the extra special calculations
    # adding axis etc after calculation will need to extract cmor bit from calc_... etc
    app_log.info("defining axes...")
    # get axis of each dimension
    t_axis, z_axis, j_axis, i_axis, p_axis = get_axis_dim(outvar, app_log)
    # should we just calculate at end??
    n_grid_pnts = 1
    cmor.set_table(tables[1])
    axis_ids = []
    if t_axis is not None:
        cmor_tName = get_cmorname('t')
        t_bounds = get_bounds(t_axis, cmor_tName, app_log)
        t_axis_id = cmor.axis(table_entry=cmor_tName,
            units=axis.units,
            length=len(t_axis),
            coord_vals=t_axis.values,
            cell_bounds=t_bounds,
            interval=None)
        axis_ids.append(t_axis_id)
    if z_axis is not None:
        z_bounds = get_bounds(z_axis, cmor_name, app_log)
        t_axis_id = cmor.axis(table_entry=cmor_tName,
            units=axis.units,
            length=len(t_axis),
            coord_vals=t_axis.values,
            cell_bounds=t_bounds[:],
            interval=None)
        axis_ids.append(z_axis_id)
    if j_axis == None or i_axis.ndim == 2:
           #cmor.set_table(tables[0])
           j_axis_id = cmor.axis(table=table[0],
               table_entry='j_index',
               units='1',
               coord_vals=np.arange(len(dim_values)))
           axis_ids.append(j_axis_id)
       #             n_grid_pts=len(dim_values)
    else:
        cmor_jName = get_cmorname('j')
        j_bounds = get_bounds(j_axis, cmor_name, app_log)
        j_axis_id = cmor.axis(table_entry=cmor_jName,
            units=axis.units,
            length=len(j_axis),
            coord_vals=j_axis.values,
            cell_bounds=j_bounds[:],
            interval=None)
        axis_ids.append(j_axis_id)
    #    n_grid_pts = n_grid_pts * len(j_axis)
    if i_axis == None or i_axis.ndim == 2:
        setgrid = True
        i_axis_id = cmor.axis(table=table[0],
             table_entry='i_index',
             units='1',
             coord_vals=np.arange(len(i_axis)))
        axis_ids.append(i_axis_id)
    else:
        setgrid = False
        cmor_iName = get_cmorname('i')
        i_bounds = get_bounds(i_axis)
        i_axis_id = cmor.axis(table_entry=cmor_iName,
            units=axis.units,
            length=len(i_axis),
            coord_vals=np.mod(i_axis.values,360),
            cell_bounds=i_bounds[:],
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
    sys.stdout.flush()

    #If we are on a non-cartesian grid, Define the spatial grid
    if setgrid:
        grid_id = create_grid(i_axis_id, i_axis, j_axis_id, j_axis, tables[0], app_log)
    #create oline, siline, basin axis
    for axm in ['oline', 'siline', 'basin']:
        if axm in ctx.obj['axes_modifier']:
            axis_id = create_axis(axm, tables[1], app_log)
            axis_ids.append(axis_id)

    #set up additional hybrid coordinate information
    if cmor_zName in ['hybrid_height', 'hybrid_height_half']:
        zfactor_b_id, zfactor_orog_id = hybrid_axis(lev_name, app_log)
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
        #PP potentially we might want this defined before?? or at least check somewhere that vcmip is in table
        variable_id = cmor.variable(table_entry=ctx.obj['vcmip'],
                    units=in_units,
                    axis_ids=axis_ids,
                    data_type='f',
                    missing_value=in_missing,
                    positive=positive)
    except Exception as e:
        app_log.error(f"E: Unable to define the CMOR variable {e}")
        raise
    try:
        app_log.info('writing...')
        if time_dimension != None:
            app_log.info(f"Variable shape is {out_var.shape}")
            cmor.write(variable_id, out_var.values,
                ntimes_passed=out_var[time_dimension].size)
        else:
            cmor.write(variable_id, out_var.values, ntimes_passed=0)

    except Exception as e:
        app_log.error(f"E: Unable to write the CMOR variable to file {e}")
    #
    #Close the CMOR file.
    #
    app_log.info(f"finished writing @ {timetime.time()-start_time}")
    try:
        path = cmor.close(variable_id, file_name=True)
    except:
        app_log.error("E: We should not be here!")
        raise
    return path

#
#function to process set of rows in the database
#if overRideFiles is true, write over files that already exist
#otherwise they will be skipped
#
#PP not sure if better passing dreq_years with context or as argument
@click.pass_context
def process_row(ctx, row):
    app_log = ctx.obj['log']
    #set version number
    #date=datetime.today().strftime('%Y%m%d')
    #set location of cmor tables
    cmip_table_path = os.environ.get('CMIP_TABLES')
    #
    #First map entries from database row to variables
    #
    experiment_id = row[0]
    realization_idx = row[1]
    initialization_idx = row[2]
    physics_idx = row[3]
    forcing_idx = row[4]
    infile = row[5]
    outpath = row[6]
    file_name = row[7]
    vin = row[8].split()
    vcmip = row[9]
    table = row[10]
    cmip_table = f"CMIP6_{row[10]}"
    frequency = row[11]
    tstart = row[12]
    tend = row[13]
    status = row[14]
    file_size = row[15]
    local_exp_id = row[16]
    calculation = row[17]
    axes_modifier = row[18]
    in_units = row[19]
    positive = row[20]
    timeshot = row[21]
    # check that calculation is defined if more than one variable is passed as input
    if len(vin)>1 and calculation == '':
        app_log.error("error: multiple input variables are given without a description of the calculation")
        return -1
    try: 
        years = ast.literal_eval(row[22])
    except: 
        years = row[22]
    var_notes = row[23]
    cfname = row[24]
    activity_id = row[25]
    institution_id = row[26]
    source_id = row[27]
    grid_label = row[28]
    access_version = row[29]
    json_file_path = row[30]
    reference_date = row[31]
    version = row[32]
    rowid = row[33]
    notes = f"Local exp ID: {local_exp_id}; Variable: {vcmip} ({vin})"
    try:
        exp_description = os.environ.get('EXP_DESCRIPTION')
    except: 
        exp_description = f"Exp: {experiment_id}"
    if ctx.obj['dreq_years']:
        #PP temporarily adding this
        try:
            msg_return = ("years requested for variable are outside " +
                         f"specified period: {table}, {vcmip}, {tstart}, {tend}")
            int(years[0])
            if tstart >= years[0]:
                pass
            elif (tstart < years[0]) and (tend >= years[0]):
                tstart = years[0]
            else:
                return msg_return
            if tend <= years[-1]:
                pass
            elif (tend > years[-1]) and (tstart <= years[-1]):
                tend = years[-1]
            else:
                return msg_return
        except:
            pass
    else:
        pass
    #
    print("\n#---------------#---------------#---------------#---------------#\nprocessing row with details:\n")
    print(f"{cmip_table},{vcmip}")
    print(f"vcmip= {vcmip}")
    print(f"vin= {vin}")
    print(f"cfname= {cfname}")
    print(f"cmip_table= {cmip_table}")
    print(f"calculation= {calculation}")
    print(f"in_units= {in_units}")
    print(f"axes_modifier= {axes_modifier}")
    print(f"positive= {positive}")
    print(f"timeshot= {timeshot}")
    print(f"frequency= {frequency}")
    try:
        int(years[0])
        print(f"years= {years[0]}-{years[-1]}")
    except:
        print(f"years= {years}")
    print(f"var_notes= {var_notes}")
    print(f"local_exp_id= {local_exp_id}")
    print(f"reference_date= {reference_date}")
    print(f"tstart= {tstart}")
    print(f"tend= {tend}")
    print(f"access_version= {access_version}")
    print(f"infile= {infile}")
    print(f"outpath= {outpath}")
    print(f"activity_id= {activity_id}")
    print(f"institution_id= {institution_id}")
    print(f"source_id= {source_id}")
    print(f"experiment_id= {experiment_id}")
    print(f"grid_label= {grid_label}")
    print(f"version= {version}")
    print(f"realization_idx= {realization_idx}")
    print(f"initialization_idx= {initialization_idx}")
    print(f"physics_idx= {physics_idx}")
    print(f"forcing_idx= {forcing_idx}")
    print(f"json_file_path= {json_file_path}")
    print(f"exp_description= {exp_description}")
    print(f"expected file name= {file_name}")
    print(f"status= {status}")
    #
    try:
        #Do the processing:
        #
        expected_file = file_name
        successlists = ctx.obj['successlists']
        #if not os.path.exists(outpath):
        #    print(f"creating outpath directory: {outpath}")
        #    os.makedirs(outpath)
        if ctx.obj['overRideFiles'] or not os.path.exists(expected_file):
            #if file doesn't already exist (and we're not overriding), run the app
            #
            #version_number = f"v{version}"
            local_vars = locals()
            for x in ['infile', 'tstart', 'tend', 'vin', 'vcmip', 'cmip_table_path', 'frequency',
                'cmip_table', 'in_units', 'calculation', 'axes_modifier', 'positive', 'notes',
                'json_file_path', 'timeshot', 'access_version', 'reference_date', 'exp_description']:
                ctx.obj[x] = local_vars[x]
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
                msg = f"\ndata incomplete for variable: {vcmip}\n"
                with open(ctx.obj['database_updater'],'a+') as dbu:
                    dbu.write(f"setStatus('data_Unavailable',{rowid})\n")
                dbu.close()
            elif ret == -1:
                msg = "\nreturn status from the APP shows an error\n"
                with open(['database_updater'],'a+') as dbu:
                    dbu.write(f"setStatus('unknown_return_code',{rowid})\n")
                dbu.close()
            else:
                insuccesslist = 0
                with open(f"{successlists}/{ctx.obj['exp']}_success.csv",'a+') as c:
                    reader = csv.reader(c, delimiter=',')
                    for row in reader:
                        if (row[0] == table and row[1] == vcmip and
                            row[2] == tstart and row[3] == tend):
                            insuccesslist = 1
                        else: 
                            pass
                    if insuccesslist == 0:
                        c.write(f"{table},{vcmip},{tstart},{tend},{ret}\n")
                        print(f"added \'{table},{vcmip},{tstart},{tend},...\'" +
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
                    msg = f"\nsuccessfully processed variable: {table},{vcmip},{tstart},{tend}\n"
                    #modify file permissions to globally readable
                    #os.chmod(ret, 0o493)
                    with open(ctx.obj['database_updater'],'a+') as dbu:
                        dbu.write(f"setStatus('processed',{rowid})\n")
                    dbu.close()
                    #plot variable
                    #try:
                    #    if plot:
                    #        plotVar(outpath,ret,cmip_table,vcmip,source_id,experiment_id)
                    #except: 
                    #    msg = f"{msg},plot_fail: "
                    #    traceback.print_exc()
                else :
                    print("expected file: {expected_file}")
                    print("expected and cmor file paths do not match")
                    msg = f"\nproduced but file name does not match expected: {table},{vcmip},{tstart},{tend}\n"
                    with open(ctx.obj['database_updater'],'a+') as dbu:
                        dbu.write(f"setStatus('file_mismatch',{rowid})\n")
                    dbu.close()
        else :
            #
            #we are not processing because the file already exists.     
            #
            msg = f"\nskipping because file already exists for variable: {table},{vcmip},{tstart},{tend}\n"
            print(f"file: {expected_file}")
            with open(ctx.obj['database_updater'],'a+') as dbu:
                dbu.write(f"setStatus('processed',{rowid})\n")
            dbu.close()
    except Exception as e: #something has gone wrong in the processing
        print(e)
        traceback.print_exc()
        infailedlist = 0
        with open(f"{successlists}/{ctx.obj['exp']}_failed.csv",'a+') as c:
            reader = csv.reader(c, delimiter=',')
            for row in reader:
                if row[0] == vcmip and row[1] == table and row[2] == tstart and row[3] == tend:
                    infailedlist = 1
                else:
                    pass
            if infailedlist == 0:
                c.write(f"{table},{vcmip},{tstart},{tend}\n")
                print(f"added '{table},{vcmip},{tstart},{tend}' to {successlists}/{ctx.obj['exp']}_failed.csv")
            else:
                pass
        c.close()
        msg = f"\ncould not process file for variable: {table},{vcmip},{tstart},{tend}\n"
        with open(ctx.obj['database_updater'],'a+') as dbu:
            dbu.write(f"setStatus('processing_failed',{rowid})\n")
        dbu.close()
    print(msg)
    return msg


@click.pass_context
def process_experiment(ctx, row):
    varlogfile = f"{ctx.obj['varlogs']}/varlog_{row[10]}_{row[9]}_{row[12]}-{row[13]}.txt"
    sys.stdout = open(varlogfile, 'w')
    sys.stderr = open(varlogfile, 'w')
    print(f"process: {mp.Process()}")
    t1=timetime.time()
    print(f"start time: {timetime.time()-t1}")
    print(f"processing row:")
    print(row)
    msg = process_row(row)
    print(f"end time: {timetime.time()-t1}")
    return msg


def pool_handler(rows, ncpus):
    p = mp.Pool(ncpus)
    results = p.imap_unordered(process_experiment,((row) for row in rows))
    p.close()
    p.join()
    return results


if __name__ == "__main__":
    app()
