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
'''



from optparse import OptionParser
import netCDF4
import numpy as np
import string
import glob
from datetime import datetime
import re
from app_functions import *
from cli2_functions import *
import os,sys
#import cdms2
import xarray as xr
import cmor
import warnings
import time as timetime
import psutil
import calendar
import click
import logging

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
@app_args
@click.pass_context
def app(ctx, debug, mode, infile, tstart, tend, vin, vcmip, cmip_table_path, frequency,
        cmip_table, in_units, calculation, axes_modifier, positive, notes,
        json_file_path, timeshot, access_version, reference_date, exp_description):
    # To go to config yaml file: cmip_table_path, exp_description, json_file_path,
    # and possibly: access_version

    ctx.obj={}
    # set up a default value for flow if none selected for logging
    if flow is None: flow = 'default'
    ctx.obj['log'] = config_log(debug)
    # check that calculation is defined if more than one variable is passed as input
    if len(vin)>1 and calculation == '':
        app_log.error("error: multiple input variables are given without a description of the calculation")
        return -1
    # PP set up options dictionary in context (this is temporary)
    # eventually we want to create a class for each variable, hence file paths and exp info etc should be hold elsewhere
    for x in ['mode', 'infile', 'tstart', 'tend', 'vin', 'vcmip', 'cmip_table_path', 'frequency',
        'cmip_table', 'in_units', 'calculation', 'axes_modifier', 'positive', 'notes',
        'json_file_path', 'timeshot', 'access_version', 'reference_date', 'exp_description']:
        ctx.obj[x] = locals(x)

    app_bulk()


@click.pass_context
def app_bulk():
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
    all_files, extra_files = find_files(ctx, app_log)

    # PP FUNCTION END return all_files, extra_files
    app_log.info(f"access files from: {os.path.basename(all_files[0])}" +
                 f"to {os.path.basename(all_files[-1])}")
    app_log.info(f"first file: {all_files[0]}")
    #
    #PP FUNCTION check var in files
    #find file with first element of inpiut variable (vin) 
    temp_file = check_var_in_file(all_files, varname, app_log)
    #
    #PP create time_axis function
    # PP in my opinion this can be fully skipped, but as a start I will move it to a function
    #time_dimension, opts = get_time_axis(temp_file, opts, app_log)
    #
    #Now find all the ACCESS files in the desired time range (and neglect files outside this range).
    #PP start function
    # this could be potentially simplified??
    inrange_files = check_in_range(all_files, time_dimension, ctx, app_log) 
    #check if the requested range is covered
    if inrange_files == []:
        app_log.warning("no data exists in the requested time range")
        return 0
    #
    #
    #Load the first ACCESS NetCDF data file, and get the required information about the dimensions and so on.
    #
    #access_file = netCDF4.Dataset(inrange_files[0], 'r')
    dsin = xr.open_mfdataset(inrange_files, 'r', parallel=True, use_cftime=True)
    invar = dsin[ctx.obj['vin'][0]]
    (axis_ids, z_ids, i_axis_id, j_axis_id, time_axis_id, n_grid_pts,
        lev_name, z_len, dim_values, dim_vals, dim_val_bounds) = new_axis_dim(invar, app_log)


    #app_log.info("opened input netCDF file: {inrange_files[0]}")
    #PP load all files with xarray and grab time axis lat lon from it
    #app_log.info("checking axes...")
    sys.stdout.flush()
    try:
        #
        #Determine which axes are X and Y, and what the values of the latitudes and longitudes are.
        #PP possibly redundant but moving to function
        #data_vals, lon_name, lat_name, lon_vals, lat_vals = check_axis(access_file, inrange_files, app_log)
        #
        #PP again might be redundant but moving it to function
        #Work out which dimension(s) are associated with each coordinate variable.
        #PP not yet sure what to return here!
        # j_axis_id, i_axis_id etc are returned by cmor.axis
        (axis_ids, z_ids, i_axis_id, j_axis_id, time_axis_id, n_grid_pts, 
                lev_name, z_len, dim_values, dim_vals, dim_val_bounds) = axis_dim(data_vals, app_log)
        #
        #PP move to function
        #If we are on a non-cartesian grid, Define the spatial grid
        #
        grid_id, axis_ids, z_ids = create_grid(i_axis_id, i_axis,
                                 j_axis_id, j_axis, tables[0], app_log)
    except Exception as e:
        app_log.error(f"E: We should not be here! {e}")
    #
    #create oline, siline, basin axis
    for axm in ['oline', 'siline', 'basin']:
        if axm in ctx.obj['axes_modifier']:
            axis_id = create_axis(axm, tables[1], app_log)
            axis_ids.append(axis_id)

    #set up additional hybrid coordinate information
    if lev_name in ['hybrid_height', 'hybrid_height_half']:
        zfactor_b_id, zfactor_orog_id = hybrid_axis(lev_name, app_log)
    #
    #Define the CMOR variable.
    #
    cmor.set_table(tables[1])
    #
    #First try and get the units of the variable.
    #
    in_units, in_missing, positive = get_attrs(invar, app_log) 
    app_log.info(f"cmor axis variables: {axis_ids}")
    #
    #Define the CMOR variable, taking account of possible direction information.
    #
    app_log.info("defining cmor variable...")
    try:    
        #set positive value from input variable attribute
        variable_id = cmor_var(app_log, positive=positive)
    except Exception as e:
        app_log.error(f"E: Unable to define the CMOR variable {e}")
        raise
    #
    #Close the ACCESS file.
    #
    #access_file.close()
    #app_log.info("closed input netCDF file")    
    #Loop over all the in time range ACCESS files, and process those which we need to.
    #
    app_log.info("writing data, and calculating if needed...")
    app_log.info(f"calculation: {ctx.obj['calculation']}")
    sys.stdout.flush()
    #
    #PP this is were some of the calculation starts I think we shpuld reverse the order, calculate first and then define axis based on results
    #calculate time integral of the first variable (possibly adding a second variable to each time)
    #
    if 'time_integral' in ctx.obj['axes_modifier']:
        axm_t_integral(invar, dsin, variable_id, app_log)
    #
    #Monthly Climatology case
    #
    elif ctx.obj['timeshot'].find('clim') != -1:
        axm_timeshot(dsin, variable_id, app_log)
    #
    #Annual means - Oyr / Eyr tables
    #
    elif 'mon2yr' in ctx.obj['axes_modifier']:
        axm_mon2yr(dsin, variable_id, app_log)
    #
    #Annual point values - landUse variables
    #
    elif 'yrpoint' in ctx.obj['axes_modifier']:
        #PP we should be able to just write them as they are, I believe the assumption here is that
        # the frequency in the file is already yearly, just slecting time between start and end date
        dsinsel = dsin.sel(time_dimension=slice(startyear, endyear)).sel(time_dimension.dt.month==12)
        #PP then it seems to select only Dec?? Does this means actually orignal frequnecy is monthly?
        if ctx.obj['calculation'] != '':
            var = calculateVals(dsinsel, ctx.obj['vin'], ctx.obj['calculation'])
            try:
                data_vals = var.values()
                data_vals = data_vals.filled(in_missing)
            except:
                #if values aren't in a masked array
                pass 
                app_log(f"shape: {np.shape(data_vals)}")
                app_log(f"time index: {index}, date: {d}")
        app_log.info("writing with cmor...")
        app_log.info(np.shape(data_vals))
        cmor.write(variable_id, data_vals[0,:,:,:], ntimes_passed=1)
    #
    #Aday10Pt processing for CCMI2022
    #
    elif ctx.obj['cmip_table'].find('A10dayPt') != -1:
        calc_a10daypt(dsin, time_dimension, variable_id, app_log)

    #
    #Convert monthly integral to rate (e.g. K to K s-1, as in tntrl)
    #
    elif 'monsecs' in ctx.obj['axes_modifier']:
        calc_monsecs(dsin, time_dimension, variable_id, app_log)
    #
    #normal case
    #
    else:
        try:
            normal_case(dsin, time_dimension, variable_id, app_log)
            app_log.info(f"finished writing @ {timetime.time()-start_time}")
        except Exception as e:
            app_log.error(f"E: Unable to write the CMOR variable to file {e}")
            raise
    #
    #Close the CMOR file.
    #
    try:
        path = cmor.close(variable_id, file_name=True)
    except:
        app_log.error("E: We should not be here!")
        raise
    return path


if __name__ == "__main__":
    app()

