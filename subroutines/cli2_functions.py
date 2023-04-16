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
import datetime
import re
from app_functions import *
import os,sys
import stat
#import cdms2
import xarray as xr
import cmor
import warnings
import time as timetime
import psutil
import calendar
import click
import logging


def config_log(debug, path):
    ''' configure log file to keep track of users queries '''
    # start a logger
    logger = logging.getLogger('app_log')
    # set a formatter to manage the output format of our handler
    formatter = logging.Formatter('%(asctime)s; %(message)s',"%Y-%m-%d %H:%M:%S")
    # set the level for the logger, has to be logging.LEVEL not a string
    # until we do so cleflog doesn't have a level and inherits the root logger level:WARNING
    logger.setLevel(logging.INFO)

    # add a handler to send WARNING level messages to console
    clog = logging.StreamHandler()
    clog.setLevel(logging.WARNING)
    logger.addHandler(clog)

    # add a handler to send INFO level messages to file
    # the messagges will be appended to the same file
    # create a new log file every month
    logname = f"{path}/app4_log.txt"
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


@click.pass_context
def find_files(ctx, app_log):
    """Find all the ACCESS file names which match the "glob" pattern.
    Sort the filenames, assuming that the sorted filenames will
    be in chronological order because there is usually some sort of date
    and/or time information in the filename.
    """
    
    app_log.info(f"input file structure: {ctx.obj['infile']}")
    app_log.info(ctx.obj['cmip_table'])
    tmp = ctx.obj['infile'].split()
    file_touse = tmp[0]
    #if there are two different files used make a list of extra access files
    if len(tmp)>1:
        extra_files = glob.glob(tmp[1])
        extra_files.sort()
    else:
        extra_files = None
    #set normal set of files
    all_files = glob.glob(file_touse)
    all_files.sort()
    #hack to remove files not in time range
    tmp = []
    for fn in all_files:
        if os.path.basename(fn).startswith('ice'):
            #tstamp=int(os.path.basename(fn).split('.')[1][0:3])
            tstamp = int(re.search("\d{4}",os.path.basename(fn)).group())
        elif os.path.basename(fn).startswith('ocean'):
            #tstamp=int(os.path.basename(fn).split('.')[1][3:6])
            tstamp = int(re.search("\d{4}",os.path.basename(fn)).group())
        else:
            if ctx.obj['access_version'].find('CM2') != -1:
                tstamp = int(os.path.basename(fn).split('.')[1][2:6])
            elif ctx.obj['access_version'].find('ESM') != -1:
                tstamp = int(os.path.basename(fn).split('.')[1][3:7])
            else:
                raise Exception('E: ACCESS_version not identified')
        if ctx.obj['tstart'] <= tstamp and tstamp <= ctx.obj['tend']:
            tmp.append(fn)
    all_files = tmp
    if extra_files != None:
        tmp = []
        for fn in extra_files:
            tstamp = int(re.search("\d{4}",fn).group())
            if ctx.obj['tstart'] <= tstamp and tstamp <= ctx.obj['tend']:
                tmp.append(fn)
        extra_files = tmp
    return all_files, extra_files


def check_var_in_file(all_files, varname, app_log):
    """Find file with first element of 'vin'
    """
    i = 0
    found = False
    while i < len(all_files):
        try:
            ds = xr.open_dataset(all_files[i], use_cftime=True)
            #see if the variable is in the file
            var = ds[varname]
            found = True
            break
        except:
            #try next file
            del ds
            i+=1
            continue
    if found:
        app_log.info(f"using file: {all_files[i]}")
    else:
        raise Exception(f"Error! Variable missing from files: {varname}")
    try:
        app_log.info(f"variable '{varname}' has units in ACCESS file: {var.units}")
    except:
        app_log.info(f"variable '{varname}' has no units listed in ACCESS file")
    return ds 


@click.pass_context
def get_time_axis(ctx, ds, app_log):
    """Find time info: time axis, reference time and set tstart and tend
    """
    ##PP changed most of this as it doesn't make sense setting time_dimension to None and then trying to access variable None in file
    time_dimension = None
    varname = [ctx.obj['vin'][0]]
    #    
    try:
        if  ('dropT' in ctx.obj['axes_modifier']) and ('fx' in ctx.obj['cmip_table']):
            #try to find and set the correct time axis:
            app_log.debug(f" check time var dims: {ds[varname].dims}")
            for var_dim in ds[varname].dims:
                if 'time' in var_dim or ds[var_dim].axis == 'T':
                    time_dimension = var_dim
                    app_log.debug(f"first attempt to tdim: {time_dimension}")
    except:
        #use the default time dimension 'time'
        pass 
    if 'tMonOverride' in ctx.obj['axes_modifier']:
        #if we want to override dodgey units in the input files
        print("overriding time axis...")
        refString = "days since {r:04d}-01-01".format(r=ctx.obj['reference_date'])
        #time_dimension=None    
        inrange_files = all_files
        startyear = ctx.obj['tstart']
        endyear = ctx.obj['tend']
    elif 'fx' in ctx.obj['cmip_table']:
        print("fx variable, no time axis")
        refString = f"days since {ctx.obj['reference_date'][:4]}-01-01"
        time_dimension = None    
        inrange_files = all_files
    else:
        try:
            # changing this behaviour!
            app_log.debug(f" check time var dims: {ds[varname].dims}")
            for var_dim in ds[varname].dims:
                if 'time' in var_dim or ds[var_dim].axis == 'T':
                    time_dimension = var_dim
                    app_log.debug(f"first attempt to tdim: {time_dimension}")
            #
            #refdate is the "reference date" used as an arbitray 0 point for dates. Common values for
            #refdate are 1 (0001-01-01) and 719 163 (1970-01-01), but other values are possible.
            #We cannot handle negative reference dates (ie. BC dates); maybe we will encounter these
            #in some climate runs?
            #
            app_log.info(f"time var is: {time_dimension}")
        except:
            pass
        del ds 
    return time_dimension



@click.pass_context
def check_in_range(ctx, all_files, tdim, app_log):
    """
    """
    inrange_files = []
    app_log.info("loading files...")
    app_log.info(f"time dimension: {tdim}")
    sys.stdout.flush()
    #if we are using a time invariant parameter, just use a file with vin
    if ctx.obj['cmip_table'].find('fx') != -1:
        inrange_files = [all_files[0]]
    else:
        for input_file in all_files:
            #try:
                ds = xr.open_dataset(input_file, use_cftime=True)
                # get first and last values as date string
                tmin = ds[tdim][0].dt.strftime('%Y%m%d')
                tmax = ds[tdim][-1].dt.strftime('%Y%m%d')
                app_log.debug(f"tmax from time dim: {tmax}")
                app_log.debug(f"tend from opts: {ctx.obj['tend']}")
                if int(tmin) > ctx.obj['tend'] or int(tmax) < ctx.obj['tstart']:
                    inrange_files.append(input_file)
                del ds
            #except Exception as e:
            #    app_log.error(f"Cannot open file: {e}")
    app_log.info(f"number of files in time range: {inrange_files}")
    print(inrange_files)
    sys.stdout.flush()
    return inrange_files

 
@click.pass_context
def check_axis(ctx, fobj, inrange_files, ancil_path, app_log):
    """
    """
    try:
        data_vals = fobj[ctx.obj['vin'][0]]
        print("shape of data: {np.shape(data_vals)}")
    except Exception as e:
        print("E: Unable to read {ctx.obj['vin'][0]} from ACCESS file")
        raise
    try:
        coord_vals = string.split(data_vals.coordinates)
        coord_vals.extend(data_vals.dimensions)
    except:
        coord_vals = data_vals.dimensions
    lon_vals = None
    lat_vals = None
    lon_name = None
    lat_name = None
    #search for strings 'lat' or 'lon' in coordinate names
    app_log.info(coord_vals)
    for coord in coord_vals:
        if coord.lower().find('lon') != -1:
            print(coord)
            lon_name = coord
            try:
                lon_vals = fobj[coord]
            except:
                if os.path.basename(inrange_files[0]).startswith('ocean'):
                    if ctx.obj['access_version'] == 'OM2-025':
                        acnfile = ancil_path+'grid_spec.auscom.20150514.nc'
                        acndata = netCDF4.Dataset(acnfile,'r')
                        lon_vals = acndata.variables['geolon_t']
                    else:
                        acnfile = ancil_path+'grid_spec.auscom.20110618.nc'
                        acndata = netCDF4.Dataset(acnfile,'r')
                        lon_vals = acndata.variables['x_T']
                if os.path.basename(inrange_files[0]).startswith('ice'):
                    if ctx.obj['access_version'] == 'OM2-025':
                        acnfile = ancil_path+'cice_grid_20150514.nc'
                    else:
                        acnfile = ancil_path+'cice_grid_20101208.nc'
                    acndata = netCDF4.Dataset(acnfile,'r')
                    lon_vals = acndata.variables[coord]
        elif coord.lower().find('lat') != -1:
            app_log.info(coord)
            lat_name = coord
            try:
                lat_vals = fobj[coord]
                app_log.info('lat from file')
            except:
                app_log.info('lat from ancil')
                if os.path.basename(inrange_files[0]).startswith('ocean'):
                    if ctx.obj['access_version'] == 'OM2-025':
                        acnfile = ancil_path+'grid_spec.auscom.20150514.nc'
                        acndata = netCDF4.Dataset(acnfile,'r')
                        lat_vals = acndata.variables['geolat_t']
                    else:
                        acnfile = ancil_path+'grid_spec.auscom.20110618.nc'
                        acndata = netCDF4.Dataset(acnfile,'r')
                        lat_vals = acndata.variables['y_T']
                if os.path.basename(inrange_files[0]).startswith('ice'):
                    if ctx.obj['access_version'] == 'OM2-025':
                        acnfile = ancil_path+'cice_grid_20150514.nc'
                    else:
                        acnfile = ancil_path+'cice_grid_20101208.nc'
                    acndata = netCDF4.Dataset(acnfile,'r')
                    lat_vals = acndata.variables[coord]
    return data_vals, lon_name, lat_name, lon_vals, lat_vals


@click.pass_context
def axis_dim(ctx, fobj, data_vals, app_log):
    """
    """
    #create a list of dimensions
    dim_list = data_vals.dimensions
    #
    #
    axis_ids = []
    z_ids = []
    time_axis_id = None
    z_axis_id = None    
    j_axis_id = None
    i_axis_id = None    
    n_grid_pts = 1
    lev_name = None
    z_len = 0
    app_log("list of dimensions: {dim_list}")
    for dim in dim_list:
        app_log.info(axis_ids)
        try:
            dim_vals = fobj[dim]
            dim_values = dim_vals[:]
        except:
            #
            #The coordinate variable associated with "dim" was not found.
            #
            app_log.info("W: No coordinate variable associated with the dimension {dim}")
            dim_vals = None
            #This should work. Look out for 'if dim_vals=None' and other use of dim_values, e.g. dim_values[:]
            dim_values = fobj.dimensions[dim]
        #
        #See if this dimension represents a spatial or temporal axis (X, Y, T and so on)
        #This information will either be stored in the axis or cartesian_axis variable attribute.
        #
        #PP This is nearly something you wan tto have resolved before, at the worst get user to input this info???
        try:
            axis_name = dim_vals.axis
        except:
            try:
                axis_name = dim_vals.cartesian_axis
            except:
                #
                #Try and guess the axis name from the dimension name.
                #
                if dim == 'ni':
                    axis_name = 'X'
                elif dim == 'nj':
                    axis_name = 'Y'
                elif (dim == time_dimension) or ('time' in dim):
                    axis_name = 'T'
                else:
                    axis_name='unknown'
        app_log.info(f"evaluating axis: {axis_name}")
        try:
            #
            #Try and get the dimension bounds. The bounds attribute might also be called "edges"
            #If we cannot find any dimension bounds, create default bounds in a sensible way.
            #
            dim_val_bounds = fobj[dim_vals.bounds]
            app_log.info("using dimension bounds")
        except:
            try:
                dim_val_bounds = fobj[dim_vals.edges]
                app_log.info("using dimension edges as bounds")
            except:
                #
                #Unable to get the dimension bounds, create some default bounds.
                #The default bounds assume that the grid cells are centred on
                #each grid point specified by the coordinate variable.
                #
                app_log.info("I: No bounds for {dim} - creating default bounds")
                if dim_vals == None:
                    app_log.info("No dimension values")
                else:
                    try:
                        min_vals = np.append((1.5*dim_values[0] - 0.5*dim_values[1]),(dim_values[0:-1] + dim_values[1:])/2)
                        max_vals = np.append((dim_values[0:-1] + dim_values[1:])/2,(1.5*dim_values[-1] - 0.5*dim_values[-2]))
                    except Exception as e:
                        app_log.warning(f"dodgy bounds for dimension: {dim}")
                        app_log.error(f"error: {e}")
                        min_vals = dim_values[:] - 15
                        max_vals = dim_values[:] + 15
                    dim_val_bounds = np.column_stack((min_vals,max_vals))
        app_log.info("handling different axes types")
        try:
            if ((axis_name == 'T') and ('dropT' in ctx.obj['axes_modifier']) 
                    and (ctx.obj['cmip_table'].find('fx') == -1)):
                #
                #Set cmor time variable name: 
                #For mean values this is just 'time'
                #for synoptic snapshots, this is 'time1'
                #for climatoloies this is time2
                #
                #PP I think matching with dictioanries it's a safer option
                app_log.info(f"dimension: {dim}")
                if ctx.obj['timeshot'].find('mean') != -1:
                    cmor_tName = 'time'
                elif ctx.obj['timeshot'].find('inst') != -1:
                    cmor_tName = 'time1'
                elif ctx.obj['timeshot'].find('clim') != -1:
                    cmor_tName = 'time2'
                else:
                    #assume timeshot is mean
                    app_log.warning("timeshot unknown or incorrectly specified")
                    cmor_tName = 'time'
                #initialise stuff:
                min_tvals = []
                max_tvals = []
                if 'tMonOverride' in ctx.obj['axes_modifier']:
                    #convert times to days since reference_date
                    # PP temporarily comment as I'm not sure what this is for
                    #tvals = np.array(inrange_times) + cdtime.reltime(0,refString).torel('days since {r:04d}-01-01'.format(r=opts['reference_date']),cdtime.DefaultCalendar).value
                    app_log.info(f"time values converted to days since 01,01,{ctx.obj['reference_date']:04d}: {tvals[0:5]}...{tvals[-5:-1]}")
                    if ctx.obj['cmip_table'].find('A10day') != -1:
                        app_log.info('Aday10: selecting 1st, 11th, 21st days')
                        a10_tvals = []
                        for a10 in tvals:
                            #a10_comp = cdtime.reltime(a10,'days since {r:04d}-01-01'.format(r=opts['reference_date'])).tocomp(cdtime.DefaultCalendar)
                            a10_comp = a10.date()
                            #print(a10, a10_comp, a10_comp.day)
                            if a10_comp.day in [1,11,21]:
                                a10_tvals.append(a10)
                        tvals = a10_tvals
                else:
                    app_log.info("manually create time axis")
                    tvals = []
                    if ctx.obj['frequency'] == 'yr':
                        app_log.info("yearly")
                        for year in range(ctx.obj['tstart'], ctx.obj['tend']+1):
                            #tvals.append(cdtime.comptime(year, 7, 2, 12).torel(refString,cdtime.DefaultCalendar).value)
                            tvals.append((datetime.datetime(year, 7, 2, 12) - dateref).days)
                    elif ctx.obj['frequency'] == 'mon':
                        app_log.info("monthly")
                        for year in range(ctx.obj['tstart'], ctx.obj['tend']+1):
                            for mon in range(1,13):
                                #tvals.append(cdtime.comptime(year, mon, 15).torel(refString,cdtime.DefaultCalendar).value)
                                tvals.append((datetime.datetime(year, mon, 15) - dateref).days)
                    elif ctx.obj['frequency'] == 'day':
                        app_log.info("daily")
                        #newstarttime = cdtime.comptime(opts['tstart'], 1, 1, 12).torel(refString,cdtime.DefaultCalendar).value
                        newstarttime = (datime.datetime(ctx.obj['tstart'], 1,  1, 12) - dateref).days
                        difftime = inrange_times[0] - newstarttime
                        #newendtimeyear = cdtime.comptime(opts['tend'], 12, 31, 12).torel(refString,cdtime.DefaultCalendar).value
                        newendtimeyear = (datetime.datetime(ctx.obj['tend'], 12, 31, 12) - dateref).days
                        numdays_cal = int(newendtimeyear - newstarttime + 1)
                        numdays_tvals = len(inrange_times)
                        #diff_days=numdays_cal - numdays_tvals
                        if numdays_cal == 366 and numdays_tvals == 365: 
                            app_log.info("adjusting for single leap year offset")
                            difftime = inrange_times[0] - newstarttime - 1
                        else: difftime = inrange_times[0] - newstarttime
                        tvals = np.array(inrange_times) - difftime
                    else: 
                        app_log.info("cannot manually create axis for this frequency, {ctx.obj['frequency']}")
                #print tvals                    
                #set refString to new value
                refString = 'days since {r:04d}-01-01'.format(r=ctx.obj['reference_date'])
                #
                #Handle different types of time axis
                #(mean, snapshot, climatology)
                #
                if cmor_tName == 'time':    
                    #data is a time mean
                    #If the data look like monthly data, or the time dimension is length one, set
                    #bounds appropriate for monthly data. We assume the Gregorian calendar has been
                    #in place since year 1, and will remain in place. This is probably all that is
                    #required for our purposes.
                    #
                    print("total time steps: {len(tvals)}")
                    if 'day2mon' in ctx.obj['axes_modifier']:
                        print("converting timevals from daily to monthly")
                        tvals,min_tvals,max_tvals = day2mon(tvals, ctx.obj['reference_date'])
                    elif 'mon2yr' in ctx.obj['axes_modifier']:
                        print("converting timevals from monthly to annual")
                        tvals,min_tvals,max_tvals=mon2yr(tvals,refString)
                    elif len(tvals) == 1:
                        print("one year of data")
                        try:
                            tvals = np.asarray(tvals)
                            min_tvals = [0.0]
                            max_tvals = [2 * tvals[0]]
                        except Exception as e:
                            print(f"E: {e}")
                            raise Exception('unable to compute time bounds')
                    elif (((tvals[1]-tvals[0]) >= 28) and ((tvals[1]-tvals[0]) <= 31)): # and (len(tvals) != 1):
                        print("monthly time bounds")
                        for i,ordinaldate in enumerate(tvals):
                            if (os.path.basename(all_files[0]).startswith('ice')) or (dim.find('time1') != -1 or dim.find('time_0') != -1):
                                ordinaldate = ordinaldate - 0.5
                            model_date = cdtime.reltime(int(ordinaldate),refString).tocomp(cdtime.DefaultCalendar)
                            #min bound is first day of month
                            model_date.day = 1
                            #min_tvals.append(model_date.torel(refString,cdtime.DefaultCalendar).value)
                            min_tvals.append((datetime.datetime(model_date) - dateref).days)
                            #max_bound is first day of next month
                            model_date.year = model_date.year+model_date.month/12
                            model_date.month = model_date.month%12+1                                
                            #max_tvals.append(model_date.torel(refString,cdtime.DefaultCalendar).value)
                            max_tvals.append((datetime.datetime(model_date) - dateref).days)
                            #if 'tMonOverride' in opts['axes_modifier']:
                            if os.path.basename(all_files[0]).startswith('ice') or (dim.find('time1') != -1):
                                #correct date to middle of month
                                mid = (max_tvals[i] - min_tvals[i]) / 2.
                                tvals[i] = min_tvals[i] + mid
                    else:
                        print("default time bounds")
                        if os.path.basename(all_files[0]).startswith('ice'):
                            tvals = tvals-0.5
                        try:
                            tvals = np.asarray(tvals)
                            min_tvals = np.append(1.5*tvals[0] - 0.5*tvals[1],(tvals[0:-1] + tvals[1:])/2)
                            max_tvals = np.append((tvals[0:-1] + tvals[1:])/2,(1.5*tvals[-1] - 0.5*tvals[-2]))
                        except:
                            print("E: Unable to compute time bounds")
                            raise Exception('unable to compute time bounds')
                    tval_bounds = np.column_stack((min_tvals,max_tvals))
                    #set up time axis:
                    cmor.set_table(tables[1])
                    time_axis_id = cmor.axis(table_entry=cmor_tName,
                        units=refString,
                        length=len(tvals),
                        coord_vals=tvals[:], 
                        cell_bounds=tval_bounds[:],
                        interval=None)
                    axis_ids.append(time_axis_id)
                    print("setup of time dimension complete")
                elif cmor_tName == 'time1':
                    #we are using time snapshots 
                    #set up time axis without cell_bounds
                    if os.path.basename(all_files[0]).startswith('ice'):
                        if (len(tvals) <= 1) or (((tvals[1]-tvals[0]) >= 28) and ((tvals[1]-tvals[0]) <= 31)):
                            print("monthly time bounds")
                            for i,ordinaldate in enumerate(tvals):
                                ordinaldate = ordinaldate - 0.5
                                model_date = cdtime.reltime(int(ordinaldate),refString).tocomp(cdtime.DefaultCalendar)
                                #min bound is first day of month
                                model_date.day = 1
                                #min_tvals.append(model_date.torel(refString,cdtime.DefaultCalendar).value)
                                min_tvals.append((datetime.datetime(model_date) - dateref).days)
                                #max_bound is first day of next month
                                model_date.year = model_date.year+model_date.month/12
                                model_date.month = model_date.month%12+1                                
                                #max_tvals.append(model_date.torel(refString,cdtime.DefaultCalendar).value)
                                max_tvals.append((datetime.datetime(model_date) - dateref).days)
                                #if 'tMonOverride' in opts['axes_modifier']:
                                if os.path.basename(all_files[0]).startswith('ice'):
                                    #correct date to middle of month
                                    mid = (max_tvals[i] - min_tvals[i]) / 2.
                                    tvals[i] = min_tvals[i] + mid
                        else:    
                            tvals = tvals - 0.5
                    elif ctx.obj['mode'] == 'ccmi' and tvals[0].is_integer():
                        if ctx.obj['cmip_table'].find('A10day') == -1:
                            tvals = tvals - 0.5
                            print('inst time shifted back half a day for CMOR')
                    elif 'yrpoint' in ctx.obj['axes_modifier']:
                        print("converting timevals from monthly to end of year")
                        tvals,min_tvals,max_tvals = yrpoint(tvals,refString) 
                    cmor.set_table(tables[1])
                    time_axis_id = cmor.axis(table_entry=cmor_tName,
                        units=refString,
                        length=len(tvals),
                        coord_vals=tvals[:],
                        interval=None)
                    axis_ids.append(time_axis_id)
                    print("setup of time dimension complete - W: no cell bounds")
                elif cmor_tName == 'time2':
                    #compute start and end bounds of whole time region
                    tstarts = []
                    tends = []
                    tmids = []
                    for n in range(12):
                        tstart = int(tvals[0])
                        tend = int(tvals[-1])
                        tstart= cdtime.reltime(tstart,refString).tocomp(cdtime.DefaultCalendar)
                        tstart.day = 1
                        tstart.month = n + 1
                        #tstart = tstart.torel(refString,cdtime.DefaultCalendar).value
                        tstart = (datetime.datetime(tstart) - dateref).days 
                        tstarts.append(tstart)
                        tend = cdtime.reltime(tend,refString).tocomp(cdtime.DefaultCalendar)
                        tend.month = n + 1
                        tend = tend.add(1,cdtime.Month)
                        tend = tend.add(-1,cdtime.Day)
                        #tend = tend.torel(refString,cdtime.DefaultCalendar).value
                        tend = (datetime.datetime(tend) - dateref).days 
                        tends.append(tend)
                        tmid = tstart + (tend - tstart) / 2
                        tmids.append(tmid)
                    tval_bounds = np.column_stack((tstarts,tends))
                    tvals = tmids
                    cmor.set_table(tables[1])
                    time_axis_id = cmor.axis(table_entry=cmor_tName,
                        units=refString,
                        length=len(tvals),
                        coord_vals=tvals[:],
                        cell_bounds=tval_bounds[:],
                        interval=None)
                    axis_ids.append(time_axis_id)
                    print("setup of climatology time dimension complete")
                else:
                    raise Exception(f"Dont know how to compute time bounds for time axis {cmor_tName}")
            elif (axis_name == 'Y')and 'dropY' in ctx.obj['axes_modifier']:
                if ((dim_vals == None) or (np.ndim(lat_vals) == 2 and
                     'dropX' in ctx.obj['axes_modifier'])):
                    # and (opts['axes_modifier'].find('') != -1):
                    #grid co-ordinates
                    cmor.set_table(tables[0])
                    j_axis_id = cmor.axis(table_entry='j_index',
                            units='1',
                            coord_vals=np.arange(len(dim_values)))
                else:
                    #lat values
                    #force the bounds back to the poles if necessary
                    if dim_val_bounds[0,0]<-90.0:
                        dim_val_bounds[0,0]=-90.0
                        print("setting minimum latitude bound to -90")
                    if dim_val_bounds[-1,-1]>90.0:
                        dim_val_bounds[-1,-1]=90.0
                        print("setting maximum latitude bound to 90")
                    cmor.set_table(tables[1])
                    if 'gridlat' in ctx.obj['axes_modifier']:
                        j_axis_id = cmor.axis(table_entry='gridlatitude',
                            units=dim_vals.units,
                            length=len(dim_values),
                            coord_vals=np.array(dim_values),
                            cell_bounds=dim_val_bounds[:])
                    else:
                        j_axis_id = cmor.axis(table_entry='latitude',
                            units=dim_vals.units,length=len(dim_values),
                            coord_vals=np.array(dim_values),
                            cell_bounds=dim_val_bounds[:])
                    n_grid_pts = n_grid_pts * len(dim_values)
                    axis_ids.append(j_axis_id)
                    z_ids.append(j_axis_id)
                print("setup of latitude dimension complete")
            elif (axis_name == 'X') and 'dropX' in ctx.obj['axes_modifier']:
                if dim_vals == None or np.ndim(lon_vals) == 2:
                    #grid co-ordinates
                    cmor.set_table(tables[0])
                    i_axis_id = cmor.axis(table_entry='i_index',
                            units='1',
                            coord_vals=np.arange(len(dim_values)))
                    n_grid_pts=len(dim_values)
                else:
                    #lon values
                    cmor.set_table(tables[1])
                    i_axis_id = cmor.axis(table_entry='longitude',
                        units=dim_vals.units,
                        length=len(dim_values),
                        coord_vals=np.mod(dim_values,360),
                        cell_bounds=dim_val_bounds[:])
                    n_grid_pts = n_grid_pts * len(dim_values)
                    axis_ids.append(i_axis_id)
                    z_ids.append(i_axis_id)
                print("setup of longitude dimension complete")    
            elif (axis_name == 'Z') and 'dropZ' in ctx.obj['axes_modifier']:
                z_len = len(dim_values)
                units = dim_vals.units
                #test different z axis names:
                if 'mod2plev19' in ctx.obj['axes_modifier']:
                    lev_name = 'plev19'
                    z_len = 19
                    units = 'Pa'
                    dim_values,dim_val_bounds = plev19()
                elif (dim == 'st_ocean') or (dim == 'sw_ocean'):
                    if 'depth100' in ctx.obj['axes_modifier']:
                        lev_name = 'depth100m'
                        dim_values = np.array([100])
                        dim_val_bounds = np.array([95,105])
                        z_len = 1
                    #ocean depth
                    else:
                        lev_name = 'depth_coord'
                    if ctx.obj['access_version'].find('OM2')!=-1 and dim == 'sw_ocean':
                        dim_val_bounds = dim_val_bounds[:]
                        dim_val_bounds[-1] = dim_values[-1]
                elif dim == 'potrho':
                    #ocean pressure levels
                    lev_name = 'rho'
                elif (dim.find('hybrid') != -1) or (dim == 'model_level_number') \
                    or (dim.find('theta_level') != -1) or (dim.find('rho_level') != -1):
                    ulev = 0.0001
                    units = 'm'
                    a_theta_85,b_theta_85,dim_val_bounds_theta_85,b_bounds_theta_85 = getHybridLevels('theta',85)
                    a_rho_85,b_rho_85,dim_val_bounds_rho_85,b_bounds_rho_85 = getHybridLevels('rho',85)
                    a_theta_38,b_theta_38,dim_val_bounds_theta_38,b_bounds_theta_38 = getHybridLevels('theta',38)
                    a_rho_38,b_rho_38,dim_val_bounds_rho_38,b_bounds_rho_38 = getHybridLevels('rho',38)
                    if z_len == 85:
                        if (a_theta_85[0]-ulev <= dim_values[0] <= a_theta_85[0]+ulev)\
                                or (dim == 'model_level_number') or (dim.find('theta_level') != -1):
                            print("85 atmosphere hybrid height theta (full) levels")
                            #theta levels
                            lev_name = 'hybrid_height'
                            if 'switchlevs' in ctx.obj['axes_modifier']:
                                lev_name = 'hybrid_height_half'
                            a_vals,b_vals,dim_val_bounds,b_bounds = getHybridLevels('theta',85)
                            if 'surfaceLevel' in ctx.obj['axes_modifier']:
                                print("surface level only")
                                #take only the first level    
                                a_vals = a_vals[0:1]
                                b_vals = b_vals[0:1]
                                z_len = 1
                            if dim_values[0] == 1:
                                dim_values = a_vals
                        elif (a_rho_85[0]-ulev <= dim_values[0] <= a_rho_85[0]+ulev)\
                                or (dim.find('rho_level') != -1):
                            print("85 atmosphere hybrid height rho (half) levels")
                            #rho levels
                            lev_name = 'hybrid_height_half'
                            if 'switchlevs' in ctx.obj['axes_modifier']:
                                lev_name = 'hybrid_height'
                            a_vals,b_vals,dim_val_bounds,b_bounds = getHybridLevels('rho',85)
                            if dim_values[0] == 1:
                                dim_values = a_vals
                    elif z_len == 38:
                        if (a_theta_38[0]-ulev <= dim_values[0] <= a_theta_38[0]+ulev)\
                                or (dim == 'model_level_number') or (dim.find('theta_level') != -1):
                            print("38 atmosphere hybrid height theta (full) levels")
                            #theta levels
                            lev_name = 'hybrid_height'
                            if 'switchlevs' in ctx.obj['axes_modifier']:
                                lev_name = 'hybrid_height_half'
                            a_vals,b_vals,dim_val_bounds,b_bounds = getHybridLevels('theta',38)
                            if 'surfaceLevel' in ctx.obj['axes_modifier']:
                                print("surface level only")
                                #take only the first level    
                                a_vals = a_vals[0:1]
                                b_vals = b_vals[0:1]
                                z_len = 1
                            if dim_values[0] == 1:
                                dim_values = a_vals
                        elif (a_rho_38[0]-ulev <= dim_values[0] <= a_rho_38[0]+ulev)\
                                or (dim.find('rho_level') != -1):
                            print("38 atmosphere hybrid height rho (half) levels")
                            #rho levels
                            lev_name = 'hybrid_height_half'
                            if 'switchlevs' in ctx.obj['axes_modifier']:
                                lev_name='hybrid_height'
                            a_vals,b_vals,dim_val_bounds,b_bounds=getHybridLevels('rho',38)
                            if dim_values[0] == 1:
                                dim_values=a_vals
                    else:
                        raise Exception(f"Unknown model levels starting at {dim_values[0]}")
                elif (dim == 'lev' or dim.find('_p_level') != -1):
                    print(ctx.obj['cmip_table'])
                    print(f"dim = {dim}")
                    #atmospheric pressure levels:
                    if z_len == 8:
                        lev_name = 'plev8'
                    elif z_len == 3:
                        lev_name = 'plev3'
                    elif z_len == 19:
                        lev_name = 'plev19'
                    elif z_len == 39:
                        lev_name = 'plev39'
                    else: 
                        raise Exception(f"Z levels do not match known levels {dim}")
                elif dim.find('pressure') != -1:
                    print(ctx.obj['cmip_table'])
                    print(f"dim = {dim}")
                    #atmospheric pressure levels:
                    if z_len == 8:
                        lev_name = 'plev8'
                    elif z_len == 3:
                        lev_name = 'plev3'
                    elif z_len == 19:
                        lev_name = 'plev19'
                    elif z_len == 39:
                        lev_name = 'plev39'
                    else: 
                        raise Exception(f"Z levels do not match known levels {dim}")
                elif (dim.find('soil') != -1) or (dim == 'depth'):
                    units = 'm'
                    if z_len == 4:
                        dim_values,dim_val_bounds = mosesSoilLevels()
                    elif z_len == 6:
                        dim_values,dim_val_bounds = cableSoilLevels()
                    else:
                        raise Exception(f"Z levels do not match known levels {dim}")
                    if 'topsoil' in ctx.obj['axes_modifier']:
                        #top layer of soil only
                        lev_name = 'sdepth1'
                        dim_values = dim_values[0:1]
                        dim_values[0] = 0.05
                        dim_val_bounds = dim_val_bounds[0:1]
                        dim_val_bounds[0][1] = 0.1
                    else:
                        #soil depth levels
                        lev_name = 'sdepth'
                else:
                    raise Exception(f"Unknown z axis {dim}")
                if ctx.obj['cmip_table'] == 'CMIP6_6hrLev' and lev_name.find('hybrid_height') == -1:
                    raise Exception('Variable on pressure levels instead of model levels. Exiting')
                print(f"lev_name = {lev_name}")
                cmor.set_table(tables[1])
                z_axis_id = cmor.axis(table_entry=lev_name,
                    units=units,length=z_len,
                    coord_vals=dim_values[:],
                    cell_bounds=dim_val_bounds[:])        
                axis_ids.append(z_axis_id)
                print("setup of height dimension complete")
            else: 
                #coordinates with axis_identifier other than X,Y,Z,T
                if dim.find('pseudo') != -1 and 'dropLev' in ctx.obj['axes_modifier']:
                    print("variable on tiles, setting pseudo levels...")
                    #z_len=len(dim_values)
                    #PP this might be wrong with new definition of axes_modifier!!!
                    lev_name = ctx.obj['axes_modifier'].split()
                    try:
                        for i in lev_name:
                            if i.find('type') != -1:
                                lev_name = i
                            else:
                                pass
                        if lev_name.find('type') == -1:
                            raise Exception('could not determine land type')
                    except:
                        raise Exception('could not determine land type, check variable dimensions and axes_modifiers')
                    landtype = det_landtype(lev_name)
                    cmor.set_table(tables[1])
                    #tiles=cableTiles()
                    axis_id = cmor.axis(table_entry=lev_name,
                            units='',
                            coord_vals=[landtype])
                    axis_ids.append(axis_id)
                if dim.find('pseudo') != -1 and 'landUse' in ctx.obj['axes_modifier']:
                    landUse = getlandUse()
                    z_len = len(landUse)
                    cmor.set_table(tables[1])
                    axis_id = cmor.axis(table_entry='landUse',
                            units='',
                         length=z_len,coord_vals=landUse)
                    axis_ids.append(axis_id)
                if dim.find('pseudo') != -1 and 'vegtype' in ctx.obj['axes_modifier']:
                    cabletiles = cableTiles()
                    z_len = len(cabletiles)
                    cmor.set_table(tables[1])
                    axis_id = cmor.axis(table_entry='vegtype',
                            units='',
                            length=z_len,
                            coord_vals=cabletiles)
                    axis_ids.append(axis_id)
                else:
                    print(f"Unidentified cartesian axis: {axis_name}")
        except Exception as e:
            print(f"Exception: {e}")
            print(f"Error setting dimension: {dim}")
            raise e
        #PP what to return?
        return (axis_ids, z_ids, i_axis_id, j_axis_id, time_axis_id,
                n_grid_pts, lev_name, z_len, dim_values, dim_vals, dim_val_bounds) 


def create_axis(name, table, app_log):
    """
    """
    app_log.info("creating {name} axis...")
    func_dict = {'oline': getTransportLines(),
                 'siline': geticeTransportLines(),
                 'basin': np.array(['atlantic_arctic_ocean','indian_pacific_ocean','global_ocean'])}
    result = func_dict[name]
    cmor.set_table(table)
    axis_id = cmor.axis(table_entry=name,
                        units='',
                        length=len(result),
                        coord_vals=result)
    app_log.info(f"setup of {name} axis complete")
    return axis_id


def hybrid_axis(lev, app_log):
    """
    """
    hybrid_dict = {'hybrid_height': 'b',
                   'hybrid_height_half': 'b_half'}
    orog_vals = getOrog()
    zfactor_b_id = cmor.zfactor(zaxis_id=z_axis_id,
        zfactor_name=hybrid_dict[lev],
        axis_ids=z_axis_id,
        units='1',
        type='d',
        zfactor_values=b_vals,
        zfactor_bounds=b_bounds)
    zfactor_orog_id = cmor.zfactor(zaxis_id=z_axis_id,
            zfactor_name='orog',
            axis_ids=z_ids,
            units='m',
            type='f',
            zfactor_values=orog_vals)
    return zfactor_b_id, zfactor_orog_id


@click.pass_context
def define_grid(ctx, i_axis_id, i_axis, j_axis_id, j_axis,
                tables, app_log):
    """If we are on a non-cartesian grid, Define the spatial grid
    """

    grid_id=None
    if i_axis_id != None and i_axis.ndim == 2:
        app_log.info("setting grid vertices...")
        #ensure longitudes are in the 0-360 range.
        if ctx.obj['access_version'] == 'OM2-025':
            app_log.info('1/4 degree grid')
            lon_vals_360 = np.mod(i_axis.values,360)
            lon_vertices = np.ma.asarray(np.mod(get_vertices_025(i_axis.name),360)).filled()
            #lat_vals_360=np.mod(lat_vals[:],300)
            lat_vertices = np.ma.asarray(get_vertices_025(j_axis.name)).filled()
            #lat_vertices=np.mod(get_vertices_025(lat_name),300)
        else:
            lon_vals_360 = np.mod(i_axis[:],360)
            lat_vertices = get_vertices(j_axis.name)
            lon_vertices = np.mod(get_vertices(i_axis.name),360)
        app_log.info(j_axis.name)
        app_log.debug(type(lat_vertices),lat_vertices[0])
        app_log.info(i_axis.name)
        app_log.debug(type(lon_vertices),lon_vertices[0])
        app_log.info(f"grid shape: {lat_vertices.shape} {lon_vertices.shape}")
        app_log.info("setup of vertices complete")
        try:
            #Set grid id and append to axis and z ids
            cmor.set_table(table)
            grid_id = cmor.grid(axis_ids=np.array([j_axis,i_axis]),
                    latitude=j_axis[:],
                    longitude=lon_vals_360[:],
                    latitude_vertices=lat_vertices[:],
                    longitude_vertices=lon_vertices[:])
                #replace i,j axis ids with the grid_id
            axis_ids.append(grid_id)
            z_ids.append(grid_id)
            app_log.info("setup of lat,lon grid complete")
        except Exception as e:
            app_log.error(f"E: We really should not be here! {e}")
    return grid_id, axis_ids, z_ids


@click.pass_context
def cmor_var(ctx, app_log, positive=None):
    """
    """
    variable_id = cmor.variable(table_entry=ctx.obj['vcmip'],
                    units=in_units,
                    axis_ids=axis_ids,
                    data_type='f',
                    missing_value=in_missing,
                    positive=positive)
    app_log.info(f"positive: {positive}")
    return variable_id


@click.pass_context
def new_axis_dim(ctx, invar, app_log):
    """
    """

    axis_ids = []
    z_ids = []
    time_axis_id = None
    z_axis_id = None    
    j_axis_id = None
    i_axis_id = None    
    n_grid_pts = 1
    lev_name = None
    z_len = 0

    # Check variable dimensions
    dims = invar.dims
    app_log("list of dimensions: {dim_list}")

    try:
        for i,dim in dims.enumerate:
            if 'time' in dim:
                t_axis = dsin[dim]
                t_axis.attrs['axis'] = 'T'
            elif any(x in dim for x in ['lat', 'y', 'nj']):
                j_axis = dsin[dim]
                j_axis.attrs['axis'] = 'Y'
            elif any(x in dim for x in ['lon', 'x', 'ni']):
                i_axis = dsin[dim]
                i_axis.attrs['axis'] = 'X'
            elif any(x in dim for x in ['lev', 'heigth', 'depth']):
                z_axis = dsin[dim]
                z_axis.attrs['axis'] = 'Z'
    except:
        #
        #The coordinate variable associated with "dim" was not found.
        #
        #PP this is not quite right!
        app_log.info("W: No coordinate variable associated with the dimension {dim}")
     #PP after this anything should be really done right at the end of calculation!!!
        try:
            #
            #Try and get the dimension bounds. The bounds attribute might also be called "edges"
            #If we cannot find any dimension bounds, create default bounds in a sensible way.
            dim_val_bounds = dsin[dim].bounds
            app_log.info("using dimension bounds")
        except:
            dim_val_bounds = dsin[dim].edges
            app_log.info("using dimension edges as bounds")
        else:
            #
            #Unable to get the dimension bounds, create some default bounds.
            #The default bounds assume that the grid cells are centred on
            #each grid point specified by the coordinate variable.
            #
            app_log.info("I: No bounds for {dim} - creating default bounds")
            try:
                min_vals = np.append((1.5*dim_values[0] - 0.5*dim_values[1]),(dim_values[0:-1] + dim_values[1:])/2)
                max_vals = np.append((dim_values[0:-1] + dim_values[1:])/2,(1.5*dim_values[-1] - 0.5*dim_values[-2]))
            except Exception as e:
                app_log.warning(f"dodgy bounds for dimension: {dim}")
                app_log.error(f"error: {e}")
                min_vals = dim_values[:] - 15
                max_vals = dim_values[:] + 15
            dim_val_bounds = np.column_stack((min_vals,max_vals))
    return (axis_ids, z_ids, i_axis_id, j_axis_id, time_axis_id, n_grid_pts,
                lev_name, z_len, dim_values, dim_vals, dim_val_bounds) 


@click.pass_context
def get_attrs(ctx, invar, app_log):
    """
    """
    var_attrs = invar.attrs 
    in_units = ctx.obj['in_units']
    if in_units in [None, '']:
        in_units = var_attrs.get('units', 1)
    in_missing = var_attrs.get('_FillValue', 9.96921e+36)
    in_missing = var_attrs.get('missing_value', in_missing)
    in_missing = float(in_missing)
    if all(x not in var_attrs.keys() for x in ['_FillValue', 'missing_value']):
        app_log.info("trying fillValue as missing value")
        
    #Now try and work out if there is a vertical direction associated with the variable
    #(for example radiation variables).
    #search for positive attribute keyword in standard name / postive option
    positive = None
    if ctx.obj['positive'] in ['up', 'down']:
        positive = ctx.obj['positive']
    else:
        standard_name = var_attrs.get('standard_name', 'None')
        # .lower shouldn't be necessary as standard_names are always lower_case
        if any(x in standard_name.lower() for x in ['up', 'outgoing', 'out_of']):
            positive = 'up'
        elif any(x in standard_name.lower() for x in ['down', 'incoming', 'into']):
            positive = 'down'
    return in_units, in_missing, positive


@click.pass_context
def axm_t_integral(ctx, invar, dsin, variable_id, app_log):
    """I couldn't find anywhere in mappings where this is used
    so I'm keeping it exactly as it is it's not worth it to adapt it
    still some cdms2 options and we're now passing all files at one time but this code assumes more than one file
    """
    try:
        run = np.float32(ctx.obj['calculation'])
    except:
        run = np.float32(0.0)
    #for input_file in inrange_files:
    #If the data is a climatology, store the values in a running sum

    t = invar[time_dimension]
    # need to look ofr xarray correspondent of daysInMonth (cdms2)
    tbox = daysInMonth(t)
    varout = np.float32(var[:,0]*tbox[:]*24).cumsum(0) + run
    run = varout[-1]
    #if we have a second variable, just add this to the output (not included in the integration)
    if len(ctx.obj['vin']) == 2:
        varout += dsin[ctx.obj['vin'][1]][:]
    cmor.write(variable_id, (varout), ntimes_passed=np.shape(varout)[0])
    return


@click.pass_context
def axm_timeshot(ctx, dsin, variable_id, app_log):
    """
        #Set var to be sum of variables in 'vin' (can modify to use calculation if needed)
    """
    var = None
    for v in ctx.obj['vin']:
        try:        
            var += (dsin[v])
            app_log.info("added extra variable")
        #PP I'm not sure this makes sense, if sum on next variable fails then I restart from that variable??
        except:        
            var = dsin[v][:]
    try: 
        vals_wsum, clim_days = monthClim(var,t,vals_wsum,clim_days)
    except:
        #first time
        tmp = var[0,:].shape
        out_shape = (12,) + tmp
        vals_wsum = np.ma.zeros(out_shape,dtype=np.float32)
        app_log.info(f"first time, data shape: {np.shape(vals_wsum)}")
        clim_days = np.zeros([12],dtype=int)#sum of number of days in the month
        vals_wsum,clim_days = monthClim(var,t,vals_wsum,clim_days)
    #calculate the climatological average for each month from the running sum (vals_wsum)
    #and the total number of days for each month (clim_days)
    for j in range(12):
        app_log.info(f"month: {j+1}, sum of days: {clim_days[j]}")
        #average vals_wsum using the total number of days summed for each month
        vals_wsum[j,:] = vals_wsum[j,:] / clim_days[j]
    cmor.write(variable_id, (vals_wsum), ntimes_passed=12)


@click.pass_context
def axm_mon2yr(ctx, invar, dsin, variable_id, app_log):
    """A lot of the original code was tryiong to get the year from eiother the input filename 
       or from the timestap. With xarray we can just calculate this with resample and datetime accessor
    """
    if ctx.obj['calculation'] != '':
            #PP I really do not udnerstand what calculateVals does aside from extracting variable
            # the axis needed by calculation and executing calculation 
        data = calculateVals(dsin, ctx.obj['vin'], ctx.obj['calculation'])
    vshape = np.shape(data)
    app_log.debug(vshape)
        #PP don't need this block
        #for year in range(startyear,endyear+1):
        #    print(f"processing year {year}")
        #    count = 0
        #    vsum = np.ma.zeros(vshape[1:],dtype=np.float32)
        #    for input_file in inrange_files:
        #        if 'tMonOverride' in opts['axes_modifier']:
        #            print("reading date info from file name")
        #            if os.path.basename(input_file).startswith('ocean'):
        #                yearstamp = int(os.path.basename(input_file).split('.')[1][3:7])
        #            else:
        #                if opts['access_version'].find('CM2') != -1:
        #                    yearstamp = int(os.path.basename(input_file).split('.')[1][2:6])
        #                elif opts['access_version'].find('ESM') != -1:
        #                    yearstamp = int(os.path.basename(input_file).split('.')[1][3:7])
        #            access_file = xr.open_dataset(f'{input_file}')
        #            t = access_file[opts['vin'][0]].getTime()
        #            datelist = t.asComponentTime()
        #            if yearstamp == year:
        #                yearinside=True
        #            else:
        #                yearinside = False
        #        else:
        #            print('reading date info from time dimension')
        #           access_file = xr.open_dataset(f'{input_file}')
        #            t = access_file[opts['vin'][0]].getTime()
        #            datelist = t.asComponentTime()
        #            yearinside = False
        #            for date in datelist:
        #                if date.year == year: yearinside=True
        #        #try: print year, yearstamp, yearinside, input_file
        #        #except: print year, yearinside, input_file
        #        if yearinside:
        #            print(f"found data in file {input_file}")
        #            for index, d in enumerate(datelist[:]):
        #                if (d.year == year) or 'tMonOverride' in opts['axes_modifier']:
        #                    if opts['calculation'] == '':
        #                        data_vals = access_file[opts['vin'][0]][:]
        #                    else:
        #                        data_vals = calculateVals((access_file,),opts['vin'],opts['calculation'])
        #                    try: 
        #                        data_vals = data_vals.filled(in_missing)
        #                    except: 
        #                        pass
        #                    print(f"shape: {np.shape(data_vals)}")
        #                    print(f"time index: {index}, date: {d}")
        #                    try: 
        #                        vsum += data_vals[index,:,:]
        #                        count += 1
        #                    except: 
        #                        vsum += data_vals[index,:,:,:]
        #                        count += 1
        #        access_file.close()
    vyr = data.groupby('year').mean()
        #    if count == 12:
        #        vyr = vsum / 12
    app_log.info("writing with cmor...")
    cmor.write(variable_id, vyr.values, ntimes_passed=1)
        #    else:
        #        print(count)
        #        raise Exception(f'WARNING: annual data contains {count} months of data')    
    return


@click.pass_context
def calc_a10daypt(ctx, dsin, time_dimension, variable_id, app_log):
    """
    """
    app_log.info('ONLY 1st, 11th, 21st days to be used')
    dsinsel = dsin.where(dsin[time_dimension].dt.day.isin([1, 11, 21]), drop=True)
    a10_datavals = dsinsel[ctx.obj['vin']]
    if ctx.obj['calculation'] != '':
        app_log.info("calculating...")
        a10_datavals = calculateVals(dsinsel, ctx.obj['vin'], ctx.obj['calculation'])
        try:
            a10_datavals = a10_datavals.filled(in_missing)
        except:
            #if values aren't in a masked array
            pass 
    app_log.info("writing with cmor...")
    try:
        if time_dimension != None:
            #assuming time is the first dimension
            app_log.info(a10_datavals.shape)
            cmor.write(variable_id, a10_datavals.values,
                ntimes_passed=a10_datavals.shape[0])
        else:
            cmor.write(variable_id, a10_datavals.values, ntimes_passed=0)
    except Exception as e:
        app_log.error(f"E: Unable to write the CMOR variable to file {e}")
        raise
    return


@click.pass_context
def calc_monsecs(ctx, dsin, tdim, variable_id, app_log):
    """
    """
    monsecs = calendar.monthrange(dsin[tdim].dt.year,dsin[tdim].dt.month)[1] * 86400
    if ctx.obj['calculation'] == '':
        data = dsin[ctx.obj['vin'][0]]
        data_vals = data_vals / monsecs
        #print(data_vals)
    else:
        app_log.info("calculating...")
        data = calculateVals(dsin, ctx.obj['vin'], ctx.obj['calculation'])
        data = data / monsecs
        #convert mask to missing values
        try: 
            data_vals = data.values().filled(in_missing)
        except:
            #if values aren't in a masked array
            pass 
    app_log.info("writing with cmor...")
    try:
        if time_dimension != None:
            #assuming time is the first dimension
            app_log.info(np.shape(data_vals))
            cmor.write(variable_id, data_vals.values,
                ntimes_passed=np.shape(data_vals)[0])
        else:
            cmor.write(variable_id, data_vals, ntimes_passed=0)
    except Exception as e:
        print(f"E: Unable to write the CMOR variable to file {e}")
        raise
    return


@click.pass_context
def normal_case(ctx, dsin, tdim, variable_id, app_log):
    """
    """
    try:
        if ctx.obj['calculation'] == '':
            data = fobj[ctx.obj['vin'][0]][:]
            app_log.debug(data)
        else:
            print("calculating...")
            data = calculateVals((fobj,), ctx.obj['vin'], ctx.obj['calculation'])
            #convert mask to missing values
            try:
                data_vals = data.values().filled(in_missing)
            except:
                #if values aren't in a masked array
                pass 
        if 'depth100' in ctx.obj['axes_modifier']:
            data_vals = depth100(data_vals[:,9,:,:], data_vals[:,10,:,:])
    except Exception as e:
        app_log.error(f"E: Unable to process data {e}")
        raise
    #If the data is not a climatology:
    #Write the data to the CMOR file.
    else:
        try:
            app_log.info('writing...')
            if time_dimension != None:
                #assuming time is the first dimension
                app_log.info(np.shape(data_vals))
                cmor.write(variable_id, data_vals.values,
                    ntimes_passed=np.shape(data_vals)[0])
            else:
                cmor.write(variable_id, data_vals.values, ntimes_passed=0)
        except:
            pass
    return
