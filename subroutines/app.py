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
# for some reason this still imports all the datetime module???
#from datetime import datetime
import datetime
import re
from app_functions import *
import os,sys
#import cdms2
import xarray as xr
from cftime import num2date, date2num
#import cdtime
#import cmorx as cmor
import cmor
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
import time as timetime
import psutil
import calendar

cmorlogs=os.environ.get('CMOR_LOGS')

#
#main function to post-process files
#
def app(option_dictionary):
    start_time = timetime.time()
    print("starting main app function...")
    #check the options passed to the function:    
    len0 = len(opts)
    opts.update(option_dictionary) 
    #overwrite default parameters with new parameters
    len1 = len(opts)
    if(len0 != len1): 
        #new parameters don't match old ones 
        raise ValueError('Error: {} input parameters don\'t match valid variable names'.format(str(len1-len0)))
    #
    #cdtime.DefaultCalendar = cdtime.GregorianCalendar
    default_cal = "gregorian"
    #
    cmor.setup(inpath=opts['cmip_table_path'],
        netcdf_file_action = cmor.CMOR_REPLACE_4,
        set_verbosity = cmor.CMOR_NORMAL,
        exit_control = cmor.CMOR_NORMAL,
        #exit_control=cmor.CMOR_EXIT_ON_MAJOR,
        logfile = f"{cmorlogs}/log", create_subdirectories=1)
    #
    #Define the dataset.
    #
    cmor.dataset_json(opts['json_file_path'])
    #
    #Write a global variable called version_number which is used for CSIRO purposes.
    #
    #try:
    #    cmor.set_cur_dataset_attribute('version_number', opts['version_number'])
    #except:
    #    print("E: Unable to add a global attribute called version_number")
    #    raise Exception('E: Unable to add a global attribute called version_number')
    #
    #Write a global variable called notes which is used for CSIRO purposes.
    #
    cmor.set_cur_dataset_attribute('notes', opts['notes'])
    #except:
    #    print 'E: Unable to add a global attribute called notes'
    #    raise Exception('E: Unable to add a global attribute called notes')
    cmor.set_cur_dataset_attribute('exp_description', opts['exp_description'])
    cmor.set_cur_dataset_attribute('contact', os.environ.get('CONTACT'))
    #
    #Load the CMIP tables into memory.
    #
    tables = []
    tables.append(cmor.load_table(f"{opts['cmip_table_path']}/CMIP6_grids.json"))
    tables.append(cmor.load_table(f"{opts['cmip_table_path']}/{opts['cmip_table']}.json"))
    #
    #PP SEPARATE FUNCTION
    #Find all the ACCESS file names which match the "glob" pattern.
    #Sort the filenames, assuming that the sorted filenames will
    #be in chronological order because there is usually some sort of date
    #and/or time information in the filename.
    #
    print(f"input file structure: {opts['infile']}")
    print(opts['cmip_table'])
    tmp = opts['infile'].split()
    #if there are two different files used make a list of extra access files
    if len(tmp)>1:
        extra_access_files = glob.glob(tmp[1])
        extra_access_files.sort()
        opts['infile'] = tmp[0]
    else:
        extra_access_files = None
    #set normal set of files
    all_access_files = glob.glob(opts['infile'])
    all_access_files.sort()
    #hack to remove files not in time range
    tmp = []
    for fn in all_access_files:
        if os.path.basename(fn).startswith('ice'):
            #tstamp=int(os.path.basename(fn).split('.')[1][0:3])
            tstamp = int(re.search("\d{4}",os.path.basename(fn)).group())
        elif os.path.basename(fn).startswith('ocean'):
            #tstamp=int(os.path.basename(fn).split('.')[1][3:6])
            tstamp = int(re.search("\d{4}",os.path.basename(fn)).group())
        else:
            if opts['access_version'].find('CM2') != -1:
                tstamp = int(os.path.basename(fn).split('.')[1][2:6])
            elif opts['access_version'].find('ESM') != -1:
                tstamp = int(os.path.basename(fn).split('.')[1][3:7])
            else:
                raise Exception('E: ACCESS_version not identified')
        if opts['tstart'] <= tstamp and tstamp <= opts['tend']:
            tmp.append(fn)
    all_access_files = tmp
    if extra_access_files != None:
        tmp = []
        for fn in extra_access_files:
            tstamp = int(re.search("\d{4}",fn).group())
            if opts['tstart'] <= tstamp and tstamp <= opts['tend']:
                tmp.append(fn)
        extra_access_files = tmp
    del tmp
    # PP FUNCTION END return all_access_files, extra_access_files
    print(f"access files from: {os.path.basename(all_access_files[0])} to {os.path.basename(all_access_files[-1])}")
    print(f"first file: {all_access_files[0]}")
    inrange_access_files = []
    inrange_access_times = []
    #
    #PP FUNCTION check var in files
    #find file with first element of 'vin'
    i = 0
    while True:
        try:
            temp_file = netCDF4.Dataset(all_access_files[i], 'r')
        except IndexError: 
            #gone through all files and haven't found variable
            raise Exception(f"Error! Variable missing from files: {opts['vin'][0]}")
        try:
            #see if the variable is in the file
            var = temp_file[opts['vin'][0]]
            break
        except:
            #try next file
            temp_file.close()
            i+=1
            continue
    print(f"using file: {all_access_files[i]}")
    try:
        print(f"variable '{opts['vin'][0]}' has units in ACCESS file: {var.units}")
    except:
        print(f"variable '{opts['vin'][0]}' has no units listed in ACCESS file")
    #PP end of function
    #
    #PP start time_dimension function
    time_dimension = None
    #find time info: time axis, reference time and set tstart and tend
    #    
    try:
        if  (opts['axes_modifier'].find('dropT') == -1) and (opts['cmip_table'].find('fx') == -1):
            #try to find and set the correct time axis:
            for var_dim in var.dimensions:
                if var_dim.find('time') != -1 or temp_file[var_dim].axis == 'T':
                    time_dimension = var_dim
                    #PP
                    print(time_dimension)
    except:
        #use the default time dimension 'time'
        pass 
    if opts['axes_modifier'].find('tMonOverride') != -1:
        #if we want to override dodgey units in the input files
        print("overriding time axis...")
        refString = "days since {r:04d}-01-01".format(r=opts['reference_date'])
        #time_dimension=None    
        inrange_access_files = all_access_files
        startyear = opts['tstart']
        endyear = opts['tend']
    elif opts['cmip_table'].find('fx') != -1:
        print("fx variable, no time axis")
        refString = f"days since {opts['reference_date'][:4]}-01-01"
        time_dimension = None    
        inrange_access_files = all_access_files
    else:
        try:
            #
            #refdate is the "reference date" used as an arbitray 0 point for dates. Common values for
            #refdate are 1 (0001-01-01) and 719 163 (1970-01-01), but other values are possible.
            #We cannot handle negative reference dates (ie. BC dates); maybe we will encounter these
            #in some climate runs?
            #
            time = temp_file[time_dimension]
            refString = time.units
            #if opts['reference_date'] != "":
            #    refString = "days since {r:04d}-01-01".format(r=opts['reference_date'])
        except:
            refString = "days since 0001-01-01"
            print("W: Unable to extract a reference date: assuming it is 0001")
        temp_file.close()
        try:
            # PP set reference time as datetime
            date1 = refString.split('since ')[1]
            #PP ignoring time assuming is 0
            dateref =  date1.split(" ")[0]
            print(f"Paola dateref step1: {dateref}")
            dateref = datetime.datetime.strptime(dateref, "%Y-%m-%d")
            print(f"Paola dateref step2: {dateref}")
            #set to very end of the year
            startyear = int(opts['tstart'])
            endyear = int(opts['tend'])
            #PP shouldn't we check first that this is already what we want?
            # this will produce first a year,month etc time and then convert it to a relative time
            # 
            #opts['tstart'] = cdtime.comptime(opts['tstart']).torel(refString,cdtime.DefaultCalendar).value
            opts['tstart'] = (datetime.datetime(startyear, 1, 1) - dateref).days
            #opts['tstart'] = date2num(startyear, units=refString, calendar=default_cal)
            print('Paola start after datetime', opts['tstart'])
            if os.path.basename(all_access_files[0]).startswith('ice'):
                #opts['tend'] = cdtime.comptime(opts['tend']+1).torel(refString,cdtime.DefaultCalendar).value
                opts['tend'] = (datetime.datetime(endyear+1, 1, 1) - dateref).days
                #opts['tend'] = date2num(endyear, units=refString, calendar=default_cal)
            elif (time_dimension.find('time1') != -1 or time_dimension.find('time_0') != -1) \
                and (opts['frequency'].find('mon') != -1) and (opts['timeshot'].find('mean') != -1):
                #opts['tend'] = cdtime.comptime(opts['tend']+1).torel(refString,cdtime.DefaultCalendar).value
                opts['tend'] = (datetime.datetime(endyear+1, 1, 1) - dateref).days
                #opts['tend'] = date2num(endyear + 1, units=refString, calendar=default_cal)
            else:
                #opts['tend'] = cdtime.comptime(opts['tend']+1).torel(refString,cdtime.DefaultCalendar).value - 0.01 
                opts['tend'] = (datetime.datetime(endyear+1, 1, 1) - dateref).days - 0.01
                print(f"shuld be here: {opts['tend']}")
                #opts['tend'] = date2num(endyear + 1, units=refString, calendar=default_cal) - 0.01
            print(f"time start: {opts['tstart']}")
            print(f"time end: {opts['tend']}")
        except Exception as e:
            print(f"time range not correctly supplied. {e}")
    #
    #Now find all the ACCESS files in the desired time range (and neglect files outside this range).
    #
    #PP start function to retunr file in time range
    print("loading files...")
    print(f"time dimension: {time_dimension}")
    sys.stdout.flush()
    exit = False
    # if a time dimension exists and tMonOverride is not part of axes_modifiers
    if (time_dimension != None) and (opts['axes_modifier'].find('tMonOverride') == -1):
        for i, input_file in enumerate(all_access_files):
            try:
                access_file=netCDF4.Dataset(input_file,'r')
                #
                #Read the time information.
                #
                tvals = access_file[time_dimension]
                print(f"tend: {opts['tend']}")
                print(f"tstart: {opts['tstart']}")
                print("first values time axis")
                print( float(tvals[0]))
                #test each file to see if it contains time values within the time range from tstart to tend
                #if (opts['tend'] == None or float(tvals[0]) < float(opts['tend'])) and (opts['tstart'] == None or float(tvals[-1]) > float(opts['tstart'])):
                if (opts['tend'] == None or float(tvals[0]) <= float(opts['tend'])) and (opts['tstart'] == None or float(tvals[-1]) >= float(opts['tstart'])):
                    print("if thisnshows I'm in if")
                    inrange_access_files.append(input_file)
                    if opts['axes_modifier'].find('firsttime') != -1:
                        #only take first time
                        print("first time stamp used only")
                        inrange_access_times.append(tvals[0])
                        exit = True
                    else:
                        print("I really should b ehere")
                        irefString = access_file[time_dimension].units
                        print(f"irefString: {irefString}, refstring: {refString}")
                        if irefString != refString: 
                            #print( 'WRONG refString ', irefString, refString)
                            #tvals = np.array(tvals) + cdtime.reltime(0,irefString).torel(refString,cdtime.DefaultCalendar).value
                            print(f"just before tvals: dateref {dateref}")
                            print(f"file ref time correpsondent:  {datetime.datetime(irefString[:-10])}")
                            tvals = np.array(tvals) + (datetime.datetime(irefString[:-10]) - dateref).days 
                            print(f"should be here: tvals: {tvals}")
                        inrange_access_times.extend(tvals[:])
                #
                #Close the file.
                #
                access_file.close()
            except Exception as e:
                print(f"Cannot open file: {e}")
            if exit:
                break
    else:
        #if we are using a time invariant parameter, just use a file with vin
        if opts['cmip_table'].find('fx') != -1:
            inrange_access_files = [all_access_files[0]]
        else:
            for i, input_file in enumerate(all_access_files):
                try:
                    access_file = netCDF4.Dataset(input_file,'r')
                    tvals = access_file[time_dimension]
                    #print( opts['tend'])
                    #print( float(tvals[0]))
                    irefString = access_file[time_dimension].units
                    print(f"irefsttring: {irefString}")
                    print(f"refsttring: {refString}")
                    if irefString != refString: 
                        #print 'WRONG refString ', irefString, refString
                        #tvals = np.array(tvals) + cdtime.reltime(0,irefString).torel(refString,cdtime.DefaultCalendar).value
                        print(f"just before tvals: dateref {dateref}")
                        print(irefString[:-10])
                        print(f"file ref time correpsondent:  {datetime.datetime(irefString[:-10])}")
                        tvals = np.array(tvals) + (datetime.datetime(irefString[:-10]) - dateref).days 
                        print(f"should be here: tvals: {tvals}")
                    inrange_access_times.extend(tvals[:])
                    access_file.close()
                except Exception as e:
                    print(f"Cannot open file: {e}")
                if exit:
                    break
    print(f"number of files in time range: {inrange_access_files}")
    #check if the requested range is covered
    if inrange_access_files == []:
        print("no data exists in the requested time range")
        return 0
    #PP end function
    #
    #Load the first ACCESS NetCDF data file, and get the required information about the dimensions and so on.
    #
    access_file=netCDF4.Dataset(inrange_access_files[0],'r')
    print("opened input netCDF file: {inrange_access_files[0]}")
    print("checking axes...")
    sys.stdout.flush()
    try:
        #
        #Determine which axes are X and Y, and what the values of the latitudes and longitudes are.
        #
        try:
            data_vals=access_file[opts['vin'][0]]
            print("shape of data: {np.shape(data_vals)}")
        except Exception as e:
            print("E: Unable to read {opts['vin'][0]} from ACCESS file")
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
        #search for strings 'lat' or 'lon' in co-ordinate names
        print(coord_vals)
        for coord in coord_vals:
            if coord.lower().find('lon') != -1:
                print(coord)
                lon_name = coord
                try:
                    lon_vals = access_file[coord]
                except:
                    if os.path.basename(inrange_access_files[0]).startswith('ocean'):
                        if opts['access_version'] == 'OM2-025':
                            acnfile = ancillary_path+'grid_spec.auscom.20150514.nc'
                            acndata = netCDF4.Dataset(acnfile,'r')
                            lon_vals = acndata.variables['geolon_t']
                        else:
                            acnfile = ancillary_path+'grid_spec.auscom.20110618.nc'
                            acndata = netCDF4.Dataset(acnfile,'r')
                            lon_vals = acndata.variables['x_T']
                    if os.path.basename(inrange_access_files[0]).startswith('ice'):
                        if opts['access_version'] == 'OM2-025':
                            acnfile = ancillary_path+'cice_grid_20150514.nc'
                        else:
                            acnfile = ancillary_path+'cice_grid_20101208.nc'
                        acndata = netCDF4.Dataset(acnfile,'r')
                        lon_vals = acndata.variables[coord]
            elif coord.lower().find('lat') != -1:
                print(coord)
                lat_name = coord
                try:
                    lat_vals = access_file[coord]
                    print('lat from file')
                except:
                    print('lat from ancil')
                    if os.path.basename(inrange_access_files[0]).startswith('ocean'):
                        if opts['access_version'] == 'OM2-025':
                            acnfile = ancillary_path+'grid_spec.auscom.20150514.nc'
                            acndata = netCDF4.Dataset(acnfile,'r')
                            lat_vals = acndata.variables['geolat_t']
                        else:
                            acnfile = ancillary_path+'grid_spec.auscom.20110618.nc'
                            acndata = netCDF4.Dataset(acnfile,'r')
                            lat_vals = acndata.variables['y_T']
                    if os.path.basename(inrange_access_files[0]).startswith('ice'):
                        if opts['access_version'] == 'OM2-025':
                            acnfile = ancillary_path+'cice_grid_20150514.nc'
                        else:
                            acnfile = ancillary_path+'cice_grid_20101208.nc'
                        acndata = netCDF4.Dataset(acnfile,'r')
                        lat_vals = acndata.variables[coord]
        #create a list of dimensions
        dim_list = data_vals.dimensions
        #
        #Work out which dimension(s) are associated with each coordinate variable.
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
        print("list of dimensions: {dim_list}")
        for dim in dim_list:
            print(axis_ids)
            try:
                dim_vals = access_file[dim]
                dim_values = dim_vals[:]
            except:
                #
                #The coordinate variable associated with "dim" was not found.
                #
                print("W: No coordinate variable associated with the dimension {dim}")
                dim_vals = None
                #This should work. Look out for 'if dim_vals=None' and other use of dim_values, e.g. dim_values[:]
                dim_values = access_file.dimensions[dim]
            #
            #See if this dimension represents a spatial or temporal axis (X, Y, T and so on)
            #This information will either be stored in the axis or cartesian_axis variable attribute.
            #
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
                    elif (dim == time_dimension) or (dim.find('time') != -1):
                        axis_name = 'T'
                    else:
                        axis_name='unknown'
            print(f"evaluating axis: {axis_name}")
            try:
                #
                #Try and get the dimension bounds. The bounds attribute might also be called "edges"
                #If we cannot find any dimension bounds, create default bounds in a sensible way.
                #
                dim_val_bounds=access_file[dim_vals.bounds]
                print("using dimension bounds")
            except:
                try:
                    dim_val_bounds=access_file[dim_vals.edges]
                    print("using dimension edges as bounds")
                except:
                    #
                    #Unable to get the dimension bounds, create some default bounds.
                    #The default bounds assume that the grid cells are centred on
                    #each grid point specified by the coordinate variable.
                    #
                    print("I: No bounds for {dim} - creating default bounds")
                    if dim_vals == None:
                        print("No dimension values")
                    else:
                        try:
                            min_vals = np.append((1.5*dim_values[0] - 0.5*dim_values[1]),(dim_values[0:-1] + dim_values[1:])/2)
                            max_vals = np.append((dim_values[0:-1] + dim_values[1:])/2,(1.5*dim_values[-1] - 0.5*dim_values[-2]))
                        except Exception as e:
                            print(f"WARNING: dodgy bounds for dimension: {dim}")
                            print(f"error: {e}")
                            min_vals = dim_values[:] - 15
                            max_vals = dim_values[:] + 15
                        dim_val_bounds = np.column_stack((min_vals,max_vals))
            print("handling different axes types")
            try:
                if (axis_name == 'T') and (opts['axes_modifier'].find('dropT') == -1) and (opts['cmip_table'].find('fx') == -1):
                    #
                    #Set cmor time variable name: 
                    #For mean values this is just 'time'
                    #for synoptic snapshots, this is 'time1'
                    #for climatoloies this is time2
                    #
                    print(f"dimension: {dim}")
                    if opts['timeshot'].find('mean') != -1:
                        cmor_tName='time'
                    elif opts['timeshot'].find('inst') != -1:
                        cmor_tName='time1'
                    elif opts['timeshot'].find('clim') != -1:
                        cmor_tName='time2'
                    else:
                        #assume timeshot is mean
                        print("timeshot unknown or incorrectly specified")
                        cmor_tName = 'time'
                    #initialise stuff:
                    min_tvals = []
                    max_tvals = []
                    if opts['axes_modifier'].find('tMonOverride') == -1:
                        #convert times to days since reference_date
                        # PP temporarily comment as I'm not sure what this is for
                        #tvals = np.array(inrange_access_times) + cdtime.reltime(0,refString).torel('days since {r:04d}-01-01'.format(r=opts['reference_date']),cdtime.DefaultCalendar).value
                        print(f"time values converted to days since 01,01,{opts['reference_date']:04d}: {tvals[0:5]}...{tvals[-5:-1]}")
                        if opts['cmip_table'].find('A10day') != -1:
                            print('Aday10: selecting 1st, 11th, 21st days')
                            a10_tvals = []
                            for a10 in tvals:
                                #a10_comp = cdtime.reltime(a10,'days since {r:04d}-01-01'.format(r=opts['reference_date'])).tocomp(cdtime.DefaultCalendar)
                                a10_comp = a10.date()
                                #print(a10, a10_comp, a10_comp.day)
                                if a10_comp.day in [1,11,21]:
                                    a10_tvals.append(a10)
                            tvals = a10_tvals
                    else:
                        print("manually create time axis")
                        tvals = []
                        if opts['frequency'] == 'yr':
                            print("yearly")
                            for year in range(opts['tstart'],opts['tend']+1):
                                #tvals.append(cdtime.comptime(year, 7, 2, 12).torel(refString,cdtime.DefaultCalendar).value)
                                tvals.append((datetime.datetime(year, 7, 2, 12) - dateref).days)
                        elif opts['frequency'] == 'mon':
                            print("monthly")
                            for year in range(opts['tstart'],opts['tend']+1):
                                for mon in range(1,13):
                                    #tvals.append(cdtime.comptime(year, mon, 15).torel(refString,cdtime.DefaultCalendar).value)
                                    tvals.append((datetime.datetime(year, mon, 15) - dateref).days)
                        elif opts['frequency'] == 'day':
                            print("daily")
                            #newstarttime = cdtime.comptime(opts['tstart'], 1, 1, 12).torel(refString,cdtime.DefaultCalendar).value
                            newstarttime = (datime(opts['tstart'], 1,  1, 12) - dateref).days
                            difftime = inrange_access_times[0] - newstarttime
                            #newendtimeyear = cdtime.comptime(opts['tend'], 12, 31, 12).torel(refString,cdtime.DefaultCalendar).value
                            newendtimeyear = (datetime.datetime(opts['tend'], 12, 31, 12) - dateref).days
                            numdays_cal = int(newendtimeyear - newstarttime + 1)
                            numdays_tvals = len(inrange_access_times)
                            #diff_days=numdays_cal - numdays_tvals
                            if numdays_cal == 366 and numdays_tvals == 365: 
                                print("adjusting for single leap year offset")
                                difftime = inrange_access_times[0] - newstarttime - 1
                            else: difftime = inrange_access_times[0] - newstarttime
                            tvals = np.array(inrange_access_times) - difftime
                        else: 
                            print("cannot manually create axis for this frequency, {opts['frequency']}")
                    #print tvals                    
                    #set refString to new value
                    refString = 'days since {r:04d}-01-01'.format(r=opts['reference_date'])
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
                        if opts['axes_modifier'].find('day2mon') != -1:
                            print("converting timevals from daily to monthly")
                            tvals,min_tvals,max_tvals = day2mon(tvals,opts['reference_date'])
                        elif opts['axes_modifier'].find('mon2yr') != -1:
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
                                if (os.path.basename(all_access_files[0]).startswith('ice')) or (dim.find('time1') != -1 or dim.find('time_0') != -1):
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
                                #if opts['axes_modifier'].find('tMonOverride') != -1:
                                if os.path.basename(all_access_files[0]).startswith('ice') or (dim.find('time1') != -1):
                                    #correct date to middle of month
                                    mid = (max_tvals[i] - min_tvals[i]) / 2.
                                    tvals[i] = min_tvals[i] + mid
                        else:
                            print("default time bounds")
                            if os.path.basename(all_access_files[0]).startswith('ice'):
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
                        if os.path.basename(all_access_files[0]).startswith('ice'):
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
                                    #if opts['axes_modifier'].find('tMonOverride') != -1:
                                    if os.path.basename(all_access_files[0]).startswith('ice'):
                                        #correct date to middle of month
                                        mid = (max_tvals[i] - min_tvals[i]) / 2.
                                        tvals[i] = min_tvals[i] + mid
                            else:    
                                tvals = tvals - 0.5
                        elif opts['mode'] == 'ccmi' and tvals[0].is_integer():
                            if opts['cmip_table'].find('A10day') == -1:
                                tvals = tvals - 0.5
                                print('inst time shifted back half a day for CMOR')
                        elif opts['axes_modifier'].find('yrpoint') != -1:
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
                elif (axis_name == 'Y')and opts['axes_modifier'].find('dropY') == -1:
                    if ((dim_vals == None) or (np.ndim(lat_vals) == 2 and
                         opts['axes_modifier'].find('dropX') == -1)):
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
                        if opts['axes_modifier'].find('gridlat') != -1:
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
                elif (axis_name == 'X') and opts['axes_modifier'].find('dropX') == -1:
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
                elif (axis_name == 'Z') and opts['axes_modifier'].find('dropZ') == -1:
                    z_len = len(dim_values)
                    units = dim_vals.units
                    #test different z axis names:
                    if opts['axes_modifier'].find('mod2plev19') != -1:
                        lev_name = 'plev19'
                        z_len = 19
                        units = 'Pa'
                        dim_values,dim_val_bounds = plev19()
                    elif (dim == 'st_ocean') or (dim == 'sw_ocean'):
                        if opts['axes_modifier'].find('depth100') != -1:
                            lev_name = 'depth100m'
                            dim_values = np.array([100])
                            dim_val_bounds = np.array([95,105])
                            z_len = 1
                        #ocean depth
                        else:
                            lev_name = 'depth_coord'
                        if opts['access_version'].find('OM2')!=-1 and dim == 'sw_ocean':
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
                                if opts['axes_modifier'].find('switchlevs') != -1:
                                    lev_name = 'hybrid_height_half'
                                a_vals,b_vals,dim_val_bounds,b_bounds = getHybridLevels('theta',85)
                                if opts['axes_modifier'].find('surfaceLevel') != -1:
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
                                if opts['axes_modifier'].find('switchlevs') != -1:
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
                                if opts['axes_modifier'].find('switchlevs') != -1:
                                    lev_name = 'hybrid_height_half'
                                a_vals,b_vals,dim_val_bounds,b_bounds = getHybridLevels('theta',38)
                                if opts['axes_modifier'].find('surfaceLevel') != -1:
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
                                if opts['axes_modifier'].find('switchlevs') != -1:
                                    lev_name='hybrid_height'
                                a_vals,b_vals,dim_val_bounds,b_bounds=getHybridLevels('rho',38)
                                if dim_values[0] == 1:
                                    dim_values=a_vals
                        else:
                            raise Exception(f"Unknown model levels starting at {dim_values[0]}")
                    elif (dim == 'lev' or dim.find('_p_level') != -1):
                        print(opts['cmip_table'])
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
                        print(opts['cmip_table'])
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
                        else: raise Exception(f"Z levels do not match known levels {dim}")
                        if opts['axes_modifier'].find('topsoil') != -1:
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
                    if opts['cmip_table'] == 'CMIP6_6hrLev' and lev_name.find('hybrid_height') == -1:
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
                    if dim.find('pseudo') != -1 and opts['axes_modifier'].find('dropLev') == -1:
                        print("variable on tiles, setting pseudo levels...")
                        #z_len=len(dim_values)
                        lev_name = opts['axes_modifier'].split()
                        try:
                            for i in lev_name:
                                if i.find('type') != -1:
                                    lev_name = i
                                else: pass
                            if lev_name.find('type') == -1:
                                raise Exception('could not determine land type')
                        except: raise Exception('could not determine land type, check variable dimensions and axes_modifiers')
                        landtype = det_landtype(lev_name)
                        cmor.set_table(tables[1])
                        #tiles=cableTiles()
                        axis_id = cmor.axis(table_entry=lev_name,
                                units='',
                                coord_vals=[landtype])
                        axis_ids.append(axis_id)
                    if dim.find('pseudo') != -1 and opts['axes_modifier'].find('landUse') != -1:
                        landUse = getlandUse()
                        z_len = len(landUse)
                        cmor.set_table(tables[1])
                        axis_id = cmor.axis(table_entry='landUse',
                                units='',
                             length=z_len,coord_vals=landUse)
                        axis_ids.append(axis_id)
                    if dim.find('pseudo') != -1 and opts['axes_modifier'].find('vegtype') != -1:
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
        #
        #If we are on a non-cartesian grid, Define the spatial grid
        #
        grid_id=None
        if i_axis_id != None and np.ndim(lon_vals) == 2:
            print("setting grid vertices...")
            #ensure longitudes are in the 0-360 range.
            if opts['access_version'] == 'OM2-025':
                print('1/4 degree grid')
                lon_vals_360 = np.mod(lon_vals[:],360)
                lon_vertices = np.ma.asarray(np.mod(get_vertices_025(lon_name),360)).filled()
                #lat_vals_360=np.mod(lat_vals[:],300)
                lat_vertices = np.ma.asarray(get_vertices_025(lat_name)).filled()
                #lat_vertices=np.mod(get_vertices_025(lat_name),300)
            else:
                lon_vals_360 = np.mod(lon_vals[:],360)
                lat_vertices = get_vertices(lat_name)
                lon_vertices = np.mod(get_vertices(lon_name),360)
            print(lat_name)
            #print(type(lat_vertices),lat_vertices[0])
            print(lon_name)
            #print(type(lon_vertices),lon_vertices[0])
            print(f"grid shape: {lat_vertices.shape} {lon_vertices.shape}")
            print("setup of vertices complete")
            try:
                #Set grid id and append to axis and z ids
                cmor.set_table(tables[0])
                grid_id = cmor.grid(axis_ids=np.array([j_axis_id,i_axis_id]),
                    latitude=lat_vals[:],
                    longitude=lon_vals_360[:],
                    latitude_vertices=lat_vertices[:],
                    longitude_vertices=lon_vertices[:])
                #replace i,j axis ids with the grid_id
                axis_ids.append(grid_id)
                z_ids.append(grid_id)
                print("setup of lat,lon grid complete")
            except Exception as e:
                print(f"E: We really should not be here! {e}")
    except Exception as e:
        print(f"E: We should not be here! {e}")
    #
    #create oline axis
    if opts['axes_modifier'].find('oline') != -1:
        print("creating oline axis...")
        lines = getTransportLines()
        cmor.set_table(tables[1])
        oline_axis_id = cmor.axis(table_entry='oline',
            units='',
            length=len(lines),
            coord_vals=lines)
        print("setup of oline axis complete")
        axis_ids.append(oline_axis_id)
    #create siline axis
    if opts['axes_modifier'].find('siline') != -1:
        print("creating siline axis...")
        lines = geticeTransportLines()
        cmor.set_table(tables[1])
        siline_axis_id = cmor.axis(table_entry='siline',
            units='',
            length=len(lines),
            coord_vals=lines)
        print("setup of siline axis complete")
        axis_ids.append(siline_axis_id)
    #create basin axis
    if opts['axes_modifier'].find('basin') != -1:
        print("creating basin axis...")
        cmor.set_table(tables[1])
        basins = np.array(['atlantic_arctic_ocean','indian_pacific_ocean','global_ocean'])
        oline_axis_id = cmor.axis(table_entry='basin',
            units='',
            length=len(basins),
            coord_vals=basins)
        print("setup of basin axis complete")
        axis_ids.append(oline_axis_id)
    #set up additional hybrid coordinate information
    if lev_name == 'hybrid_height':
        orog_vals = getOrog()
        zfactor_b_id = cmor.zfactor(zaxis_id=z_axis_id,
            zfactor_name='b',
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
    elif lev_name == 'hybrid_height_half':
        orog_vals = getOrog()
        zfactor_b_id = cmor.zfactor(zaxis_id=z_axis_id,
                zfactor_name='b_half',
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
    #
    #Define the CMOR variable.
    #
    cmor.set_table(tables[1])
    #
    #First try and get the units of the variable.
    #
    try:
        if opts['in_units'] == None or opts['in_units'] == '':
            try:
                in_units = data_vals.units
            except:    
                in_units = 1
        else:
            in_units = opts['in_units']
        try:
            in_missing = float(data_vals.missing_value)
        except:
            print("trying fillValue as missing value")
            try:
                in_missing = float(data_vals._FillValue)
            except: 
                    #print 'Warning!!! using default missing value of 1e20, may be incorrect!'
                    print("Warning!!! using default missing value of 9.96921e+36, may be incorrect!")
                    in_missing = float(9.96921e+36)
                    #in_missing=float(1e20)
        #
        #Now try and work out if there is a vertical direction associated with the variable
        #(for example radiation variables).
        #
        try:
            standard_name = data_vals.standard_name.lower()
        except:
            standard_name = 'None'
        print(f"cmor axis variables: {axis_ids}")
        #
        #Define the CMOR variable, taking account of possible direction information.
        #
        print("defining cmor variable...")
        try:    
            #set positive value from input variable attribute
            variable_id = cmor.variable(table_entry=opts['vcmip'],
                    units=in_units,
                    axis_ids=axis_ids,
                    # docs are using both type an data_type for the same argument
                    data_type='f',
                    missing_value=in_missing,
                    positive=data_vals.positive)
            print(f"positive: {data_vals.positive}")
        except:
            #search for positive attribute keyword in standard name / postive option
            if ((standard_name.find('up') != -1 or standard_name.find('outgoing') != -1 or 
                standard_name.find('out_of') != -1 or opts['positive'] == 'up')):
                variable_id = cmor.variable(table_entry=opts['vcmip'],
                        units=in_units, 
                        axis_ids=axis_ids,
                        # docs are using both type an data_type for the same argument
                        data_type='f',
                        missing_value=in_missing,
                        positive='up')
                print("positive: up")
            elif (standard_name.find('down') != -1 or standard_name.find('incoming') != -1 or \
                standard_name.find('into') != -1 or opts['positive'] == 'down'):
                variable_id = cmor.variable(table_entry=opts['vcmip'],
                        units=in_units,
                        axis_ids=axis_ids,
                        data_type='f',
                        missing_value=in_missing,
                        positive='down')
                print("positive: down")
            else:
                #don't assign positive attribute
                variable_id = cmor.variable(table_entry=opts['vcmip'],
                        units=in_units,
                        axis_ids=axis_ids,
                        data_type='f',
                        missing_value=in_missing)
                print("positive: None")
    except Exception as e:
        print(f"E: Unable to define the CMOR variable {e}")
        raise
    #
    #Close the ACCESS file.
    #
    access_file.close()
    print("closed input netCDF file")    
    #Loop over all the in time range ACCESS files, and process those which we need to.
    #
    print("writing data, and calculating if needed...")
    if opts['calculation'] == '':
        print("no calculation necessary")
    else:
        print(f"calculation: {opts['calculation']}")
    sys.stdout.flush()
    #
    #calculate time integral of the first variable (possibly adding a second variable to each time)
    #PP can't find time-integral anywhere in variable mapping, maybe is not needed?
    #
    if opts['axes_modifier'].find('time_integral') != -1:
        try:    
            run = np.float32(opts['calculation'])
        except:        
            run = np.float32(0.0)
        for input_file in inrange_access_files:
            #If the data is a climatology, store the values in a running sum
            access_file = xr.open_dataset(f'{input_file}', use_cftime=True)
            var = access_file[opts['vin'][0]]
            t = var.getTime()
            tbox = daysInMonth(t)
            varout = np.float32(var[:,0]*tbox[:]*24).cumsum(0) + run
            run = varout[-1]
            #if we have a second variable, just add this to the output (not included in the integration)
            if len(opts['vin']) == 2:
                varout += access_file[opts['vin'][1]][:]
            access_file.close()
            cmor.write(variable_id, (varout), ntimes_passed=np.shape(varout)[0])
    #
    #Monthly Climatology case
    # PP timeshot here means that car is either instantaneous, monthly or climatology value
    #
    elif opts['timeshot'].find('clim') != -1:
        for input_file in inrange_access_files:
            access_file = xr.open_dataset(f'{input_file}', use_cftime=True)
            t = access_file[opts['vin'][0]].getTime()
            #Set var to be sum of variables in 'vin' (can modify to use calculation if needed)
            var = None
            for v in opts['vin']:
                try:        
                    var += (access_file[v][:])
                    print("added extra variable")
                except:        
                    var = access_file[v][:]
            try: 
                vals_wsum,clim_days = monthClim(var,t,vals_wsum,clim_days)
            except:
                #first time
                tmp = var[0,:].shape
                out_shape = (12,) + tmp
                vals_wsum = np.ma.zeros(out_shape,dtype=np.float32)
                print(f"first time, data shape: {np.shape(vals_wsum)}")
                clim_days = np.zeros([12],dtype=int)#sum of number of days in the month
                vals_wsum,clim_days = monthClim(var,t,vals_wsum,clim_days)
            access_file.close()
        #calculate the climatological average for each month from the running sum (vals_wsum)
        #and the total number of days for each month (clim_days)
        for j in range(12):
            print(f"month: {j+1}, sum of days: {clim_days[j]}")
            #average vals_wsum using the total number of days summed for each month
            vals_wsum[j,:] = vals_wsum[j,:] / clim_days[j]
        cmor.write(variable_id, (vals_wsum), ntimes_passed=12)
    #
    #Annual means - Oyr / Eyr tables
    #
    elif (opts['axes_modifier'].find('mon2yr') != -1):
        access_file0 = xr.open_dataset(f'{inrange_access_files[0]}', use_cftime=True)
        #access_file0=netCDF4.Dataset(inrange_access_files[0])
        if opts['calculation'] == '':
            data_val0 = access_file0[opts['vin'][0]][:]
        else:
            data_val0 = calculateVals((access_file0,),opts['vin'],opts['calculation'])
        vshape = np.shape(data_val0)
        print(vshape)
        for year in range(startyear, endyear+1):
            print(f"processing year {year}")
            count = 0
            vsum = np.ma.zeros(vshape[1:],dtype=np.float32)
            for input_file in inrange_access_files:
                if opts['axes_modifier'].find('tMonOverride') != -1:
                    print("reading date info from file name")
                    if os.path.basename(input_file).startswith('ocean'):
                        yearstamp = int(os.path.basename(input_file).split('.')[1][3:7])
                    else:
                        if opts['access_version'].find('CM2') != -1:
                            yearstamp = int(os.path.basename(input_file).split('.')[1][2:6])
                        elif opts['access_version'].find('ESM') != -1:
                            yearstamp = int(os.path.basename(input_file).split('.')[1][3:7])
                    access_file = xr.open_dataset(f'{input_file}', use_cftime=True)
                    t = access_file[opts['vin'][0]].getTime()
                    datelist = t.asComponentTime()
                    if yearstamp == year: yearinside=True
                    else: yearinside = False
                else:
                    print('reading date info from time dimension')
                    access_file = xr.open_dataset(f'{input_file}', use_cftime=True)
                    t = access_file[opts['vin'][0]].getTime()
                    datelist = t.asComponentTime()
                    yearinside = False
                    for date in datelist:
                        if date.year == year: yearinside=True
                #try: print year, yearstamp, yearinside, input_file
                #except: print year, yearinside, input_file
                if yearinside:
                    print(f"found data in file {input_file}")
                    for index, d in enumerate(datelist[:]):
                        if (d.year == year) or (opts['axes_modifier'].find('tMonOverride') != -1):
                            if opts['calculation'] == '':
                                data_vals = access_file[opts['vin'][0]][:]
                            else:
                                data_vals = calculateVals((access_file,),opts['vin'],opts['calculation'])
                            try: 
                                data_vals = data_vals.filled(in_missing)
                            except: 
                                pass
                            print(f"shape: {np.shape(data_vals)}")
                            print(f"time index: {index}, date: {d}")
                            try: 
                                vsum += data_vals[index,:,:]
                                count += 1
                            except: 
                                vsum += data_vals[index,:,:,:]
                                count += 1
                access_file.close()
            if count == 12:
                vyr = vsum / 12
                print("writing with cmor...")
                cmor.write(variable_id, vyr.values, ntimes_passed=1)
            else:
                print(count)
                raise Exception(f'WARNING: annual data contains {count} months of data')    
    #
    #Annual point values - landUse variables
    #
    elif opts['axes_modifier'].find('yrpoint') != -1:
        for year in range(startyear,endyear+1):
            for input_file in inrange_access_files:
                if opts['access_version'].find('CM2') != -1:
                    yearstamp = int(os.path.basename(input_file).split('.')[1][2:6])
                    monstamp = int(os.path.basename(input_file).split('.')[1][6:8])
                elif opts['access_version'].find('ESM') != -1:
                    try:
                        yearstamp = int(os.path.basename(input_file).split('.')[1][3:7])
                        monstamp = int(os.path.basename(input_file).split('.')[1][8:10])
                    except:
                        yearstamp = int(os.path.basename(input_file).split('.')[1][3:7])
                        monstamp = int(os.path.basename(input_file).split('.')[1][7:9])
                if not monstamp == 12 or not yearstamp == year:
                    continue
                access_file = xr.open_dataset(f'{input_file}', use_cftime=True)
                t = access_file[opts['vin'][0]].getTime()
                datelist = t.asComponentTime()
                yearcurrent = datelist[0].year
                if yearcurrent == year:
                    print(f"processing year {year}, file {input_file}")
                    for index, d in enumerate(datelist[:]):
                        if d.month == 12:
                            if opts['calculation'] == '':
                                data_vals = access_file[opts['vin'][0]][:]
                            else:
                                data_vals = calculateVals((access_file,),opts['vin'],opts['calculation'])
                            try: data_vals = data_vals.filled(in_missing)
                            except:
                                #if values aren't in a masked array
                                pass 
                            print(f"shape: {np.shape(data_vals)}")
                            print(f"time index: {index}, date: {d}")
                        else: 
                            pass
                access_file.close()
            print("writing with cmor...")
            print(np.shape(data_vals))
            cmor.write(variable_id, data_vals[0,:,:,:], ntimes_passed=1)
    #
    #Aday10Pt processing for CCMI2022
    #
    elif opts['cmip_table'].find('A10dayPt') != -1:
        for i, input_file in enumerate(inrange_access_files):
            print(f"processing file: {input_file}")
            access_file = xr.open_dataset(f'{input_file}', use_cftime=True)
            t = access_file[opts['vin'][0]].getTime()
            datelist = t.asdatetime()
            print('ONLY 1st, 11th, 21st days to be used')
            a10_idxlist = []
            for idx, date in enumerate(datelist):
                if date.day in [1,11,21]: a10_idxlist.append(idx)
            print(a10_idxlist)
            a10_datavals = []
            try:
                if opts['calculation'] == '':
                    if len(opts['vin'])>1:
                        print("error: multiple input variables are given without a description of the calculation")
                        return -1
                    else: 
                        for a10 in a10_idxlist:
                            a10_datavals.append(access_file[opts['vin'][0]][a10])
                        access_file.close()
                else: 
                    print("calculating...")
                    data_vals = calculateVals((access_file,),opts['vin'],opts['calculation'])
                    for a10 in a10_idxlist:
                        a10_datavals.append(data_vals[a10])
                    try: a10_datavals = a10_datavals.filled(in_missing)
                    except:
                        #if values aren't in a masked array
                        pass 
                    access_file.close()
            except Exception as e:
                print(f"E: Unable to process data from {input_file} {e}")
                raise
            print("writing with cmor...")
            try:
                if time_dimension != None:
                    #assuming time is the first dimension
                    print(np.shape(a10_datavals))
                    cmor.write(variable_id, a10_datavals.values,
                        ntimes_passed=np.shape(a10_datavals)[0])
                else:
                    cmor.write(variable_id, a10_datavals.values, ntimes_passed=0)
            except Exception as e:
                print(f"E: Unable to write the CMOR variable to file {e}")
                raise
    #
    #Convert monthly integral to rate (e.g. K to K s-1, as in tntrl)
    #
    elif opts['axes_modifier'].find('monsecs') != -1:
        for i, input_file in enumerate(inrange_access_files):
            print(f"processing file: {input_file}")
            access_file = xr.open_dataset(f'{input_file}', use_cftime=True)
            t = access_file[opts['vin'][0]].getTime()
            datelist = t.asdatetime()
            #print(calendar.monthrange(datelist[0].year,datelist[0].month)[1])
            monsecs = calendar.monthrange(datelist[0].year,datelist[0].month)[1] * 86400
            try:
                if opts['calculation'] == '':
                    if len(opts['vin'])>1:
                        print("error: multiple input variables are given without a description of the calculation")
                        return -1
                    else: 
                        data_vals = access_file[opts['vin'][0]][:]
                        data_vals = data_vals / monsecs
                        #print data_vals
                        access_file.close()
                else:
                    print("calculating...")
                    data_vals = calculateVals((access_file,),opts['vin'],opts['calculation'])
                    data_vals = data_vals / monsecs
                    #convert mask to missing values
                    try: 
                        data_vals = data_vals.filled(in_missing)
                    except:
                        #if values aren't in a masked array
                        pass 
                    access_file.close()
            except Exception as e:
                print(f"E: Unable to process data from {input_file} {e}")
                raise
            print("writing with cmor...")
            try:
                if time_dimension != None:
                    #assuming time is the first dimension
                    print(np.shape(data_vals))
                    cmor.write(variable_id, data_vals.values,
                        ntimes_passed=np.shape(data_vals)[0])
                else:
                    cmor.write(variable_id, data_vals.values, ntimes_passed=0)
            except Exception as e:
                print(f"E: Unable to write the CMOR variable to file {e}")
                raise
    #
    #normal case
    #
    else:
        for i, input_file in enumerate(inrange_access_files):
            #
            #Load the ACCESS NetCDF data.
            #
            access_file = xr.open_dataset(f'{input_file}', use_cftime=True)
            #access_file=netCDF4.Dataset(input_file)
            print(f"processing file: {input_file}")
            try:
                if opts['calculation'] == '':
                    if len(opts['vin'])>1:
                        print("error: multiple input variables are given without a description of the calculation")
                        return -1
                    else: 
                        data_vals = access_file[opts['vin'][0]][:]
                        #print data_vals
                        access_file.close()
                else:
                    print("calculating...")
                    data_vals = calculateVals((access_file,),opts['vin'],opts['calculation'])
                    #convert mask to missing values
                    try: data_vals = data_vals.filled(in_missing)
                    except:
                        #if values aren't in a masked array
                        pass 
                    access_file.close()
                if opts['axes_modifier'].find('depth100') != -1:
                    data_vals = depth100(data_vals[:,9,:,:],data_vals[:,10,:,:])
            except Exception as e:
                print(f"E: Unable to process data from {input_file} {e}")
                raise
            #
            #If the data is not a climatology:
            #Write the data to the CMOR file.
            #
            else:
                try:
                    #print 'writing...'
                    print(f"started writing @ {timetime.time()-start_time}")
                    if time_dimension != None:
                        #assuming time is the first dimension
                        print(np.shape(data_vals))
                        cmor.write(variable_id, data_vals.values,
                            ntimes_passed=np.shape(data_vals)[0])
                    else:
                        cmor.write(variable_id, data_vals.values, ntimes_passed=0)
                    print(f"finished writing @ {timetime.time()-start_time}")
                except Exception as e:
                    print(f"E: Unable to write the CMOR variable to file {e}")
                    raise
    #
    #Close the CMOR file.
    #
    try:
        path = cmor.close(variable_id, file_name=True)
    except:
        print("E: We should not be here!")
        raise
    return path

#Read the command line, setting reasonable default values for most things.
parser=OptionParser()
parser.add_option('-i', '--infile', dest='infile', default='/short/p73/tph548/test_data/test_data.nc',
    help='Input file to process [default: %default]')
parser.add_option('--tstart', dest='tstart', default=None, type="int",
    help='Start time for data to process (units: years) [default: %default years]')
parser.add_option('--tend', dest='tend', default=None, type="int",
    help='End time for data to process (units: years) [default: %default years]')
parser.add_option('--vin', action="append",dest='vin', default=[],
    help='Name of the input variable to process [default: %default]')
parser.add_option('--vcmip', dest='vcmip', default='tos',
    help='Name of the CMIP-5 variable [default: %default]')
parser.add_option('--cmip_table_path', dest='cmip_table_path', default='./cmip-cmor-tables/Tables',
    help='Path to the directory where the CMIP tables are stored [default: %default]')
parser.add_option('--frequency', dest='frequency', default='mon',
    help='requested frequency of variable')
parser.add_option('--cmip_table', dest='cmip_table', default='CMIP6_Amon',
    help='Name of CMIP table to load [default: %default]')
#parser.add_option('--version_number', dest='version_number', default='unassigned',
#    help='ACCESS version number [default: %default]')
parser.add_option('--in_units', dest='in_units', default=None,
    help='Units of input variable [default: read from the units attribute]')
parser.add_option('--calculation', dest='calculation', default='',
    help='Calculation deriving the data values for the cmip variable from the input variables[default: %default]')
parser.add_option('--axes_modifier',dest='axes_modifier',default='',
    help='string defining commands to modify axes: possible values: \
    dropX ,dropY, dropZ, dropTiles (remove axis),\
    monClim (monthly climatological averages), time1 (time snapshots),\
    day2mon (convert time from daily to monthly,\
    basin (add axes for basins),\
    oline (add axis for ocean lines)')
parser.add_option('--positive',dest='positive',default='',
    help='string defining whether the variable has the positive attribute: possible values: up, down')
parser.add_option('--notes',dest='notes',default='',
    help='notes to be inserted directly into the netcdf file metadata')
parser.add_option('--json_file_path',dest='json_file_path',default='./input_file/access_cmip6.json',
    help='Path to cmor json file')
parser.add_option('--timeshot',dest='timeshot',default='mean',
    help='mean, inst, or clim - specified by the frequency column of the data request')
parser.add_option('--access_version',dest='access_version',default='CM2',
    help='CM2 or ESM')
parser.add_option('--reference_date',dest='reference_date',default='0001',
    help='The internally-consistent date that the experiment began')
parser.add_option('--mode',dest='mode',default='cmip6',
    help='CMIP6, CCMI2022, or custom mode')
parser.add_option('--exp_description',dest='exp_description',default='cmip6 standard experiment',
    help='Description of the experiment setup')
(options, args)=parser.parse_args()
opts=dict()
#produce a dictionary out of the options object
opts['tstart'] = options.tstart
opts['tend'] = options.tend
opts['cmip_table'] = options.cmip_table
#opts['version_number']=options.version_number
opts['infile'] = options.infile
opts['in_units'] = options.in_units
opts['vin'] = options.vin
opts['vcmip'] = options.vcmip
opts['cmip_table_path'] = options.cmip_table_path
opts['calculation'] = options.calculation
opts['axes_modifier'] = options.axes_modifier
opts['positive'] = options.positive
opts['notes']=options.notes
opts['json_file_path']=options.json_file_path
opts['timeshot']=options.timeshot
opts['access_version']=options.access_version
opts['reference_date']=options.reference_date
opts['frequency']=options.frequency
opts['mode']=options.mode
opts['exp_description']=options.exp_description

if __name__ == "__main__":
    app(opts)

