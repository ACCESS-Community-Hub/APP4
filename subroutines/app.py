# This is the ACCESS Post Processor.
#
# Originally written for CMIP5 by Peter Uhe
#
# Adapted for CMIP6 by Chloe Mackallah
# Version 3.4 March 2019
#
from optparse import OptionParser
import netCDF4
import numpy as np
import string
import glob
import datetime
import re
from app_functions import *
import os,sys
import cdms2
import cdtime
#import cmorx as cmor
import cmor
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
import time as timetime
import psutil

cmorlogs=os.environ.get('CMOR_LOGS')

#
#main function to post-process files
#
def app(option_dictionary):
    start_time=timetime.time()
    print '\nstarting main app function...'
    #check the options passed to the function:    
    len0=len(opts)
    opts.update(option_dictionary) 
    #overwrite default parameters with new parameters
    len1=len(opts)
    if(len0 != len1): 
        #new parameters don't match old ones 
        raise ValueError('Error: {} input parameters don\'t match valid variable names'.format(str(len1-len0)))
    #
    cdtime.DefaultCalendar=cdtime.GregorianCalendar
    #
    cmor.setup(inpath=opts['cmip6_table_path'],
        netcdf_file_action=cmor.CMOR_REPLACE_4,
        set_verbosity=cmor.CMOR_NORMAL,
        exit_control=cmor.CMOR_NORMAL,
        logfile='{}/log'.format(cmorlogs),create_subdirectories=1)
    #
    #Define the dataset.
    #
    cmor.dataset_json(opts['json_file_path'])
    #
    #Write a global variable called version_number which is used for CSIRO purposes.
    #
    #try:
    #    cmor.set_cur_dataset_attribute('version_number',opts['version_number'])
    #except:
    #    print 'E: Unable to add a global attribute called version_number'
    #    raise Exception('E: Unable to add a global attribute called version_number')
    #
    #Write a global variable called notes which is used for CSIRO purposes.
    #
    try:
        cmor.set_cur_dataset_attribute('notes',opts['notes'])
    except:
        print 'E: Unable to add a global attribute called notes'
        raise Exception('E: Unable to add a global attribute called notes')
    #
    #Load the CMIP tables into memory.
    #
    tables=[]
    tables.append(cmor.load_table('{}/CMIP6_grids.json'.format(opts['cmip6_table_path'])))
    tables.append(cmor.load_table('{}/{}.json'.format(opts['cmip6_table_path'],opts['cmip_table'])))
    #
    #Find all the ACCESS file names which match the "glob" pattern.
    #Sort the filenames, assuming that the sorted filenames will
    #be in chronological order because there is usually some sort of date
    #and/or time information in the filename.
    #
    print 'input file structure: {}'.format(opts['infile'])
    tmp=opts['infile'].split()
    #if there are two different files used make a list of extra access files
    if len(tmp)>1:
        extra_access_files=glob.glob(tmp[1])
        extra_access_files.sort()
        opts['infile']=tmp[0]
    else: extra_access_files=None
    #set normal set of files
    all_access_files=glob.glob(opts['infile'])
    all_access_files.sort()
    #hack to remove files not in time range
    tmp=[]
    for fn in all_access_files:
        if os.path.basename(fn).startswith('ice'):
            #tstamp=int(os.path.basename(fn).split('.')[1][0:3])
            tstamp=int(re.search("\d{4}",os.path.basename(fn)).group())
        elif os.path.basename(fn).startswith('ocean'):
            #tstamp=int(os.path.basename(fn).split('.')[1][3:6])
            tstamp=int(re.search("\d{4}",os.path.basename(fn)).group())
        else:
            if opts['access_version'].find('CM2') != -1:
                tstamp=int(os.path.basename(fn).split('.')[1][2:6])
            elif opts['access_version'].find('ESM') != -1:
                tstamp=int(os.path.basename(fn).split('.')[1][3:7])
            else:
                raise Exception('E: ACCESS_version not identified')
        if opts['tstart'] <= tstamp and tstamp <= opts['tend']:
            tmp.append(fn)
    all_access_files=tmp
    if extra_access_files != None:
        tmp=[]
        for fn in extra_access_files:
            tstamp=int(re.search("\d{4}",fn).group())
            if opts['tstart'] <= tstamp and tstamp <= opts['tend']:
                tmp.append(fn)
        extra_access_files=tmp
    del tmp
    print 'access files from: {} to {}'.format(os.path.basename(all_access_files[0]),os.path.basename(all_access_files[-1]))
    print 'first file: {}'.format(all_access_files[0])
    inrange_access_files=[]
    inrange_access_times=[]
    #
    #find file with first element of 'vin'
    i=0
    while True:
        try:
                        temp_file=netCDF4.Dataset(all_access_files[i],'r')
        except IndexError: 
            #gone through all files and haven't found variable
            raise Exception('Error! Variable missing from files: {}'.format(opts['vin'][0]))
        try:
            #see if the variable is in the file
            var=temp_file.variables[opts['vin'][0]]
            break
        except:
            #try next file
            temp_file.close()
            i+=1
            continue
    print 'using file: {}'.format(all_access_files[i])
    try:
        print "variable '{}' has units in ACCESS file: {}".format(opts['vin'][0],var.units)
    except:
        print "variable '{}' has no units listed in ACCESS file".format(opts['vin'][0])
    #
    time_dimension=None
    #find time info: time axis, reference time and set tstart and tend
    #    
    try:
        if  (opts['axes_modifier'].find('dropT') == -1) and (opts['cmip_table'].find('fx') == -1):
            #try to find and set the correct time axis:
            for var_dim in var.dimensions:
                if var_dim.find('time') != -1 or temp_file.variables[var_dim].axis == 'T':
                    time_dimension=var_dim
    except:
        #use the default time dimension 'time'
        pass 
    if opts['axes_modifier'].find('tMonOverride') != -1:
        #if we want to override dodgey units in the input files
        print 'overriding time axis...'
        refString="days since {r}-01-01".format(r=opts['reference_date'])
        time_dimension=None    
        inrange_access_files=all_access_files
    elif opts['cmip_table'].find('fx') != -1:
        print 'fx variable, no time axis'
        refString="days since {r}-01-01".format(r=opts['reference_date'])
        time_dimension=None    
        inrange_access_files=all_access_files
    else:
        try:
            #
            #refdate is the "reference date" used as an arbitray 0 point for dates. Common values for
            #refdate are 1 (0001-01-01) and 719 163 (1970-01-01), but other values are possible.
            #We cannot handle negative reference dates (ie. BC dates); maybe we will encounter these
            #in some climate runs?
            #
            time=temp_file.variables[time_dimension]
            refString=time.units
        except:
            refString="days since 0001-01-01"
            print 'W: Unable to extract a reference date: assuming it is 0001'
        temp_file.close()
        try:
            #set to very end of the year
            startyear=opts['tstart']
            endyear=opts['tend']
            opts['tstart']=cdtime.comptime(opts['tstart']).torel(refString,cdtime.DefaultCalendar).value
            if os.path.basename(all_access_files[0]).startswith('ice'):
                opts['tend']=cdtime.comptime(opts['tend']+1).torel(refString,cdtime.DefaultCalendar).value
            elif (time_dimension.find('time1') != -1 or time_dimension.find('time_0') != -1) \
                and (opts['frequency'].find('mon') != -1) and (opts['timeshot'].find('mean') != -1):
                opts['tend']=cdtime.comptime(opts['tend']+1).torel(refString,cdtime.DefaultCalendar).value
            else:
                opts['tend']=cdtime.comptime(opts['tend']+1).torel(refString,cdtime.DefaultCalendar).value-.01 
            print 'time start: {}'.format(opts['tstart'])
            print 'time end: {}'.format(opts['tend'])
        except Exception, e:
            print 'time range not correctly supplied. {}'.format(e)
    #
    #Now find all the ACCESS files in the desired time range (and neglect files outside this range).
    #
    print "loading files..."
    print 'time dimension: {}'.format(time_dimension)
    sys.stdout.flush()
    exit=False
    if time_dimension != None:
        for i, input_file in enumerate(all_access_files):
            try:
                access_file=netCDF4.Dataset(input_file,'r')
                #
                #Read the time information.
                #
                tvals=access_file.variables[time_dimension]
                #test each file to see if it contains time values within the time range from tstart to tend
                #if (opts['tend'] == None or float(tvals[0]) < float(opts['tend'])) and (opts['tstart'] == None or float(tvals[-1]) > float(opts['tstart'])):
                if (opts['tend'] == None or float(tvals[0]) <= float(opts['tend'])) and (opts['tstart'] == None or float(tvals[-1]) >= float(opts['tstart'])):
                    inrange_access_files.append(input_file)
                    if opts['axes_modifier'].find('firsttime') != -1:
                        #only take first time
                        print 'first time stamp used only'
                        inrange_access_times.append(tvals[0])
                        exit=True
                    else:
                        inrange_access_times.extend(tvals[:])
                #
                #Close the file.
                #
                access_file.close()
            except Exception, e:
                print 'Cannot open file: {}'.format(e)
            if exit:
                break
    else:
        #print all_access_files[0]
        #if we are using a time invariant parameter, just use a file with vin
        #inrange_access_files.append(all_access_files[i])
        inrange_access_files = [all_access_files[0]]
        #print inrange_access_files
    print 'number of files in time range: {}'.format(len(inrange_access_files))
    #check if the requested range is covered
    if inrange_access_files == []:
        print 'no data exists in the requested time range'
        return 0
    #
    #Load the first ACCESS NetCDF data file, and get the required information about the dimensions and so on.
    #
    access_file=netCDF4.Dataset(inrange_access_files[0],'r')
    print 'opened input netCDF file: {}'.format(inrange_access_files[0])
    print 'checking axes...'
    sys.stdout.flush()
    try:
        #
        #Determine which axes are X and Y, and what the values of the latitudes and longitudes are.
        #
        try:
            data_vals=access_file.variables[opts['vin'][0]]
            print 'shape of data: {}'.format(np.shape(data_vals))
        except Exception, e:
            print 'E: Unable to read {} from ACCESS file'.format(opts['vin'][0])
            raise
        try:
            coord_vals=string.split(data_vals.coordinates)
            coord_vals.extend(data_vals.dimensions)
        except:
            coord_vals=data_vals.dimensions
        lon_vals=None
        lat_vals=None
        lon_name=None
        lat_name=None
        #search for strings 'lat' or 'lon' in co-ordinate names
        print coord_vals
        for coord in coord_vals:
            if coord.lower().find('lon') != -1:
                print(coord)
                lon_name=coord
                try:
                    lon_vals=access_file.variables[coord]
                except:
                    if os.path.basename(inrange_access_files[0]).startswith('ocean'):
                        if opts['access_version'] == 'OM2-025':
                            acnfile=ancillary_path+'grid_spec.auscom.20150514.nc'
                            acndata=netCDF4.Dataset(acnfile,'r')
                            lon_vals=acndata.variables['geolon_t']
                        else:
                            acnfile=ancillary_path+'grid_spec.auscom.20110618.nc'
                            acndata=netCDF4.Dataset(acnfile,'r')
                            lon_vals=acndata.variables['x_T']
                    if os.path.basename(inrange_access_files[0]).startswith('ice'):
                        if opts['access_version'] == 'OM2-025':
                            acnfile=ancillary_path+'cice_grid_20150514.nc'
                        else:
                            acnfile=ancillary_path+'cice_grid_20101208.nc'
                        acndata=netCDF4.Dataset(acnfile,'r')
                        lon_vals=acndata.variables[coord]
            elif coord.lower().find('lat') != -1:
                print coord
                lat_name=coord
                try:
                    lat_vals=access_file.variables[coord]
                    print('lat from file')
                except:
                    print('lat from ancil')
                    if os.path.basename(inrange_access_files[0]).startswith('ocean'):
                        if opts['access_version'] == 'OM2-025':
                            acnfile=ancillary_path+'grid_spec.auscom.20150514.nc'
                            acndata=netCDF4.Dataset(acnfile,'r')
                            lat_vals=acndata.variables['geolat_t']
                        else:
                            acnfile=ancillary_path+'grid_spec.auscom.20110618.nc'
                            acndata=netCDF4.Dataset(acnfile,'r')
                            lat_vals=acndata.variables['y_T']
                    if os.path.basename(inrange_access_files[0]).startswith('ice'):
                        if opts['access_version'] == 'OM2-025':
                            acnfile=ancillary_path+'cice_grid_20150514.nc'
                        else:
                            acnfile=ancillary_path+'cice_grid_20101208.nc'
                        acndata=netCDF4.Dataset(acnfile,'r')
                        lat_vals=acndata.variables[coord]
        #create a list of dimensions
        dim_list=data_vals.dimensions
        #
        #Work out which dimension(s) are associated with each coordinate variable.
        #
        axis_ids=[]
        z_ids=[]
        time_axis_id=None
        z_axis_id=None    
        j_axis_id=None
        i_axis_id=None    
        n_grid_pts=1
        lev_name=None
        z_len=0
        print 'list of dimensions: {}'.format(dim_list)
        for dim in dim_list:
            try:
                dim_vals=access_file.variables[dim]
                dim_values=dim_vals[:]
            except:
                #
                #The coordinate variable associated with "dim" was not found.
                #
                print 'W: No coordinate variable associated with the dimension {}'.format(dim)
                dim_vals=None
                #This should work. Look out for 'if dim_vals=None' and other use of dim_values, e.g. dim_values[:]
                dim_values=access_file.dimensions[dim]
            #
            #See if this dimension represents a spatial or temporal axis (X, Y, T and so on)
            #This information will either be stored in the axis or cartesian_axis variable attribute.
            #
            try:
                axis_name=dim_vals.axis
            except:
                try:
                    axis_name=dim_vals.cartesian_axis
                except:
                    #
                    #Try and guess the axis name from the dimension name.
                    #
                    if dim == 'ni':
                        axis_name='X'
                    elif dim == 'nj':
                        axis_name='Y'
                    elif dim == time_dimension:
                        axis_name='T'
                    else:
                        axis_name='unknown'
            print 'evaluating axis: {}'.format(axis_name)
            try:
                #
                #Try and get the dimension bounds. The bounds attribute might also be called "edges"
                #If we cannot find any dimension bounds, create default bounds in a sensible way.
                #
                dim_val_bounds=access_file.variables[dim_vals.bounds]
                print 'using dimension bounds'
            except:
                try:
                    dim_val_bounds=access_file.variables[dim_vals.edges]
                    print 'using dimension edges as bounds'
                except:
                    #
                    #Unable to get the dimension bounds, create some default bounds.
                    #The default bounds assume that the grid cells are centred on
                    #each grid point specified by the coordinate variable.
                    #
                    print 'I: No bounds for {} - creating default bounds'.format(dim)
                    if dim_vals == None:
                        print 'No dimension values'
                    else:
                        try:
                            min_vals=np.append((1.5*dim_values[0] - 0.5*dim_values[1]),(dim_values[0:-1] + dim_values[1:])/2)
                            max_vals=np.append((dim_values[0:-1] + dim_values[1:])/2,(1.5*dim_values[-1] - 0.5*dim_values[-2]))
                        except Exception, e:
                            print 'WARNING: dodgy bounds for dimension: {}'.format(dim)
                            print 'error: {}'.format(e)
                            min_vals=dim_values[:]-15
                            max_vals=dim_values[:]+15
                        dim_val_bounds=np.column_stack((min_vals,max_vals))
            print 'handling different axes types'
            try:
                if (axis_name == 'T') and (opts['axes_modifier'].find('dropT') == -1) and (opts['cmip_table'].find('fx') == -1):
                    #
                    #Set cmor time variable name: 
                    #For mean values this is just 'time'
                    #for synoptic snapshots, this is 'time1'
                    #for climatoloies this is time2
                    #
                    print 'dimension: {}'.format(dim)
                    if opts['timeshot'].find('mean') != -1:
                        cmor_tName='time'
                    elif opts['timeshot'].find('inst') != -1:
                        cmor_tName='time1'
                    elif opts['timeshot'].find('clim') != -1:
                        cmor_tName='time2'
                    else:
                        #assume timeshot is mean
                        print 'timeshot unknown or incorrectly specified'
                        cmor_tName='time'
                    #initialise stuff:
                    min_tvals=[]
                    max_tvals=[]
                    if opts['axes_modifier'].find('tMonOverride') == -1:
                        #convert times to days since reference_date
                        tvals=np.array(inrange_access_times) + cdtime.reltime(0,refString).torel('days since {r}-01-01'.format(r=opts['reference_date']),cdtime.DefaultCalendar).value
                        print 'time values converted to days since 01,01,{r}: {a}...{b}'.format(r=opts['reference_date'],a=tvals[0:5],b=tvals[-5:-1])
                    else:
                        #manually create time axis
                        tvals=[]
                        for year in range(opts['tstart'],opts['tend']+1):
                            for mon in range(1,13):
                                tvals.append(cdtime.comptime(year,mon,15).torel(refString,cdtime.DefaultCalendar).value)
                    #set refString to new value
                    refString='days since {r}-01-01'.format(r=opts['reference_date'])
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
                        print 'total time steps: {}'.format(len(tvals))
                        if opts['axes_modifier'].find('day2mon') != -1:
                            print 'converting timevals from daily to monthly'
                            tvals,min_tvals,max_tvals=day2mon(tvals,opts['reference_date'])
                        elif opts['axes_modifier'].find('mon2yr') != -1:
                            print 'converting timevals from monthly to annual'
                            tvals,min_tvals,max_tvals=mon2yr(tvals,refString)
                        elif (len(tvals) <= 1) or (((tvals[1]-tvals[0]) >= 28) and ((tvals[1]-tvals[0]) <= 31)):
                            print 'monthly time bounds'
                            for i,ordinaldate in enumerate(tvals):
                                if (os.path.basename(all_access_files[0]).startswith('ice')) or (dim.find('time1') != -1 or dim.find('time_0') != -1):
                                    ordinaldate=ordinaldate-0.5
                                model_date=cdtime.reltime(int(ordinaldate),refString).tocomp(cdtime.DefaultCalendar)
                                #min bound is first day of month
                                model_date.day=1
                                min_tvals.append(model_date.torel(refString,cdtime.DefaultCalendar).value)
                                #max_bound is first day of next month
                                model_date.year=model_date.year+model_date.month/12
                                model_date.month=model_date.month%12+1                                
                                max_tvals.append(model_date.torel(refString,cdtime.DefaultCalendar).value)
                                #if opts['axes_modifier'].find('tMonOverride') != -1:
                                if os.path.basename(all_access_files[0]).startswith('ice') or (dim.find('time1') != -1):
                                    #correct date to middle of month
                                    mid=(max_tvals[i]-min_tvals[i])/2.
                                    tvals[i]=min_tvals[i]+mid
                        else:
                            print 'default time bounds'
                            if os.path.basename(all_access_files[0]).startswith('ice'):
                                tvals=tvals-0.5
                            try:
                                min_tvals=np.append(1.5*tvals[0] - 0.5*tvals[1],(tvals[0:-1] + tvals[1:])/2)
                                max_tvals=np.append((tvals[0:-1] + tvals[1:])/2,(1.5*tvals[-1] - 0.5*tvals[-2]))
                            except:
                                print 'E: Unable to compute time bounds'
                                raise Exception('unable to compute time bounds')
                        tval_bounds=np.column_stack((min_tvals,max_tvals))
                        #set up time axis:
                        cmor.set_table(tables[1])
                        time_axis_id=cmor.axis(table_entry=cmor_tName,
                            units=refString,length=len(tvals),
                            coord_vals=tvals[:],cell_bounds=tval_bounds[:],
                            interval=None)
                        axis_ids.append(time_axis_id)
                        print 'setup of time dimension complete'
                    elif cmor_tName == 'time1':
                        #we are using time snapshots 
                        #set up time axis without cell_bounds
                        if os.path.basename(all_access_files[0]).startswith('ice'):
                            if (len(tvals) <= 1) or (((tvals[1]-tvals[0]) >= 28) and ((tvals[1]-tvals[0]) <= 31)):
                                print 'monthly time bounds'
                                for i,ordinaldate in enumerate(tvals):
                                    ordinaldate=ordinaldate-0.5
                                    model_date=cdtime.reltime(int(ordinaldate),refString).tocomp(cdtime.DefaultCalendar)
                                    #min bound is first day of month
                                    model_date.day=1
                                    min_tvals.append(model_date.torel(refString,cdtime.DefaultCalendar).value)
                                    #max_bound is first day of next month
                                    model_date.year=model_date.year+model_date.month/12
                                    model_date.month=model_date.month%12+1                                
                                    max_tvals.append(model_date.torel(refString,cdtime.DefaultCalendar).value)
                                    #if opts['axes_modifier'].find('tMonOverride') != -1:
                                    if os.path.basename(all_access_files[0]).startswith('ice'):
                                        #correct date to middle of month
                                        mid=(max_tvals[i]-min_tvals[i])/2.
                                        tvals[i]=min_tvals[i]+mid
                            else:    
                                tvals=tvals-0.5
                        elif opts['axes_modifier'].find('yrpoint') != -1:
                            print 'converting timevals from monthly to end of year'
                            tvals,min_tvals,max_tvals=yrpoint(tvals,refString) 
                        cmor.set_table(tables[1])
                        time_axis_id=cmor.axis(table_entry=cmor_tName,
                            units=refString,length=len(tvals),
                            coord_vals=tvals[:],
                            interval=None)
                        axis_ids.append(time_axis_id)
                        print 'setup of time dimension complete - W: no cell bounds'
                    elif cmor_tName == 'time2':
                        #compute start and end bounds of whole time region
                        tstarts=[]
                        tends=[]
                        tmids=[]
                        for n in range(12):
                            tstart=int(tvals[0])
                            tend=int(tvals[-1])
                            tstart=cdtime.reltime(tstart,refString).tocomp(cdtime.DefaultCalendar)
                            tstart.day=1
                            tstart.month=n+1
                            tstart=tstart.torel(refString,cdtime.DefaultCalendar).value
                            tstarts.append(tstart)
                            tend=cdtime.reltime(tend,refString).tocomp(cdtime.DefaultCalendar)
                            tend.month=n+1
                            tend=tend.add(1,cdtime.Month)
                            tend=tend.add(-1,cdtime.Day)
                            tend=tend.torel(refString,cdtime.DefaultCalendar).value
                            tends.append(tend)
                            tmid=tstart+(tend-tstart)/2
                            tmids.append(tmid)
                        tval_bounds=np.column_stack((tstarts,tends))
                        tvals=tmids
                        cmor.set_table(tables[1])
                        time_axis_id=cmor.axis(table_entry=cmor_tName,
                            units=refString,length=len(tvals),
                            coord_vals=tvals[:],cell_bounds=tval_bounds[:],
                            interval=None)
                        axis_ids.append(time_axis_id)
                        print 'setup of climatology time dimension complete'
                    else: raise Exception('Dont know how to compute time bounds for time axis {}'.format(cmor_tName))
                elif (axis_name == 'Y')and opts['axes_modifier'].find('dropY') == -1:
                    if (dim_vals == None) or (np.ndim(lat_vals) == 2 and opts['axes_modifier'].find('dropX') == -1):# and (opts['axes_modifier'].find('') != -1):
                        #grid co-ordinates
                        cmor.set_table(tables[0])
                        j_axis_id=cmor.axis(table_entry='j_index',units='1',
                            coord_vals=np.arange(len(dim_values)))
                    else:
                        #lat values
                        #force the bounds back to the poles if necessary
                        if dim_val_bounds[0,0]<-90.0:
                            dim_val_bounds[0,0]=-90.0
                            print 'setting minimum latitude bound to -90'
                        if dim_val_bounds[-1,-1]>90.0:
                            dim_val_bounds[-1,-1]=90.0
                            print 'setting maximum latitude bound to 90'
                        cmor.set_table(tables[1])
                        if opts['axes_modifier'].find('gridlat') != -1:
                            j_axis_id=cmor.axis(table_entry='gridlatitude',
                                units=dim_vals.units,length=len(dim_values),
                                coord_vals=np.array(dim_values),
                                cell_bounds=dim_val_bounds[:])
                        else:
                            j_axis_id=cmor.axis(table_entry='latitude',
                                units=dim_vals.units,length=len(dim_values),
                                coord_vals=np.array(dim_values),
                                cell_bounds=dim_val_bounds[:])
                        n_grid_pts=n_grid_pts * len(dim_values)
                        axis_ids.append(j_axis_id)
                        z_ids.append(j_axis_id)
                    print 'setup of latitude dimension complete'
                elif (axis_name == 'X') and opts['axes_modifier'].find('dropX') == -1:
                    if dim_vals == None or np.ndim(lon_vals) == 2:
                        #grid co-ordinates
                        cmor.set_table(tables[0])
                        i_axis_id=cmor.axis(table_entry='i_index',units='1',
                            coord_vals=np.arange(len(dim_values)))
                        n_grid_pts=len(dim_values)
                    else:
                        #lon values
                        cmor.set_table(tables[1])
                        i_axis_id=cmor.axis(table_entry='longitude',
                            units=dim_vals.units,length=len(dim_values),
                            coord_vals=np.mod(dim_values,360),
                            cell_bounds=dim_val_bounds[:])
                        n_grid_pts=n_grid_pts * len(dim_values)
                        axis_ids.append(i_axis_id)
                        z_ids.append(i_axis_id)
                    print 'setup of longitude dimension complete'    
                elif (axis_name == 'Z') and opts['axes_modifier'].find('dropZ') == -1:
                    z_len=len(dim_values)
                    units=dim_vals.units
                    #test different z axis names:
                    if opts['axes_modifier'].find('mod2plev19') != -1:
                        lev_name='plev19'
                        z_len=19
                        units='Pa'
                        dim_values,dim_val_bounds=plev19()
                    elif (dim == 'st_ocean') or (dim == 'sw_ocean'):
                        if opts['axes_modifier'].find('depth100') != -1:
                            lev_name='depth100m'
                            dim_values=np.array([100])
                            dim_val_bounds=np.array([95,105])
                            z_len=1
                        #ocean depth
                        else:
                            lev_name='depth_coord'
                        if opts['access_version'] == 'OM2-025' and dim == 'sw_ocean':
                            dim_val_bounds=dim_val_bounds[:]
                            dim_val_bounds[-1]=dim_values[-1]
                    elif dim == 'potrho':
                        #ocean pressure levels
                        lev_name='rho'
                    elif (dim.find('hybrid') != -1) or (dim == 'model_level_number') \
                        or (dim.find('theta_level') != -1) or (dim.find('rho_level') != -1):
                        ulev=0.0001
                        units='m'
                        a_theta_85,b_theta_85,dim_val_bounds_theta_85,b_bounds_theta_85=getHybridLevels('theta',85)
                        a_rho_85,b_rho_85,dim_val_bounds_rho_85,b_bounds_rho_85=getHybridLevels('rho',85)
                        a_theta_38,b_theta_38,dim_val_bounds_theta_38,b_bounds_theta_38=getHybridLevels('theta',38)
                        a_rho_38,b_rho_38,dim_val_bounds_rho_38,b_bounds_rho_38=getHybridLevels('rho',38)
                        if z_len == 85:
                            if (a_theta_85[0]-ulev <= dim_values[0] <= a_theta_85[0]+ulev)\
                                    or (dim == 'model_level_number') or (dim.find('theta_level') != -1):
                                print '85 atmosphere hybrid height theta (full) levels'
                                #theta levels
                                lev_name='hybrid_height'
                                if opts['axes_modifier'].find('switchlevs') != -1:
                                    lev_name='hybrid_height_half'
                                a_vals,b_vals,dim_val_bounds,b_bounds=getHybridLevels('theta',85)
                                if opts['axes_modifier'].find('surfaceLevel') != -1:
                                    print 'surface level only'
                                    #take only the first level    
                                    a_vals=a_vals[0:1]
                                    b_vals=b_vals[0:1]
                                    z_len=1
                                if dim_values[0] == 1:
                                    dim_values=a_vals
                            elif (a_rho_85[0]-ulev <= dim_values[0] <= a_rho_85[0]+ulev)\
                                    or (dim.find('rho_level') != -1):
                                print '85 atmosphere hybrid height rho (half) levels'
                                #rho levels
                                lev_name='hybrid_height_half'
                                if opts['axes_modifier'].find('switchlevs') != -1:
                                    lev_name='hybrid_height'
                                a_vals,b_vals,dim_val_bounds,b_bounds=getHybridLevels('rho',85)
                                if dim_values[0] == 1:
                                    dim_values=a_vals
                        elif z_len == 38:
                            if (a_theta_38[0]-ulev <= dim_values[0] <= a_theta_38[0]+ulev)\
                                    or (dim == 'model_level_number') or (dim.find('theta_level') != -1):
                                print '38 atmosphere hybrid height theta (full) levels'
                                #theta levels
                                lev_name='hybrid_height'
                                if opts['axes_modifier'].find('switchlevs') != -1:
                                    lev_name='hybrid_height_half'
                                a_vals,b_vals,dim_val_bounds,b_bounds=getHybridLevels('theta',38)
                                if opts['axes_modifier'].find('surfaceLevel') != -1:
                                    print 'surface level only'
                                    #take only the first level    
                                    a_vals=a_vals[0:1]
                                    b_vals=b_vals[0:1]
                                    z_len=1
                                if dim_values[0] == 1:
                                    dim_values=a_vals
                            elif (a_rho_38[0]-ulev <= dim_values[0] <= a_rho_38[0]+ulev)\
                                    or (dim.find('rho_level') != -1):
                                print '38 atmosphere hybrid height rho (half) levels'
                                #rho levels
                                lev_name='hybrid_height_half'
                                if opts['axes_modifier'].find('switchlevs') != -1:
                                    lev_name='hybrid_height'
                                a_vals,b_vals,dim_val_bounds,b_bounds=getHybridLevels('rho',38)
                                if dim_values[0] == 1:
                                    dim_values=a_vals
                        else:
                            raise Exception('Unknown model levels starting at {}'.format(dim_values[0]))
                    elif (dim == 'lev' or dim.find('_p_level') != -1):
                        print opts['cmip_table']
                        print 'dim = ',dim
                        #atmospheric pressure levels:
                        if z_len == 8:
                            lev_name='plev8'
                        elif z_len == 3:
                            lev_name='plev3'
                        elif z_len == 19:
                            lev_name='plev19'
                        else: raise Exception('Z levels do not match known levels {}'.format(dim))
                    elif dim.find('pressure') != -1:
                        print opts['cmip_table']
                        print 'dim = ',dim
                        #atmospheric pressure levels:
                        if z_len == 8:
                            lev_name='plev8'
                        elif z_len == 3:
                            lev_name='plev3'
                        elif z_len == 19:
                            lev_name='plev19'
                        else: raise Exception('Z levels do not match known levels {}'.format(dim))
                    elif (dim.find('soil') != -1) or (dim == 'depth'):
                        units='m'
                        if z_len == 4:
                            dim_values,dim_val_bounds=mosesSoilLevels()
                        elif z_len == 6:
                            dim_values,dim_val_bounds=cableSoilLevels()
                        else: raise Exception('Z levels do not match known levels {}'.format(dim))
                        if opts['axes_modifier'].find('topsoil') != -1:
                            #top layer of soil only
                            lev_name='sdepth1'
                            dim_values=dim_values[0:1]
                            dim_values[0]=0.05
                            dim_val_bounds=dim_val_bounds[0:1]
                            dim_val_bounds[0][1]=0.1
                        else:
                            #soil depth levels
                            lev_name='sdepth'
                    else:
                        raise Exception('Unknown z axis {}'.format(dim))
                    if opts['cmip_table'] == 'CMIP6_6hrLev' and lev_name.find('hybrid_height') == -1:
                        raise Exception('Variable on pressure levels instead of model levels. Exiting')
                    print 'lev_name = {}'.format(lev_name)
                    cmor.set_table(tables[1])
                    z_axis_id=cmor.axis(table_entry=lev_name,
                        units=units,length=z_len,
                        coord_vals=dim_values[:],cell_bounds=dim_val_bounds[:])        
                    axis_ids.append(z_axis_id)
                    print 'setup of height dimension complete'
                else: 
                    #coordinates with axis_identifier other than X,Y,Z,T
                    if dim.find('pseudo') != -1 and opts['axes_modifier'].find('dropLev') == -1:
                        print 'variable on tiles, setting pseudo levels...'
                        #z_len=len(dim_values)
                        lev_name=opts['axes_modifier'].split()
                        try:
                            for i in lev_name:
                                if i.find('type') != -1:
                                    lev_name=i
                                else: pass
                            if lev_name.find('type') == -1:
                                raise Exception('could not determine land type')
                        except: raise Exception('could not determine land type, check variable dimensions and axes_modifiers')
                        landtype=det_landtype(lev_name)
                        cmor.set_table(tables[1])
                        #tiles=cableTiles()
                        axis_id=cmor.axis(table_entry=lev_name,units='',
                            coord_vals=[landtype])
                        axis_ids.append(axis_id)
                    if dim.find('pseudo') != -1 and opts['axes_modifier'].find('landUse') != -1:
                        landUse=getlandUse()
                        z_len=len(landUse)
                        cmor.set_table(tables[1])
                        axis_id=cmor.axis(table_entry='landUse',units='',
                            length=z_len,coord_vals=landUse)
                        axis_ids.append(axis_id)
                    if dim.find('pseudo') != -1 and opts['axes_modifier'].find('vegtype') != -1:
                        cabletiles=cableTiles()
                        z_len=len(cabletiles)
                        cmor.set_table(tables[1])
                        axis_id=cmor.axis(table_entry='vegtype',units='',
                            length=z_len,coord_vals=cabletiles)
                        axis_ids.append(axis_id)
                    else:
                        print 'Unidentified cartesian axis: {}'.format(axis_name)
            except Exception, e:
                print 'Exception: {}'.format(e)
                print 'Error setting dimension: {}'.format(dim)
                raise e
        #
        #If we are on a non-cartesian grid, Define the spatial grid
        #
        grid_id=None
        if i_axis_id != None and np.ndim(lon_vals) == 2:
            print 'setting grid vertices...'
            #ensure longitudes are in the 0-360 range.
            if opts['access_version'] == 'OM2-025':
                print('1/4 degree grid')
                lon_vals_360=np.mod(lon_vals[:],360)
                lon_vertices=np.ma.asarray(np.mod(get_vertices_025(lon_name),360)).filled()
                #lat_vals_360=np.mod(lat_vals[:],300)
                lat_vertices=np.ma.asarray(get_vertices_025(lat_name)).filled()
                #lat_vertices=np.mod(get_vertices_025(lat_name),300)
            else:
                lon_vals_360=np.mod(lon_vals[:],360)
                lat_vertices=get_vertices(lat_name)
                lon_vertices=np.mod(get_vertices(lon_name),360)
            print(lat_name)
            #print(type(lat_vertices),lat_vertices[0])
            print(lon_name)
            #print(type(lon_vertices),lon_vertices[0])
            print 'grid shape: {} {}'.format(lat_vertices.shape,lon_vertices.shape)
            print 'setup of vertices complete'
            try:
                #Set grid id and append to axis and z ids
                cmor.set_table(tables[0])
                grid_id=cmor.grid(axis_ids=np.array([j_axis_id,i_axis_id]),
                    latitude=lat_vals[:],longitude=lon_vals_360[:],latitude_vertices=lat_vertices[:],
                    longitude_vertices=lon_vertices[:])
                #replace i,j axis ids with the grid_id
                axis_ids.append(grid_id)
                z_ids.append(grid_id)
                print 'setup of lat,lon grid complete'
            except Exception, e:
                print 'E: We really should not be here! {}'.format(e)
    except Exception, e:
        print 'E: We should not be here!'
        raise e
    #
    #create oline axis
    if opts['axes_modifier'].find('oline') != -1:
        print 'creating oline axis...'
        lines=getTransportLines()
        cmor.set_table(tables[1])
        oline_axis_id=cmor.axis(table_entry='oline',
            units='',length=len(lines),
            coord_vals=lines)
        print 'setup of oline axis complete'
        axis_ids.append(oline_axis_id)
    #create siline axis
    if opts['axes_modifier'].find('siline') != -1:
        print 'creating siline axis...'
        lines=geticeTransportLines()
        cmor.set_table(tables[1])
        siline_axis_id=cmor.axis(table_entry='siline',
            units='',length=len(lines),
            coord_vals=lines)
        print 'setup of siline axis complete'
        axis_ids.append(siline_axis_id)
    #create basin axis
    if opts['axes_modifier'].find('basin') != -1:
        print 'creating basin axis...'
        cmor.set_table(tables[1])
        basins=np.array(['atlantic_arctic_ocean','indian_pacific_ocean','global_ocean'])
        oline_axis_id=cmor.axis(table_entry='basin',
            units='',length=len(basins),
            coord_vals=basins)
        print 'setup of basin axis complete'
        axis_ids.append(oline_axis_id)
    #set up additional hybrid coordinate information
    if lev_name == 'hybrid_height':
        orog_vals=getOrog()
        zfactor_b_id=cmor.zfactor(zaxis_id=z_axis_id,zfactor_name='b',
            axis_ids=z_axis_id,units='1',type='d',zfactor_values=b_vals,
            zfactor_bounds=b_bounds)
        zfactor_orog_id=cmor.zfactor(zaxis_id=z_axis_id,zfactor_name='orog',
            axis_ids=z_ids,units='m',type='f',zfactor_values=orog_vals)
    elif lev_name == 'hybrid_height_half':
        orog_vals=getOrog()
        zfactor_b_id=cmor.zfactor(zaxis_id=z_axis_id,zfactor_name='b_half',
            axis_ids=z_axis_id,units='1',type='d',zfactor_values=b_vals,
            zfactor_bounds=b_bounds)
        zfactor_orog_id=cmor.zfactor(zaxis_id=z_axis_id,zfactor_name='orog',
            axis_ids=z_ids,units='m',type='f',zfactor_values=orog_vals)
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
                in_units=data_vals.units
            except:    
                in_units=1
        else:
            in_units=opts['in_units']
        try:
            in_missing=float(data_vals.missing_value)
        except:
            print 'trying fillValue as missing value'
            try:
                in_missing=float(data_vals._FillValue)
            except: 
                    #print 'Warning!!! using default missing value of 1e20, may be incorrect!'
                    print 'Warning!!! using default missing value of 9.96921e+36, may be incorrect!'
                    in_missing=float(9.96921e+36)
                    #in_missing=float(1e20)
        #
        #Now try and work out if there is a vertical direction associated with the variable
        #(for example radiation variables).
        #
        try:
            standard_name=data_vals.standard_name.lower()
        except:
            standard_name='None'
        print 'cmor axis variables: {}'.format(axis_ids)
        #
        #Define the CMOR variable, taking account of possible direction information.
        #
        print 'defining cmor variable...'
        try:    
            #set positive value from input variable attribute
            variable_id=cmor.variable(table_entry=opts['vcmip'],units=in_units, \
            axis_ids=axis_ids,type='f',missing_value=in_missing,positive=data_vals.positive)
            print 'positive: {}'.format(data_vals.positive)
        except:
            #search for positive attribute keyword in standard name / postive option
            if (standard_name.find('up') != -1 or standard_name.find('outgoing') != -1 or \
                standard_name.find('out_of') != -1 or opts['positive'] == 'up'):
                variable_id=cmor.variable(table_entry=opts['vcmip'],units=in_units, \
                axis_ids=axis_ids,type='f',missing_value=in_missing,positive='up')
                print 'positive: up'
            elif (standard_name.find('down') != -1 or standard_name.find('incoming') != -1 or \
                standard_name.find('into') != -1 or opts['positive'] == 'down'):
                variable_id=cmor.variable(table_entry=opts['vcmip'],units=in_units, \
                axis_ids=axis_ids,type='f',missing_value=in_missing,positive='down')
                print 'positive: down'
            else:
                #don't assign positive attribute
                variable_id=cmor.variable(table_entry=opts['vcmip'],units=in_units, \
                axis_ids=axis_ids,type='f',missing_value=in_missing)
                print 'positive: None'
    except Exception, e:
        print 'E: Unable to define the CMOR variable {}'.format(e)
        raise
    #
    #Close the ACCESS file.
    #
    access_file.close()
    print 'closed input netCDF file'    
    #Loop over all the in time range ACCESS files, and process those which we need to.
    #
    print 'writing data, and calculating if needed...'
    if opts['calculation'] == '':
        print 'no calculation necessary'
    else:
        print 'calculation: {}'.format(opts['calculation'])
    sys.stdout.flush()
    #
    #calculate time integral of the first variable (possibly adding a second variable to each time)
    #
    if opts['axes_modifier'].find('time_integral') != -1:
        try:    
            run=np.float32(opts['calculation'])
        except:        
            run=np.float32(0.0)
        for input_file in inrange_access_files:
            #If the data is a climatology, store the values in a running sum
            access_file=cdms2.open(input_file,'r')
            var=access_file.variables[opts['vin'][0]]
            t=var.getTime()
            tbox=daysInMonth(t)
            varout=np.float32(var[:,0]*tbox[:]*24).cumsum(0)+run
            run=varout[-1]
            #if we have a second variable, just add this to the output (not included in the integration)
            if len(opts['vin']) == 2:
                varout+=access_file.variables[opts['vin'][1]][:]
            access_file.close()
            cmor.write(variable_id,(varout),ntimes_passed=np.shape(varout)[0])
    #
    #Monthly Climatology case
    #
    elif opts['timeshot'].find('clim') != -1:
        for input_file in inrange_access_files:
            access_file=cdms2.open(input_file,'r')
            t=access_file.variables[opts['vin'][0]].getTime()
            #Set var to be sum of variables in 'vin' (can modify to use calculation if needed)
            var=None
            for v in opts['vin']:
                try:        
                    var+=(access_file.variables[v][:])
                    print 'added extra variable'
                except:        
                    var=access_file.variables[v][:]
            try: 
                vals_wsum,clim_days=monthClim(var,t,vals_wsum,clim_days)
            except:
                #first time
                tmp=var[0,:].shape
                out_shape=(12,)+tmp
                vals_wsum=np.ma.zeros(out_shape,dtype=np.float32)
                print 'first time, data shape: {}'.format(np.shape(vals_wsum))
                clim_days=np.zeros([12],dtype=int)#sum of number of days in the month
                vals_wsum,clim_days=monthClim(var,t,vals_wsum,clim_days)
            access_file.close()
        #calculate the climatological average for each month from the running sum (vals_wsum)
        #and the total number of days for each month (clim_days)
        for j in range(12):
            print 'month: {}, sum of days: {}'.format(j+1,clim_days[j])
            #average vals_wsum using the total number of days summed for each month
            vals_wsum[j,:]=vals_wsum[j,:]/clim_days[j]
        cmor.write(variable_id,(vals_wsum),ntimes_passed=12)
    #
    #Annual means - Oyr / Eyr tables
    #
    elif (opts['axes_modifier'].find('mon2yr') != -1):
        access_file0=cdms2.open(inrange_access_files[0],'r')
        #access_file0=netCDF4.Dataset(inrange_access_files[0])
        if opts['calculation'] == '':
            data_val0=access_file0.variables[opts['vin'][0]][:]
        else:
            data_val0=calculateVals((access_file0,),opts['vin'],opts['calculation'])
        vshape=np.shape(data_val0)
        print(vshape)
        for year in range(startyear,endyear+1):
            print 'processing year {}'.format(year)
            count=0
            vsum=np.ma.zeros(vshape[1:],dtype=np.float32)
            for input_file in inrange_access_files:
                #if os.path.basename(input_file).startswith('ocean'):
                #    yearstamp=int(os.path.basename(input_file).split('.')[1][3:7])
                #else:
                #    if opts['access_version'].find('CM2') != -1:
                #        yearstamp=int(os.path.basename(input_file).split('.')[1][2:6])
                #    elif opts['access_version'].find('ESM') != -1:
                #        yearstamp=int(os.path.basename(input_file).split('.')[1][3:7])
                #if not yearstamp == year: continue
                access_file=cdms2.open(input_file,'r')
                t=access_file.variables[opts['vin'][0]].getTime()
                datelist=t.asComponentTime()
                yearinside=False
                for date in datelist:
                    if date.year == year: yearinside=True
                if yearinside:
                    print 'found data in file {}'.format(input_file)
                    for index, d in enumerate(datelist[:]):
                        if d.year == year:
                            if opts['calculation'] == '':
                                data_vals=access_file.variables[opts['vin'][0]][:]
                            else:
                                data_vals=calculateVals((access_file,),opts['vin'],opts['calculation'])
                            try: data_vals=data_vals.filled(in_missing)
                            except: pass
                            print 'shape: {}'.format(np.shape(data_vals))
                            print 'time index: {}, date: {}'.format(index,d)
                            try: 
                                vsum+=data_vals[index,:,:]
                                count+=1
                            except: 
                                vsum+=data_vals[index,:,:,:]
                                count+=1
                access_file.close()
            if count == 12:
                vyr=vsum/12
                print 'writing with cmor...'
                cmor.write(variable_id,vyr,ntimes_passed=1)
            else:
                print count
                raise Exception('WARNING: annual data contains {} months of data'.format(count))    
    #
    #Annual point values - landUse variables
    #
    elif opts['axes_modifier'].find('yrpoint') != -1:
        for year in range(startyear,endyear+1):
            for input_file in inrange_access_files:
                if opts['access_version'].find('CM2') != -1:
                    yearstamp=int(os.path.basename(input_file).split('.')[1][2:6])
                    monstamp=int(os.path.basename(input_file).split('.')[1][6:8])
                elif opts['access_version'].find('ESM') != -1:
                    yearstamp=int(os.path.basename(input_file).split('.')[1][3:7])
                    monstamp=int(os.path.basename(input_file).split('.')[1][8:10])
                if not monstamp == 12 or not yearstamp == year: continue
                access_file=cdms2.open(input_file,'r')
                t=access_file.variables[opts['vin'][0]].getTime()
                datelist=t.asComponentTime()
                yearcurrent=datelist[0].year
                if yearcurrent == year:
                    print 'processing year {}, file {}'.format(year,input_file)
                    for index, d in enumerate(datelist[:]):
                        if d.month == 12:
                            if opts['calculation'] == '':
                                data_vals=access_file.variables[opts['vin'][0]][:]
                            else:
                                data_vals=calculateVals((access_file,),opts['vin'],opts['calculation'])
                            try: data_vals=data_vals.filled(in_missing)
                            except:
                                #if values aren't in a masked array
                                pass 
                            print 'shape: {}'.format(np.shape(data_vals))
                            print 'time index: {}, date: {}'.format(index,d)
                        else: pass
                access_file.close()
            print 'writing with cmor...'
            print np.shape(data_vals)
            cmor.write(variable_id,data_vals[0,:,:,:],ntimes_passed=1)
    #
    #normal case
    #
    else:
        for i, input_file in enumerate(inrange_access_files):
            #
            #Load the ACCESS NetCDF data.
            #
            access_file=cdms2.open(input_file,'r')
            #access_file=netCDF4.Dataset(input_file)
            print 'processing file: {}'.format(input_file)
            try:
                if opts['calculation'] == '':
                    if len(opts['vin'])>1:
                        print 'error: multiple input variables are given without a description of the calculation'
                        return -1
                    else: 
                        data_vals=access_file.variables[opts['vin'][0]][:]
                        access_file.close()
                else:
                    print 'calculating...'
                    data_vals=calculateVals((access_file,),opts['vin'],opts['calculation'])
                    #convert mask to missing values
                    try: data_vals=data_vals.filled(in_missing)
                    except:
                        #if values aren't in a masked array
                        pass 
                    access_file.close()
                if opts['axes_modifier'].find('depth100') != -1:
                    data_vals=depth100(data_vals[:,9,:,:],data_vals[:,10,:,:])
            except Exception, e:
                print 'E: Unable to process data from {} {}'.format(input_file,e)
                raise
            #
            #If the data is not a climatology:
            #Write the data to the CMOR file.
            #
            else:
                try:
                    #print 'writing...'
                    print 'started writing @ ',timetime.time()-start_time
                    if time_dimension != None:
                        #assuming time is the first dimension
                        print np.shape(data_vals)
                        cmor.write(variable_id,data_vals,ntimes_passed=np.shape(data_vals)[0])
                    else:
                        cmor.write(variable_id,data_vals,ntimes_passed=0)
                    print 'finished writing @ ',timetime.time()-start_time
                except Exception, e:
                    print 'E: Unable to write the CMOR variable to file {}'.format(e)
                    raise
    #
    #Close the CMOR file.
    #
    try:
        path=cmor.close(variable_id,file_name=True)
    except:
        print 'E: We should not be here!'
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
parser.add_option('--cmip6_table_path', dest='cmip6_table_path', default='./cmip6-cmor-tables/Tables',
    help='Path to the directory where the CMIP5 tables are stored [default: %default]')
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
(options, args)=parser.parse_args()
opts=dict()
#produce a dictionary out of the options object
opts['tstart']=options.tstart
opts['tend']=options.tend
opts['cmip_table']=options.cmip_table
#opts['version_number']=options.version_number
opts['infile']=options.infile
opts['in_units']=options.in_units
opts['vin']=options.vin
opts['vcmip']=options.vcmip
opts['cmip6_table_path']=options.cmip6_table_path
opts['calculation']=options.calculation
opts['axes_modifier']=options.axes_modifier
opts['positive']=options.positive
opts['notes']=options.notes
opts['json_file_path']=options.json_file_path
opts['timeshot']=options.timeshot
opts['access_version']=options.access_version
opts['reference_date']=options.reference_date
opts['frequency']=options.frequency

if __name__ == "__main__":
    app(opts)

