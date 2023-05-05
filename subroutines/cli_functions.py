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



import numpy as np
import glob
import re
from app_functions import *
import os,sys
import stat
import xarray as xr
import cmor
import warnings
import time as timetime
import calendar
import click
import logging
import cftime


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
            ds = xr.open_dataset(all_files[i], decode_times=False)
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
def get_time_dim(ctx, ds, app_log):
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
                    units = ds[var_dim].units
                    app_log.debug(f"first attempt to tdim: {time_dimension}")
            #
            #refdate is the "reference date" used as an arbitray 0 point for dates. Common values for
            #refdate are 1 (0001-01-01) and 719 163 (1970-01-01), but other values are possible.
            #We cannot handle negative reference dates (ie. BC dates); maybe we will encounter these
            #in some climate runs?
            #
            app_log.info(f"time var is: {time_dimension}")
            app_log.info(f"Reference time is: {units}")
        except:
            pass
        del ds 
    return time_dimension, units


@click.pass_context
def check_timestamp(ctx, all_files, app_log):
    """This function tries to guess the time coverage of a file based on its timestamp
       and return the files in range. At the moment it does a lot of checks based on the realm and real examples
       eventually it would make sense to make sure all files generated are consistent in naming
    """
    inrange_files = []
    realm = ctx.obj['realm']
    app_log.info("checking files timestamp ...")
    #if we are using a time invariant parameter, just use a file with vin
    if ctx.obj['cmip_table'].find('fx') != -1:
        inrange_files = [all_files[0]]
    else:
        for infile in all_files:
            inf = infile.replace('.','_')
            inf = inf.replace('-','_')
            dummy = inf.split("_")
            if realm == 'ocean':
                tstamp = dummy[-1]
            elif realm == 'ice':
                tstamp = ''.join(dummy[-3:-2])
            else:
                tstamp = dummy[-3]
            # usually atm files are xxx.code_date_frequency.nc
            # sometimes there's no separator between code and date
            # 1 make all separator _ so xxx_code_date_freq_nc
            # then analyse date to check if is only date or codedate
            # check if timestamp as the date time separator T
            if 'T' in tstamp:
                tstamp = tstamp.split('T')[0]
            # if tstamp start with number assume is date
            if not tstamp[0].isdigit():
                tstamp = re.sub("\\D", "", tstamp)
                tlen = len(tstamp)
                if tlen >= 8:
                    tstamp = tstamp[-8:]
                elif 6 <= tlen < 8:
                    tstamp = tstamp[-6:]
                elif 4 <= tlen < 6:
                    tstamp = tstamp[-4:]
            tlen = len(tstamp)
            if tlen != 8:
                if tlen in [3, 5, 7] :
                    #assume year is yyy
                    tstamp += '0'
                if len(tstamp) == 4:
                    tstamp += '0101'
                elif len(tstamp) == 6:
                    tstamp += '01'
            # get first and last values as date string
            app_log.debug(f"tstamp for {inf}: {tstamp}")
            if ctx.obj['tstart'] <= int(tstamp) <= ctx.obj['tend']:
                inrange_files.append(infile)
    return inrange_files

 
@click.pass_context
def check_in_range(ctx, all_files, tdim, app_log):
    """Return a list of files in time range
       Open each file and check based on time axis
       Use this function only if check_timestamp fails
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
            try:
                ds = xr.open_dataset(input_file, use_cftime=True)
                # get first and last values as date string
                tmin = ds[tdim][0].dt.strftime('%Y%m%d')
                tmax = ds[tdim][-1].dt.strftime('%Y%m%d')
                app_log.debug(f"tmax from time dim: {tmax}")
                app_log.debug(f"tend from opts: {ctx.obj['tend']}")
                if int(tmin) > ctx.obj['tend'] or int(tmax) < ctx.obj['tstart']:
                    inrange_files.append(input_file)
                del ds
            except Exception as e:
                app_log.error(f"Cannot open file: {e}")
    app_log.debug(f"Number of files in time range: {len(inrange_files)}")
    app_log.info("Found all the files...")
    return inrange_files

 
@click.pass_context
def check_axis(ctx, ds, inrange_files, ancil_path, app_log):
    """
    """
    try:
        array = ds[ctx.obj['vin'][0]]
        print("shape of data: {np.shape(data_vals)}")
    except Exception as e:
        print("E: Unable to read {ctx.obj['vin'][0]} from ACCESS file")
        raise
    try:
        coords = array.coords
        coords.extend(array.dims)
    except:
        coords = coords.dims
    lon_name = None
    lat_name = None
    #search for strings 'lat' or 'lon' in coordinate names
    app_log.info(coords)
    for coord in coords:
        if 'lon' in coord.lower():
            lon_name = coord
        elif 'lat' in coord.lower():
            lat_name = coord
        # try to read lon from file if failing go to ancil files
        try:
            lon_vals = ds[lon_name]
        except:
            if os.path.basename(inrange_files[0]).startswith('ocean'):
                if ctx.obj['access_version'] == 'OM2-025':
                    acnfile = ancil_path+'grid_spec.auscom.20150514.nc'
                    lon_name = 'geolon_t'
                    lat_name = 'geolat_t'
                else:
                    acnfile = ancil_path+'grid_spec.auscom.20110618.nc'
                    lon_name = 'x_T'
                    lat_name = 'y_T'
            elif os.path.basename(inrange_files[0]).startswith('ice'):
                if ctx.obj['access_version'] == 'OM2-025':
                    acnfile = ancil_path+'cice_grid_20150514.nc'
                else:
                    acnfile = ancil_path+'cice_grid_20101208.nc'
            acnds = xr.open_dataset(acnfile)
            # only lon so far not values
            lon_vals = acnds[lon_name]
            lat_vals = acnds[lat_name]
            del acnds
        #if lat in file then re-read it from file
        try:
            lat_vals = ds[lat_name]
            app_log.info('lat from file')
        except:
            app_log.info('lat from ancil')
    return data_vals, lon_name, lat_name, lon_vals, lat_vals


@click.pass_context
def get_cmorname(ctx, axis_name, z_len=None):
    """Get time cmor name based on timeshot option
    """
    if axis_name == 't':
        timeshot = ctx.obj['timeshot']
        if 'mean' in timeshot:
            cmor_name = 'time'
        elif 'inst' in timeshot:
            cmor_name = 'time1'
        elif 'clim' in timeshot:
            cmor_name = 'time2'
        else:
            #assume timeshot is mean
            app_log.warning("timeshot unknown or incorrectly specified")
            cmor_name = 'time'
    elif axis_name == 'j':
        if 'gridlat' in ctx.obj['axes_modifier']:
            cmor_name = 'gridlatitude',
        else:
            cmor_name = 'latitude'
    elif axis_name == 'i':
        if 'gridlon' in ctx.obj['axes_modifier']:
            cmor_name = 'gridlongitude',
        else:
            cmor_name = 'longitude'
    elif axis_name == 'z':
        if 'mod2plev19' in ctx.obj['axes_modifier']:
            cmor_name = 'plev19'
        elif 'depth100' in ctx.obj['axes_modifier']:
            cmor_name = 'depth100m'
        elif (dim == 'st_ocean') or (dim == 'sw_ocean'):
            cmor_name = 'depth_coord'
        #ocean pressure levels
        elif dim == 'potrho':
            cmor_name = 'rho'
        elif axis.name == 'model_level_number' or 'theta_level' in axis.name:
            cmor_name = 'hybrid_height'
            if 'switchlevs':
                cmor_name = 'hybrid_height_half'
        elif 'rho_level' in axis_name:
            cmor_name = 'hybrid_height_half'
            if 'switchlevs':
                cmor_name = 'hybrid_height'
        #atmospheric pressure levels:
        elif axis_name == 'lev' or any(x in axis_name for x in ['_p_level', 'pressure']):
            cmor_name = f"plev{str(z_len)}"
        elif 'soil' in axis_name or axis_name == 'depth':
            cmor_name = 'sdepth'
            if 'topsoil' in ctx.obj['axes_modifier']:
                #top layer of soil only
                cmor_name = 'sdepth1'
    return cmor_name


@click.pass_context
def set_plev(ctx, data_vals, app_log):
    """
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
           #         if ctx.obj['access_version'].find('OM2')!=-1 and dim == 'sw_ocean':
           #             dim_val_bounds = dim_val_bounds[:]
           #             dim_val_bounds[-1] = dim_values[-1]
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
                """
    return


@click.pass_context
def pseudo_axis():
    """coordinates with axis_identifier other than X,Y,Z,T
    """
    cmor_name = None
    p_vals = None
    p_len = None
    if 'dropLev' in ctx.obj['axes_modifier']:
        print("variable on tiles, setting pseudo levels...")
        #z_len=len(dim_values)
        for mod in ctx.obj['axes_modifier']:
            if 'type' in mod:
                cmor_name = mod
            if cmor_name is None:
                raise Exception('could not determine land type, check variable dimensions and axes_modifiers')
            #PP check if we can just return list from det_landtype
        p_vals = list( det_landtype(cmor_name) )
    if 'landUse' in ctx.obj['axes_modifier']:
        p_vals = getlandUse()
        p_len = len(landUse)
        cmor_name = 'landUse'
    if 'vegtype' in ctx.obj['axes_modifier']:
        p_vals = cableTiles()
        p_len = len(cabletiles)
        cmor_name = 'vegtype'
    return cmor_name, p_vals, p_len


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
        app_log.info(f"{j_axis.name}")
        app_log.debug(f"lat vertices type and value: {type(lat_vertices)},{lat_vertices[0]}")
        app_log.info(f"{i_axis.name}")
        app_log.debug(f"lon vertices type and value: {type(lon_vertices)},{lon_vertices[0]}")
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
            app_log.info("setup of lat,lon grid complete")
        except Exception as e:
            app_log.error(f"E: Grid setup failed {e}")
    return grid_id


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
def get_axis_dim(ctx, var, app_log):
    """
    """
    t_axis = None
    z_axis = None    
    j_axis = None
    i_axis = None    
    p_axis = None    
    # Check variable dimensions
    dims = var.dims
    app_log.info(f"list of dimensions: {dims}")

    # make sure axis are correctly defined
    for dim in dims:
        try:
            axis = var[dim]
        except:
            app_log.warning(f"No coordinate variable associated with the dimension {dim}")
            axis = None
        # need to file to give a value then???
        if axis is not None:
            attrs = axis.attrs
            axis_name = attrs.get('axis', None)
            axis_name = attrs.get('cartesian_axis', axis_name)
            if axis_name == 'T' or 'time' in dim:
                t_axis = axis
                t_axis.attrs['axis'] = 'T'
            elif axis_name == 'Y' or any(x in dim for x in ['lat', 'y', 'nj']):
                j_axis = axis
                j_axis.attrs['axis'] = 'Y'
            elif axis_name == 'X' or any(x in dim for x in ['lon', 'x', 'ni']):
                i_axis = axis 
                i_axis.attrs['axis'] = 'X'
            elif dim.axis == 'Z' or any(x in dim for x in ['lev', 'heigth', 'depth']):
                z_axis = axis
                z_axis.attrs['axis'] = 'Z'
            elif 'pseudo' in dim.axis:
                p_axis = axis
                #p_axis.attrs['axis'] = 'pseudo' #??
            else:
                #axis_name = 'unknown'
                print(f"Unknown axis: {axis_name}")
    return t_axis, z_axis, j_axis, i_axis, p_axis

# SG: need to have a chat about this function...
@click.pass_context
def get_bounds(ctx, ds, axis, cmor_name, app_log):
    """Returns bounds for input dimension, if bounds are not available
       uses edges or tries to calculate them.
       If variable goes through calculation potentially bounds are different from
       input file and forces re-calculating them
    """
    changed_bnds = False
    if ctx.obj['calculation'] != '':
        changed_bnds = True
    dim = axis.name
    print(f"Getting bounds for axis: {dim}")
    #The default bounds assume that the grid cells are centred on
    #each grid point specified by the coordinate variable.
    keys = [k for k in axis.attrs]
    if 'bounds' in keys and not changed_bnds:
        dim_val_bnds = ds[axis.bounds].values
        app_log.info("using dimension bounds")
        if 'time' in cmor_name:
            dim_val_bnds = cftime.date2num(dim_val_bnds, units=ctx.obj['reference_date'])
    # SG: I had to add this so that the bounds were taken from the Xarray dataset
    # I think this whole function needs to be re-written. There definitely doesn't need
    # to be a difference between variables that need a claulation and those that don't
    elif 'bounds' in keys and changed_bnds:
        dim_val_bnds = ds[axis.bounds].values
        app_log.info("using dimension bounds")
        if 'time' in cmor_name:
            dim_val_bnds = cftime.date2num(dim_val_bnds, units=ctx.obj['reference_date'])
    #elif edges in keys and not changed_bnds:
        #dim_val_bnds = ds[axis.edges].values
        #app_log.info("using dimension edges as bounds")
    else:
        app_log.info(f"No bounds for {dim} - creating default bounds")
        # if time check we have units and convert dates to floats
        if 'time' in cmor_name:
            axis_val = cftime.date2num(axis, units=ctx.obj['reference_date'])
        else:
            axis_val = axis.values
        try:
            min_vals = (axis + axis.shift(1))/2
            min_vals[0] = 1.5*axis[0] - 0.5*axis[1]
            max_vals = min_vals.shift(-1)
            max_vals[-1] = 1.5*axis[-1] - 0.5*axis[-2]
        except Exception as e:
            app_log.warning(f"dodgy bounds for dimension: {dim}")
            app_log.error(f"error: {e}")
        dim_val_bnds = np.column_stack((min_vals, max_vals))
    # Take into account type of axis
    # as we are often concatenating along time axis and bnds are considered variables
    # they will also be concatenated along time axis and we need only 1st timestep
    #not sure yet if I need special treatment for if cmor_name == 'time2':
    if 'time' not in cmor_name:
        if dim_val_bnds.ndim == 3:
            dim_val_bnds = dim_val_bnds[0,:,:].squeeze() 
    if cmor_name == 'time1':
        dim_val_bnds = None
    elif cmor_name == 'latitude' and changed_bnds:
        #force the bounds back to the poles if necessary
        if dim_val_bnds[0,0] < -90.0:
            dim_val_bnds[0,0] = -90.0
            print("setting minimum latitude bound to -90")
        if dim_val_bnds[-1,-1] > 90.0:
            dim_val_bnds[-1,-1] = 90.0
            print("setting maximum latitude bound to 90")
    elif cmor_name == 'depth':
        if 'OM2' in ctx.obj['access_version'] and dim == 'sw_ocean':
            dim_val_bnds[-1] = axis[-1]
    return dim_val_bnds


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
    """This can be eliminated as I oved first two lines before normal case
    """
    app_log.info('ONLY 1st, 11th, 21st days to be used')
    dsinsel = dsin.where(dsin[time_dimension].dt.day.isin([1, 11, 21]), drop=True)
    a10_datavals = dsinsel[ctx.obj['vin']]
    if ctx.obj['calculation'] != '':
        app_log.info("calculating...")
        a10_datavals = calculateVals(dsinsel, ctx.obj['vin'], ctx.obj['calculation'])
    try:
        array = array.fill(in_missing)
    except:
        #if values aren't in a masked array
        pass 
    #app_log.info("writing with cmor...")
    #try:
    #    if time_dimension != None:
            #assuming time is the first dimension
    #        app_log.info(a10_datavals.shape)
    #        cmor.write(variable_id, a10_datavals.values,
    #            ntimes_passed=a10_datavals.shape[0])
    #    else:
    #        cmor.write(variable_id, a10_datavals.values, ntimes_passed=0)
    #except Exception as e:
    #    app_log.error(f"E: Unable to write the CMOR variable to file {e}")
    #    raise
    return array


@click.pass_context
def calc_monsecs(ctx, dsin, tdim, in_missing, app_log):
    """
    """
    monsecs = calendar.monthrange(dsin[tdim].dt.year,dsin[tdim].dt.month)[1] * 86400
    if ctx.obj['calculation'] == '':
        array = dsin[ctx.obj['vin'][0]]
        #print(data_vals)
    else:
        app_log.info("calculating...")
        array = calculateVals(dsin, ctx.obj['vin'], ctx.obj['calculation'])
    array = array / monsecs
    #convert mask to missing values
    try: 
        array = array.fillna(in_missing)
    except:
        #if values aren't in a masked array
        pass 
    #app_log.info("writing with cmor...")
    #try:
    #    if time_dimension != None:
    #        #assuming time is the first dimension
    #        app_log.info(np.shape(data_vals))
    #        cmor.write(variable_id, data_vals.values,
    #            ntimes_passed=np.shape(data_vals)[0])
    #    else:
    #        cmor.write(variable_id, data_vals, ntimes_passed=0)
    #except Exception as e:
    #    print(f"E: Unable to write the CMOR variable to file {e}")
    #    raise
    return array

# SG: Modified this a bit, will tidy it up.
@click.pass_context
def normal_case(ctx, dsin, tdim, in_missing, app_log):
    """
    """
    if ctx.obj['calculation'] == '':
        array = dsin[ctx.obj['vin'][0]][:]
        app_log.debug(f"{array}")
    else:
        app_log.info("Calculating...")
        #PP potentially pass tdim??
        #app_log.info(f"Files for calculation: {dsin}")
        app_log.info(f"variables for calculation: {ctx.obj['vin']}")
        #app_log.info(f"Calculation to be performed: {ctx.obj['calculation']}")
        #array = calculateVals(dsin, ctx.obj['vin'], ctx.obj['calculation'])
        array = calculateValsTEST(dsin, ctx.obj['vin'], ctx.obj['calculation'], app_log)
        app_log.info("Calculation completed...")
        #convert mask to missing values
        #PP why mask???
        array = array.fillna(in_missing)
        app_log.debug(f"{array}")
    # temporarily ignore this exception
    #if 'depth100' in ctx.obj['axes_modifier']:
    #    data_vals = depth100(data_vals[:,9,:,:], data_vals[:,10,:,:])
    #If the data is not a climatology:
    #Write the data to the CMOR file.
    return array 

# SG: I've moved this from app_functions.py to help with debugging
# I think this should be combined into the above function anyway.
@click.pass_context
def calculateValsTEST(ctx, access_file, varNames, calculation, app_log):
    '''
    Function to call the calculation defined in the 'calculation' string in the database
    '''

    var = []
    app_log.info("Adding variables to var list")
    for v in varNames:
        print(f'variable[{varNames.index(v)}] = {v}')
        try: 
            #extract variable out of file
            var.append(access_file[v][:])
        except Exception as e:
            app_log.error(f"Error appending variable, {v}: {e}")
            raise

    app_log.info("Finished adding variables to var list")

    # Now try to perform the required calculation
    try:
        app_log.info("Doing calculation...")
        calc = eval(calculation)
        app_log.info("Done..")
    except Exception as e:
        app_log.error(f"error evaluating calculation, {calculation}: {e}")
        raise

    return calc