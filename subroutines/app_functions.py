'''
Changes to script

14/03/23:
SG - Updated print statements and exceptions to work with python3.
SG- Added spacesa and formatted script to read better.

17/03/23:
SG - Changed cdms2 to Xarray.
SG - Refactored a several functions to make them cleaner.

17/03/23:
SG - Removed some missed cdms2 lines.

30/03/23:
SG - Refactored a lot of the functions to removed repetitive code.
'''

import click
import logging

import datetime
import numpy as np
import re
import os,sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import calendar
import xarray as xr
import warnings
import math
import yaml
from operator import and_
from scipy.interpolate import interp1d 
warnings.simplefilter(action='ignore', category=FutureWarning)
np.set_printoptions(threshold=sys.maxsize)

# Global Variables
#-----------------------------------
ancillary_path = os.environ.get('ANCILLARY_FILES', '')+'/'

ice_density = 900 #kg/m3
snow_density = 300 #kg/m3

rd = 287.0
cp = 1003.5
p_0 = 100000.0

R_e = 6.378E+06
#-----------------------------------

@click.pass_context
def calculateVals(ctx, access_file, varNames, calculation):
    '''
    Function to call the calculation defined in the 'calculation' string in the database
    '''
    #PP WE need to take into account that we are using xarray now
    #PP first we should review if we need to get dimensions, that would depend on the 
    #PP calculation as defined in this file, however if we need to do so
    #PP get dimensions from variable.dims 
    #PP another possibility is to pass a list of dimensions in mapping file 
    #PP or even better being specific in calculation on what the dimension name is
    #Set array for coordinates if used by calculation
    if 'times' in calculation:
        times = access_file[varNames[0]].getTime()
    if 'depth' in calculation:
        depth = access_file[0][varNames[0]].getAxis(1)
    else:
        pass
    if calculation.find('lat')!=-1:
        lat = access_file[0][varNames[0]].getLatitude()
    else:
        pass
    if calculation.find('lon')!=-1:
        lon = access_file[0][varNames[0]].getLatitude()
    else:
        pass

    # Loop through the var names that have been pssed into the function
    # And save to the empty var list.
    var = []
    for v in varNames:
        print(f'variable[{varNames.index(v)}] = {v}')
        try: 
            #extract variable out of file
            var.append(access_file[0][v][:])
        except Exception as e:
            print(f'Error appending variable, {v}: {e}')
            raise

    # Now try to perform the required calculation
    try:
        calc = eval(calculation)
    except Exception as e:
        print(f'error evaluating calculation, {calculation}: {e}')
        raise

    return calc

def meridionalOverturning(transList,typ,om2='1'):
    '''
    Calculate the y_overturning mass stream function
    For three basins: 
    0 Atlantic-Arctic
    1 Indian-Pacific basin
    2 Global Basin
    '''
    #TODO remove masks once satified with these calculations
    ty_trans = transList[0]
    #print type(ty_trans), np.shape(ty_trans) ,np.shape(ty_trans.mask)
    #initialise array
    dims = list(np.shape(ty_trans[:,:,:,0])) +[3] #remove x, add dim for 3 basins
    transports = np.ma.zeros(dims,dtype=np.float32)
    #first calculate for global basin
    #2: global basin
    transports[:,:,:,2] = calcOverturning(transList,typ)
    try:
        if ty_trans.mask==False: 
        #if there is no mask: set masks to arrays of zeros
            for trans in transList:
                trans.mask = np.zeros(np.shape(trans),dtype=bool)
        else:
            pass
    except: 
        pass
    #grab land mask out of ty_trans file (assuming the only masked values are land)
    landMask = np.array(ty_trans.mask[0,:,:,:],dtype=bool)
    #Get basin masks
    if om2 == "025":
        mask = getBasinMask_025()
    else:
        mask = getBasinMask()
    #0: atlantic arctic basin
    #set masks
    #atlantic and arctic basin are given by mask values 2 and 4 #TODO double check this
    atlantic_arctic_mask = np.ma.make_mask(np.logical_and(mask!=2.0,mask!=4.0))
    #calculate MOC with atlantic basin mask
    transports[:,:,:,0] = calcOverturning(mask_loop(transList, ty_trans, landMask, atlantic_arctic_mask),typ)
    #1: indoPacific basin:
    #set masks
    #Indian and Pacific basin are given by mask values 3 and 5 #TODO double check this
    indoPac_mask = np.ma.make_mask(np.logical_and(mask!=3.0,mask!=5.0))
    transports[:,:,:,1] = calcOverturning(mask_loop(transList, ty_trans, landMask, indoPac_mask),typ)
    return transports

def mask_loop(transList, ty_trans, landMask, region_mask):
    for t in range(np.shape(ty_trans)[0]):
        for z in range(np.shape(ty_trans)[1]):
            for trans in transList:
                trans.mask[t,z,:,:] = np.ma.mask_or(region_mask,landMask[z,:,:])
    return transList
    
def calcOverturning(transList,typ):
    '''
    Calculate overturning circulation depending on what inputs are given.

    - Assumes transList is a list of variables:
        ty_trans, ty_trans_gm, ty_trans_submeso
        where the gm and submeso quantities may or may not exist
        gm = bolus

    - Calculation is:
        sum over the longditudes and 
        for ty_trans, run a cumalative sum over depths (not for gm or submeso)
        The result for each variable in transList are added together
    '''
    typ = typ.split('_')
    n = len(transList)
    print('type = ',typ)
    print('n = ',n)
    if (len(typ) == 1 or typ[1]=='Sv'):
        #normal case for cmip5, need to convert units
        # gm and submeso quantities are output in Sv so need to be multiplied by 10**9
        typ = typ[0]
        tmp0 = transList[0].sum(3) #*10**9 trans
        tmp1 = transList[1].sum(3) # trans_gm
        tmp2 = transList[2].sum(3) # trans_submeso
        s = tmp0.sum(1)
        
        if n == 1:
            if typ == 'bolus':
                #should be bolus transport for rho levels
                calc = tmp0
        elif n == 2:
            if typ == 'bolus':
                #bolus advection is sum of gm and submeso
                calc = tmp0 + tmp1 #*10**9
            elif typ == 'full':
                #full y overturning on rho levels, where trans and trans_gm are present (no submeso)
                calc = tmp0.cumsum(1) + tmp1 #*10**9
                for i in range(calc.shape[1]):
                    calc[:,i,:] = calc[:,i,:] - s
        elif n == 3:
            #assume full y overturning:
            #trans + trans_gm +trans_submeso
            if typ == 'full':
                calc = tmp0.cumsum(1) + tmp1 + tmp2
                s = tmp0.sum(1)
                for i in range(calc.shape[1]):
                    calc[:,i,:] = calc[:,i,:] - s
    else:
        pass

    return calc   

def landFrac(nlat):
    if nlat == 145:
        f = xr.open_dataset(f'{ancillary_path}esm_landfrac.nc')
    if nlat == 144:
        f = xr.open_dataset(f'{ancillary_path}cm2_landfrac.nc')
    vals = np.float32(f.fld_s03i395[0,:,:]).filled(0)
    f.close()
    return vals

def fracLut(var,nwd):
    #nwd (non-woody vegetation only) - tiles 6,7,9,11 only
    t,z,y,x = np.shape(var)
    vout = np.ma.zeros([t,4,y,x],dtype=np.float32)
    #p_a_s_land tiles 1-7,11,14 (+8,16,17?)
    if nwd == 0:
        for i in [1,2,3,4,5,6,7,11,14]: 
            t = i-1
            vout[:,0,:,:] += var[:,t,:,:]
    elif nwd == 1:
        for i in [6,7,11]: 
            t = i-1
            vout[:,0,:,:] += var[:,t,:,:]
    #no pastures
    vout[:,1,:,:] = vout[:,1,:,:]
    #crop tile 9
    vout[:,2,:,:] = var[:,8,:,:]
    #urban tile 15
    if nwd == 0:
        vout[:,3,:,:] = var[:,14,:,:]
    elif nwd == 1:
        vout[:,3,:,:] = vout[:,3,:,:]
    if vout.shape[2] == 145:
        landfrac = landFrac(145)
        vout[:,0,:,:] = vout[:,0,:,:]*landfrac
        vout[:,1,:,:] = vout[:,1,:,:]*landfrac
        vout[:,2,:,:] = vout[:,2,:,:]*landfrac
        vout[:,3,:,:] = vout[:,3,:,:]*landfrac
    elif vout.shape[2] == 144:
        landfrac=landFrac(144)
        vout[:,0,:,:] = vout[:,0,:,:]*landfrac
        vout[:,1,:,:] = vout[:,1,:,:]*landfrac
        vout[:,2,:,:] = vout[:,2,:,:]*landfrac
        vout[:,3,:,:] = vout[:,3,:,:]*landfrac
    else:
        raise Exception('could not apply landFrac')
    return vout
    
#list of the names of the cmip6 landUse dimension
def getlandUse():
    landuse = ['primary_and_secondary_land','pastures','crops','urban']
    return landuse
    
#list of the names of the land tiles in cable
def cableTiles():
    cabletiles = ['Evergreen_Needleleaf','Evergreen_Broadleaf','Deciduous_Needleleaf','Deciduous_Broadleaf','Shrub',\
        'C3_grass','C4_grass','Tundra','C3_crop','C4_crop', 'Wetland','','','Barren','Urban','Lakes','Ice'] 
    return cabletiles

def deg_open(deg):
    '''
    New function to open a file based on a deg value, 5 functions are using this.
    '''
    if deg == "1":
        f = xr.open_dataset(f'{ancillary_path}om2_grid.nc') #file with grids specifications
    elif deg == "025":
        f = xr.open_dataset(f'{ancillary_path}om2-025_grid.nc') #file with grids specifications
    return f

def calc_global_ave_ocean_om2(var,rho_dzt,deg):
    area_t = np.float32(deg_open(deg).area_t[:])
    deg_open(deg).close()
    mass = rho_dzt * area_t
    print(np.shape(var))
    try: 
        vnew = np.average(var,axis=(1,2,3),weights=mass)
    except: 
        vnew = np.average(var,axis=(1,2),weights=mass[:,0,:,:])
    return vnew

def tileFraci317():
    '''
    Open the tilefrac file and extract the fld_s03i317 variable.
    '''
    f = xr.open_dataset(f'{ancillary_path}cm2_tilefrac.nc')
    vals = np.float32(f.fld_s03i317[0,:,:,:]) #.filled(0)
    f.close()
    return vals

def apply_landfrac(vout):
    '''
    Apply the landfrac to the variable array.
    '''
    if vout.shape[1] == 145:
        landfrac = landFrac(145)
        vout = vout*landfrac
    elif vout.shape[1] == 144:
        landfrac = landFrac(144)
        vout = vout * landfrac
    else:
        raise Exception('could not apply landFrac')
    return vout

#calculate weighted average using tile fractions
#sum of variable for each tile 
# multiplied by tile fraction
def tileAve(var,tileFrac,lfrac=1):
    var = np.asarray(var[:])
    t,z,y,x = np.shape(var)
    vout = np.ma.zeros([t,y,x],dtype=np.float32)
    try:
        if tileFrac == '317':
            tileFrac = tileFraci317()
            #loop over pft tiles and sum
            for k in range(t):
                for i in range(z):
                    vout += var[k,i,:,:] * tileFrac[i,:,:]
        else: 
            raise Exception
    except:
        #loop over pft tiles and sum
        for i in range(z):
            vout += var[:,i,:,:] * tileFrac[:,i,:,:]
    if lfrac == 1:
        vout = apply_landfrac(vout)
    else:
        pass
    return vout

def tileSum(var,lfrac=1):
    t,z,y,x = np.shape(var)
    vout = np.ma.zeros([t,y,x],dtype=np.float32)
    for i in range(z):
        vout += var[:,i,:,:]

    if lfrac == 1:
        vout = apply_landfrac(vout)
    else:
        pass
    return vout
 
def tileFracExtract(tileFrac,tilenum):
    t,z,y,x = np.shape(tileFrac)
    vout = np.ma.zeros([t,y,x],dtype=np.float32)
    if isinstance(tilenum, int) == 1:
        n = tilenum-1
        vout += tileFrac[:,n,:,:]
    elif isinstance(tilenum, list):
        for i,t in enumerate(tilenum):
            n = t-1
            vout += tileFrac[:,n,:,:]
    else:
        raise Exception('E: tile number must be integer or list')
    
    vout = apply_landfrac(vout)
    return vout
    
def landmask(var):
    t,y,x = np.shape(var)
    vout = np.ma.zeros([t,y,x],dtype=np.float32)
    if var.shape[1] == 145:
        landfrac = landFrac(145)
    elif var.shape[1] == 144:
        landfrac = landFrac(144)
    for i in range(t):
        vout[i,:,:] = np.ma.masked_where(landfrac == 0,var[i,:,:])
    return vout

def tslsi(sf_temp,si_temp):
    tileFrac = tileFraci317()
    t,z,y,x = np.shape(sf_temp)
    sf_temp_sum = np.ma.zeros([t,y,x],dtype=np.float32)
    #loop over pft tiles and sum
    for i in range(z):
        sf_temp_sum += sf_temp[:,i,:,:] * tileFrac[i,:,:]
    
    sf_temp_sum = apply_landfrac(sf_temp_sum, vout)
    si_temp_mask = np.ma.masked_values(si_temp,271.35)
    #vout=sf_temp_sum
    vout = np.ma.array(sf_temp_sum.data+si_temp_mask.data,mask=map(and_,sf_temp_sum.mask,si_temp_mask.mask))
    return vout

#temp for land or sea ice
def calc_tslsi(var):
    #total temp,open sea temp,seaIce area fraction
    ts,ts_sea,sic = var
    #land area Fraction
    A_l = landFrac()
    #land or sea ice fraction
    A_lsi = A_l + (1 - A_l) * sic
    #open ocean fraction
    A_o = (1 - A_l) * (1 - sic)
    return (ts - ts_sea * A_o) / A_lsi

#hfbasin is output as the heat transport hfy, integrated over depth and longitude,
# calculated for each of three ocean basins
#hfy is given as the linear sum of the variables in transList
def hfbasin(transList,om2=1):
    dims = list(np.shape(transList[0][:,:,0])) + [3] #remove x add dim for 3 basins
    output = np.ma.zeros(dims,dtype=np.float32)
    #grab land mask from first var (assuming the only masked values are land)
    landMask = np.array(transList[0].mask[0,:,:],dtype=bool)
    #Get basin masks
    if om2 == "025":
        print('025deg')
        basin = getBasinMask_025()
    else:
        print('1deg')
        basin = getBasinMask()
    #2 global basin
    for trans in transList:
        output[:,:,2] += trans.sum(2)
    #0: atlantic arctic basin
    #atlantic and arctic basin are given by mask values 2 and 4 #TODO double check this
    atlantic_arctic_mask = np.ma.make_mask(np.logical_and(basin!=2.0,basin!=4.0))
    for trans in transList:
        for t in range(np.shape(trans)[0]):
                trans.mask[t,:,:] = np.ma.mask_or(atlantic_arctic_mask,landMask)    
        output[:,:,0] += trans.sum(2)
    #1: indoPacific basin:
    #Indian and Pacific basin are given by mask values 3 and 5 
    # TODO double check this
    indoPac_mask = np.ma.make_mask(np.logical_and(basin!=3.0,basin!=5.0))
    for trans in transList:
        for t in range(np.shape(trans)[0]):
                trans.mask[t,:,:] = np.ma.mask_or(indoPac_mask,landMask)    
        output[:,:,1] += trans.sum(2)        
    return output
    
#calculates the northward meridional fluxes for each basin
#assumes that the x -coordinate has already been averaged over
def basinMeridFlux(var):
    glob,atlantic,arctic,pacific,indian = var
    dims = list(np.shape(glob[:,:])) + [3] #remove x, add dim for 3 basins
    output = np.ma.zeros(dims,dtype=np.float32)
    #atlantic,arctic
    output[:,:,0] = atlantic[:,:] + arctic[:,:]
    #indo-pacific
    output[:,:,1] = indian[:,:] + pacific[:,:]
    #global
    output[:,:,2] = glob[:,:]
    return output

def tos_degC(var):
    var = np.ma.asarray(var[:])
    #PP??? shouldn't we trust units??
    #PP in any case can be simplified
    if var[0].mean() >= 200:
        print('temp in K, converting to degC')
        var = var[:] - 273.15
    return var
    
def tos_3hr(var):
    var = tos_degC(var)
    t,y,x = np.shape(var)
    vout = np.ma.zeros([t,y,x],dtype=np.float32)
    if var.shape[1] == 145:
        landfrac = landFrac(145)
    elif var.shape[1] == 144:
        landfrac = landFrac(144)
    for i in range(t):
         vout[i,:,:] = np.ma.masked_where(landfrac == 1,var[i,:,:])
    return vout
    
def tossq_degC(var):
    var = np.ma.asarray(var[:])
    if var[0].mean() >= 10000:
        print('temp in K^2, converting to degC^2')
        var=((var[:] ** 0.5) - 273.15) ** 2
    return var

def ocean_surface(var):
    # PP can be removed in v=favour of a simple sel/isel
    return var[:,0,:,:]
    
def depth100(d95,d105):
    vout = (d95+d105) / 2
    vout = np.ma.masked_where(np.ma.is_masked(d105),d105)
    return vout

def calcrsdoabsorb(heat,flux):
    # PP potentially using where z=0 ?
    t,z,y,x = np.shape(heat)
    vout = np.ma.zeros([t,z,y,x],dtype=np.float32)
    for i in range(z):
        if i == 0:
            vout[:,0,:,:] = heat[:,0,:,:]+flux[:,:,:]
        else:
            vout[:,i,:,:] = heat[:,i,:,:]
    return vout

def ocnrmadvect_offine(var,tempsalt):
    DIA = var[0] - var[1]
    KPP = var[2]
    EIT = var[3] + var[4]
    SUB = var[5]
    CON = var[1] + var[6]
    RIV = var[7]
    SIG = var[8]
    NET = var[9]
    if tempsalt == 'temp': #ocontemprmadvect
        SWP = var[10]
        FRZ = var[11]
        PME = var[12] #2D
        SMO = var[13] #2D
        CON3D = DIA + KPP + EIT + SUB + CON + RIV + SIG + SWP + FRZ
        CON2D = PME + SMO
        t,z,y,x = np.shape(CON3D)
        RHS = np.ma.zeros([t,z,y,x],dtype=np.float32)
        RHS[:,0,:,:] = CON3D[:,0,:,:] + CON2D
        RHS[:,1:,:,:] = CON3D[:,1:,:,:]
    elif tempsalt == 'salt': #osaltrmadvect
        RHS = DIA + KPP + EIT + SUB + CON + RIV + SIG
    else: 
        raise Exception("E: var[1] must be 'temp' or 'salt'")
    ADV = NET - RHS
    vout = ADV + var[3] + SUB
    return vout

def ocndepthint_var(var, rho, dz, landmask):
    '''
    Calculate the ocean depth?
    '''
    var = tos_degC(var)
    var = np.ma.filled(var[:],0)
    rho = np.ma.filled(rho[:],0)
    dz = np.ma.filled(dz[:],0)
    t,z,y,x = np.shape(var)
    vout = np.ma.zeros([t,y,x],dtype=np.float32)
    vmul = var * rho * dz
    for i in range(z):
        vout += vmul[:,i,:,:]
    for i in range(t):
        vout[i,:,:] = np.ma.masked_where(landmask == 0,vout[i,:,:])

    return vout

def ocndepthint(var,rho,dz):
    '''
    Calls ocndepthint_var to pass throught the correct landmask file.
    '''
    landmask = oceanFrac()
    vout = ocndepthint_var(var, rho, dz, landmask)
    return vout

def ocndepthint_025(var,rho,dz):
    '''
    Calls ocndepthint_var to pass throught the correct landmask file.
    '''
    landmask = oceanFrac_025()
    vout = ocndepthint_var(var, rho, dz, landmask)
    return vout

def oceanFrac():
    '''
    Opens a dataset to extract the ocean frac variable.
    '''
    f = xr.open_dataset(f'{ancillary_path}grid_spec.auscom.20110618.nc')
    ofrac = np.float32(f.wet[:,:])
    f.close()
    return ofrac

def oceanFrac_025():
    '''
    Opens a dataset to extract the ocean frac variable.
    '''
    f = xr.open_dataset(f'{ancillary_path}om2-025_ocean_mask.nc')
    ofrac = np.float32(f.variables['mask'][:,:])
    f.close()
    return ofrac

def getBasinMask():
    f = xr.open_dataset(f'{ancillary_path}lsmask_ACCESS-OM2_1deg_20110618.nc')
    mask_ttcell = f.mask_ttcell[0,:,:]
    f.close()
    return mask_ttcell

def getBasinMask_025():
    f = xr.open_dataset(f'{ancillary_path}lsmask_ACCESS-OM2_025deg_20201130.nc')
    mask_ttcell = f.variablesmask_ttcell[0,:,:]
    f.close()
    return mask_ttcell

def calc_rsds(sw_heat,swflx):
    sw_heat[:,0,:,:] = swflx+sw_heat[:,0,:,:] #correct surface level
    return sw_heat

def get_vertices_main(name, path, path_aux):
    '''
    Take name of grid and find corresponding vertex positions from ancillary file
    '''
    #dictionary to map grid names to names of vertex variables
    dictionary = {'geolon_t':'x_vert_T','geolat_t':'y_vert_T','geolon_c':'x_vert_C','geolat_c':'y_vert_C',\
    'TLON':'lont_bonds','TLAT':'latt_bonds','ULON':'lonu_bonds','ULAT':'latu_bonds'}
    try:
        vertexname = dictionary[name]
    except:
        raise Exception('app_funcs.get_vertices: ocean grid specification unknown, '+name)
    try: #ocean grid
        f = xr.open_dataset(f'{ancillary_path}{path}')
        vert = np.array(f[vertexname[:]],dtype='float32').transpose((1,2,0))
        f.close()
    except: #cice grid (convert from rad to degrees)
        f = xr.open_dataset(f'{ancillary_path}{path_aux}')
        vert = np.array(f[vertexname[:]],dtype='float32').transpose((1,2,0))*57.2957795
        f.close()
    #restrict longditudes to the range0-360
    return vert

def get_vertices(name):
    #PP important are these files always the same??
    path = 'grid_spec.auscom.20110618.nc'
    path_aux = 'cice_grid_20101208.nc'
    vert = get_vertices_main(name, path, path_aux)
    return vert[:]

def get_vertices_025(name):
    #PP important are these files always the same??
    path = 'grid_spec.auscom.20150514.nc'
    path_aux = 'cice_grid_20150514.nc'
    vert = get_vertices_main(name, path, path_aux)
    return vert[:]

#first variable is dummy tsoil (on soil levels)
#second variable is tile frac
#the rest of the variables are the soil temp for one level, for each tile
def calc_tsl(var):
    vout = var[0] * 0
    #loop over soil levels, calculate tile average for each.
    for i in range(6):
        vout[:,i,:,:] = tileAve(var[i+2],var[1])
    return vout

def calc_areacello(area,mask_v):
    area.mask = mask_v.mask
    return area.filled(0)

def calc_areacello_om2(deg):
    area = np.float32(deg_open(deg).area_t[:])
    mask_v = np.float32(deg_open(deg).ht[:])
    area.mask = mask_v.mask
    deg_open(deg).close()
    return area.filled(0)

def calc_volcello_om2(dht,deg):
    area = np.float32(deg_open(deg).area_t[:])
    vout = area * dht
    deg_open(deg).close()
    return vout
    
def getdeptho(deg):
    deptho = np.float32(deg_open(deg).ht[:])
    deg_open(deg).close()
    return deptho
    
def plevinterp(var,pmod,heavy,lat,lat_v):
    '''
    Rewrote function to optimize. 
    '''
    plev,bounds = plev19()
    # Using numpy's built-in shape command.
    t, z, x, y = var.shape
    th, zh, xh, yh = heavy.shape
    if xh != x:
        print('heavyside not on same grid as variable; interpolating...')
        # Used reshape to flatten the heavy array along the last two axes and then 
        # used interp1d with axis=-1 to interpolate over all points simultaneously, 
        # avoiding the need for nested loops.
        hint = interp1d(lat, heavy.reshape(th, zh, -1), kind="linear", axis=-1, fill_value="extrapolate")
        hout = hint(lat_v).reshape(th, zh, -1, yh)
    else:
        hout = heavy
    # Moved the hout <= 0.5 and hout > 0.5 operations to a single np.where 
    # statement to avoid creating two intermediate arrays.
    hout = np.where(hout > 0.5, 1, 0)
    vout = np.ma.zeros([t,len(plev),x,y],dtype=np.float32)
    print('interpolating var from model levels to plev19...')
    # Using NumPy vectorization instead of loops.
    interp_func = interp1d(pmod, var, kind="linear", axis=1, fill_value="extrapolate")
    vout = interp_func(plev)
    return vout/hout

def calc_zostoga(var,depth,lat):
    '''
    calculates zostga from T and pressure
    '''
    #extract variables
    [T,dz,areacello] = var
    zostoga = zost(depth, lat, T, dz, areacello, 35.00)
    return zostoga

def calc_zostoga_om2(var,depth,lat,deg):
    '''
    calculates zostga from T and pressure
    '''
    #extract variables
    [T,dz] = var
    areacello = np.float32(deg_open(deg).area_t[:])
    lat = deg_open(deg).area_t.getLatitude()
    deg_open(deg).close()
    print(F'lat: {lat}')
    zostoga = zost(depth, lat, T, dz, areacello, 35.00)
    return zostoga

def calc_zossga(var,depth,lat):
    '''
    calculates zossga from T,S and pressure
    '''
    #extract variables
    [T,S,dz,areacello] = var
    zossga = zost(depth, lat, T, dz, areacello, S)
    return zossga

def zost(depth, lat, T, dz, areacello, S):
    '''
    Duplicate code in calc_zostoga, calc_zostoga_om2, and calc_zossga.
    '''
    [nt,nz,ny,nx] = T.shape #dimension lengths
    zostoga = np.zeros([nt],dtype=np.float32)
    #calculate pressure field
    press = np.ma.array(sw_press(depth,lat))
    press.mask = T[0,:].mask
    #do calculation for each time step
    for t in range(nt):
        if S == 35.00:
            tmp = ((1. - rho_from_theta(T[t,:],S,press)/rho_from_theta(4.00,35.00,press))*dz[t,:]).sum(axis=0)
        else:
            tmp = ((1. - rho_from_theta(T[t,:],S[t,:],press)/rho_from_theta(4.00,35.00,press))*dz[t,:]).sum(axis=0)
        areacello.mask = T[0,0,:].mask
        zostoga[t] = (tmp*areacello).sum(0).sum(0)/areacello.sum()
    return zostoga

#function to calculate density from temp, salinity and pressure
def rho_from_theta(th,s,p):
    th2 = th * th
    sqrts = np.ma.sqrt(s)
    anum =          9.9984085444849347e+02 +    \
               th*( 7.3471625860981584e+00 +    \
               th*(-5.3211231792841769e-02 +    \
               th*  3.6492439109814549e-04)) +  \
                s*( 2.5880571023991390e+00 -    \
               th*  6.7168282786692355e-03 +    \
                s*  1.9203202055760151e-03)
    aden =          1.0000000000000000e+00 +    \
               th*( 7.2815210113327091e-03 +    \
               th*(-4.4787265461983921e-05 +    \
               th*( 3.3851002965802430e-07 +    \
               th*  1.3651202389758572e-10))) + \
                s*( 1.7632126669040377e-03 -    \
               th*( 8.8066583251206474e-06 +    \
              th2*  1.8832689434804897e-10) +   \
            sqrts*( 5.7463776745432097e-06 +    \
              th2*  1.4716275472242334e-09))
    pmask = (p!=0.0)
    pth = p*th
    anum = anum +   pmask*(     p*( 1.1798263740430364e-02 +   \
                       th2*  9.8920219266399117e-08 +   \
                         s*  4.6996642771754730e-06 -   \
                         p*( 2.5862187075154352e-08 +   \
                       th2*  3.2921414007960662e-12)) )
    aden = aden +   pmask*(     p*( 6.7103246285651894e-06 -   \
                  pth*(th2*  2.4461698007024582e-17 +   \
                         p*  9.1534417604289062e-18)) )
#    print 'rho',np.min(anum/aden),np.max(anum/aden)
    return anum/aden

def sw_press(dpth,lat):
    '''
    Calculates the pressure field from depth and latitude
    '''
    #return array on depth,lat,lon    
    pi = 4 * np.arctan(1.)
    deg2rad = pi / 180
    x = np.sin(abs(lat[:]) * deg2rad)  # convert to radians
    c1 = 5.92e-3 + x ** 2 * 5.25e-3
    #expand arrays into 3 dimensions
    nd,=dpth.shape
    nlat,nlon = lat.shape
    dpth = np.expand_dims(np.expand_dims(dpth[:],1),2)
    dpth = np.tile(dpth,(1,nlat,nlon))
    c1 = np.expand_dims(c1,0)
    c1 = np.tile(c1,(nd,1,1))
    vout = ((1-c1)-np.sqrt(((1-c1)**2)-(8.84e-6*dpth)))/4.42e-6
    return vout

def fix_packing_division(num, den):
    vout = num / den
    vout[vout == 0.0] = 0.5 * np.min(vout[vout > 0.0])
    return vout
    
#CCMI2022 functions
# PP probably redundant having this as a function??
def column_max(var):
    vout = np.max(var,axis=1)
    return vout

#PP should just use isel rather than defining function
#SG: This doesn't look to be used anywhere anyway
def extract_lvl(var, lvl):
    vout = var[:,lvl,:,:]
    return vout

def getSource(model):
    """PP this should come from a configuration file
       so definitons can be checked outside the code
       probably yaml would be most suitable
    """
    if model=='ACCESS1-0'or model=='ACCESS1.0':
        return 'ACCESS1-0 2011. '+\
        'Atmosphere: AGCM v1.0 (N96 grid-point, 1.875 degrees EW x approx 1.25 degree NS, 38 levels); '+\
        'ocean: NOAA/GFDL MOM4p1 (nominal 1.0 degree EW x 1.0 degrees NS, tripolar north of 65N, '+\
         'equatorial refinement to 1/3 degree from 10S to 10 N, cosine dependent NS south of 25S, 50 levels); '+\
        'sea ice: CICE4.1 (nominal 1.0 degree EW x 1.0 degrees NS, tripolar north of 65N, '+\
        'equatorial refinement to 1/3 degree from 10S to 10 N, cosine dependent NS south of 25S); '+\
        'land: MOSES2 (1.875 degree EW x 1.25 degree NS, 4 levels'
    elif model=='ACCESS1.3'or model=='ACCESS1-3':
        return 'ACCESS1.3 2011. '+\
        'Atmosphere: AGCM v1.0 (N96 grid-point, 1.875 degrees EW x approx 1.25 degree NS, 38 levels); '+\
        'ocean: NOAA/GFDL MOM4p1 (nominal 1.0 degree EW x 1.0 degrees NS, tripolar north of 65N, '+\
         'equatorial refinement to 1/3 degree from 10S to 10 N, cosine dependent NS south of 25S, 50 levels); '+\
        'sea ice: CICE4.1 (nominal 1.0 degree EW x 1.0 degrees NS, tripolar north of 65N, '+\
        'equatorial refinement to 1/3 degree from 10S to 10 N, cosine dependent NS south of 25S); '+\
        'land: CABLE1.0 (1.875 degree EW x 1.25 degree NS, 6 levels'
    elif model=='ACCESS1.4'or model=='ACCESS1-4':
        return 'ACCESS1.4 2014. '+\
        'Atmosphere: AGCM v1.0 (N96 grid-point, 1.875 degrees EW x approx 1.25 degree NS, 38 levels); '+\
        'ocean: NOAA/GFDL MOM4p1 (nominal 1.0 degree EW x 1.0 degrees NS, tripolar north of 65N, '+\
         'equatorial refinement to 1/3 degree from 10S to 10 N, cosine dependent NS south of 25S, 50 levels); '+\
        'sea ice: CICE4.1 (nominal 1.0 degree EW x 1.0 degrees NS, tripolar north of 65N, '+\
        'equatorial refinement to 1/3 degree from 10S to 10 N, cosine dependent NS south of 25S); '+\
        'land: CABLE2.0 (1.875 degree EW x 1.25 degree NS, 6 levels'
    elif model=='ACCESS2.0'or model=='ACCESS2-0':
        return 'ACCESS2.0 2019. '+\
        'Atmosphere: AGCM v1.0 (N96 grid-point, 1.875 degrees EW x approx 1.25 degree NS, 38 levels); '+\
        'ocean: NOAA/GFDL MOM4p1 (nominal 1.0 degree EW x 1.0 degrees NS, tripolar north of 65N, '+\
        'equatorial refinement to 1/3 degree from 10S to 10 N, cosine dependent NS south of 25S, 50 levels); '+\
        'sea ice: CICE4.1 (nominal 1.0 degree EW x 1.0 degrees NS, tripolar north of 65N, '+\
        'equatorial refinement to 1/3 degree from 10S to 10 N, cosine dependent NS south of 25S); '+\
        'land: CABLE2.0 (1.875 degree EW x 1.25 degree NS, 6 levels'
    else: return model +': unknown source'

