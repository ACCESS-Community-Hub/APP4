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

#Compute monthly average of daily values (for 2D variables)
def monthAve(var,time):
    datelist = time.asComponentTime()
    monthave = []
    #get month, value of first date
    month = datelist[0].month
    val_sum = var[0,:,:]
    count = 1
    #loop over all dates
    for index, d in enumerate(datelist[1:]):
        if d.month==month: #same month 
            count += 1
            val_sum += var[index+1,:,:] #add value to sum
        else: #new month
            monthave.append(val_sum/count) #calculate average for previous month and add to list
            val_sum = var[index+1,:,:] #get first value for the new month
            count = 1
            month = d.month
    #append last month to list 
    monthave.append(val_sum/count)
    monthave_shape = np.shape(np.array(monthave))
    print(f'monthly ave has shape: {monthave_shape}')
    return np.array(monthave)

#calculate a climatology of monthly means
def monthClim(var,time,vals_wsum,clim_days):
    datelist = time.asComponentTime()
    print('calculating climatology...')
    #loop over all dates,
    try:
        for index, d in enumerate(datelist):
            m = d.month #month
            #add the number of days for this month
            dummy,days = calendar.monthrange(d.year,m)
#            print index,d,m,days
            clim_days[m-1] += days
            #add values weighted by the number of days in this month
            vals_wsum[m-1,:] += (var[index,:]*days)
    except Exception as e:
        print(e)
        raise

    return vals_wsum,clim_days

#calculate a average monthly means (average is vals_wsum/clim_days)
def ave_months(var,time,vals_wsum,clim_days):
    datelist = time.asComponentTime()
    print('calculating climatology...')
    #loop over all dates,
    try:
        for index, d in enumerate(datelist):
            m = d.month #month
            #add the number of days for this month
            dummy,days = calendar.monthrange(d.year,m)
#            print index,d,m,days
            clim_days += days
            #add values weighted by the number of days in this month
            vals_wsum[:] += (var[index,:]*days)
    except Exception as e:
        print(e)
        raise

    return vals_wsum,clim_days     

def zonal_mean(var):
    return var.mean(axis=-1)

def optical_depth(lbplev,var):
    '''
    Calculates the optical depth. First saves all parts of var into a dictionary
    and then sums the conents together.
    '''
    idx = lbplev-1

    vars = {}
    for i in range(len(var)):
        vars[f"var{i}"] = np.array(var[i][:,idx,:,:])

    vout = sum(vars.values())

    return vout

# -----------------------------------------------------------
'''
Functions to calculate mass transports
'''
#List of strings giving the names of the straits used in the mass transports across lines
def getTransportLines():
    lines = ['barents_opening','bering_strait','canadian_archipelago','denmark_strait',\
        'drake_passage','english_channel','pacific_equatorial_undercurrent',\
        'faroe_scotland_channel','florida_bahamas_strait','fram_strait','iceland_faroe_channel',\
        'indonesian_throughflow','mozambique_channel','taiwan_luzon_straits','windward_passage']
    return lines

def geticeTransportLines():
    ilines = ['fram_strait','canadian_archipelago','barents_opening','bering_strait']
    return ilines

#Calculate the mass trasport across a line
#either i_start=i_end and the line goes from j_start to j_end 
#or j_start=j_end and the line goes from i_start to i_end
#var is either the x or y mass transport depending on the line
#
def transAcrossLine(var,i_start,i_end,j_start,j_end):
    if i_start==i_end or j_start==j_end:
        try:
            trans = var[:,:,j_start:j_end+1,i_start:i_end+1].sum(1).sum(1).sum(1) #sum each axis apart from time (3d)
        except:
            trans = var[:,j_start:j_end+1,i_start:i_end+1].sum(1).sum(1) #sum each axis apart from time (2d)
        #print var[0,0,j_start:j_end+1,i_start:i_end+1]
        return trans
    else: 
        raise Exception('ERROR: Transport across a line needs to be calculated for a single value of i or j')

#Calculates the mass transports across lines
#for each line requested in cmip5
#
def lineTransports(tx_trans,ty_trans):
    #print tx_trans[0,:,34,64], tx_trans[0,:,34,64].sum()
    #initialise array
    transports = np.zeros([len(tx_trans[:,0,0,0]),len(getTransportLines())],dtype=np.float32)
    #0 barents opening
    transports[:,0] = transAcrossLine(ty_trans,292,300,271,271)
    transports[:,0] += transAcrossLine(tx_trans,300,300,260,271)
    #1 bering strait
    transports[:,1] = transAcrossLine(ty_trans,110,111,246,246)
    #2 canadian archipelago
    transports[:,2] = transAcrossLine(ty_trans,206,212,285,285)
    transports[:,2] += transAcrossLine(tx_trans,235,235,287,288)
    #3 denmark strait
    transports[:,3] = transAcrossLine(tx_trans,249,249,248,251)
    transports[:,3] += transAcrossLine(ty_trans,250,255,247,247)
    #4 drake passage
    transports[:,4] = transAcrossLine(tx_trans,212,212,32,49)
    #5 english channel is unresolved by the access model
    #6 pacific equatorial undercurrent
    #specified down to 350m not the whole depth
    transports[:,6] = transAcrossLine(np.ma.masked_where(\
        tx_trans[:,0:25,:]<0,tx_trans[:,0:25,:]),124,124,128,145)
    #7 faroe scotland channel    
    transports[:,7] = transAcrossLine(ty_trans,273,274,238,238)
    transports[:,7] += transAcrossLine(tx_trans,274,274,232,238)
    #8 florida bahamas strait
    transports[:,8] = transAcrossLine(ty_trans,200,205,192,192)
    #9 fram strait
    transports[:,9] = transAcrossLine(tx_trans,267,267,279,279)
    transports[:,9] += transAcrossLine(ty_trans,268,284,278,278)
    #10 iceland faroe channel
    transports[:,10] = transAcrossLine(ty_trans,266,268,243,243)
    transports[:,10] += transAcrossLine(tx_trans,268,268,240,243)
    transports[:,10] += transAcrossLine(ty_trans,269,272,239,239)
    transports[:,10] += transAcrossLine(tx_trans,272,272,239,239)
    #11 indonesian throughflow
    transports[:,11] = transAcrossLine(tx_trans,31,31,117,127)
    transports[:,11] += transAcrossLine(ty_trans,35,36,110,110)
    transports[:,11] += transAcrossLine(ty_trans,43,44,110,110)
    transports[:,11] += transAcrossLine(tx_trans,46,46,111,112)
    transports[:,11] += transAcrossLine(ty_trans,47,57,113,113)
    #12 mozambique channel    
    transports[:,12] = transAcrossLine(ty_trans,320,323,91,91)
    #13 taiwan luzon straits
    transports[:,13] = transAcrossLine(ty_trans,38,39,190,190)
    transports[:,13] += transAcrossLine(tx_trans,40,40,184,188)
    #14 windward passage
    transports[:,14] = transAcrossLine(ty_trans,205,206,185,185)
    return transports


def gridfile(xy):
    '''
    SG: Added function for code that was repeated in the following 3 functions.
    '''
    gridfile = xr.open_dataset(f'{ancillary_path}/cice_grid_20101208.nc')
    if xy=='y':
        #for y_vel use length dx
        L = np.float32(gridfile.hun[:]/100) #grid cell length in m (from cm)
    elif xy=='x':
        #for x_vel use length dy
        L = np.float32(gridfile.hue[:]/100) #grid cell length in m (from cm)
    else: 
        raise Exception('need to supply value either \'x\' or \'y\' for ice Transports')
    gridfile.close()
    return L

#Calculate ice_mass transport. assumes only one time value
def iceTransport(ice_thickness,vel,xy):
    L = gridfile(xy)
    ice_mass = ice_density * ice_thickness * vel * L
    return ice_mass

#Calculate ice_mass transport. assumes only one time value
def snowTransport(snow_thickness,vel,xy):
    L = gridfile(xy)
    snow_mass = snow_density * snow_thickness * vel * L
    return snow_mass

def iceareaTransport(ice_fraction,vel,xy):
    L = gridfile(xy)
    ice_area = ice_fraction * vel * L
    return ice_area

def icelineTransports(ice_thickness,velx,vely):
    #ice mass transport across fram strait
    tx_trans = iceTransport(ice_thickness,velx,'x').filled(0)
    ty_trans = iceTransport(ice_thickness,vely,'y').filled(0)
    transports = fill_transports(tx_trans, ty_trans)
    return transports

def snowlineTransports(snow_thickness,velx,vely):
    #ice mass transport across fram strait
    tx_trans = snowTransport(snow_thickness,velx,'x').filled(0)
    ty_trans = snowTransport(snow_thickness,vely,'y').filled(0)
    transports = fill_transports(tx_trans, ty_trans)
    return transports

def icearealineTransports(ice_fraction,velx,vely):
    #ice mass transport across fram strait
    tx_trans = iceareaTransport(ice_fraction,velx,'x').filled(0)
    ty_trans = iceareaTransport(ice_fraction,vely,'y').filled(0)
    transports = fill_transports(tx_trans, ty_trans)
    return transports

def fill_transports(tx_trans, ty_trans):
    transports = np.zeros([len(tx_trans[:,0,0]),len(geticeTransportLines())],dtype=np.float32)
    #0 fram strait
    transports[:,0] = transAcrossLine(tx_trans,267,267,279,279)
    transports[:,0] += transAcrossLine(ty_trans,268,284,278,278)
    #1 canadian archipelago
    transports[:,1] = transAcrossLine(ty_trans,206,212,285,285)
    transports[:,1] += transAcrossLine(tx_trans,235,235,287,288)
    #2 barents opening
    transports[:,2] = transAcrossLine(ty_trans,292,300,271,271)
    transports[:,2] += transAcrossLine(tx_trans,300,300,260,271)
    #3 bering strait
    transports[:,3] = transAcrossLine(ty_trans,110,111,246,246)
    return transports
#--------------------------------------------------------------------

def msftbarot(psiu,tx_trans):
    drake_trans=transAcrossLine(tx_trans,212,212,32,49)
    #loop over times
    for i,trans in enumerate(drake_trans):
        #offset psiu by the drake passage transport at that time
        psiu[i,:] = psiu[i,:]+trans
    return psiu

def calcHeights(zgrid):
    '''
    Gets the ACCESS model orography from a file and then calculates the heights of each atmospheric 
    level at any lat and lon.
    '''
    a_vals,b_vals,dummy1,dummy2 = getHybridLevels(zgrid)
    z = len(a_vals)

    orog_file = xr.open_dataset(f'{ancillary_path}cm2_orog.nc')
    orog = np.float32(orog_file.fld_s00i033[0,:,:])
    orog_file.close()
    [y,x] = np.shape(orog)
    
    height = np.zeros([z,y,x],dtype=np.float32)
    for i, a in enumerate(a_vals):
        height[i,:] = a+b_vals[i]*orog[:]

    return height


#Calculate gas/aerosol concentration from mixing ratio
# Concentration is mass mixing ratio* density
# Density = P*rd/temp
def calcConcentration(theta,pressure,var):
    fac1 = pressure/rd
    #convert theta (potential temp) to absolute temp
    fac2 = (1.0/theta)*((p_0/pressure)**(rd/cp)) 
    con = var*fac1*fac2
    return con

#Calculate gas/aerosol concentration from mixing ratio
# Concentration is mass mixing ratio* density
# Density = pressure*rd/temp
def calcConcentration_temp(temp,pressure,var):
    con_t = var*pressure/rd/temp
    return con_t

#returns a and b coefficients of the hybrid height levels
#zgrid is either theta or rho, specifying which levels to use
def getHybridLevels(zgrid,mod_levs):
    #
    #THETA 38 models levels
    #
    a_theta_38 = [ 2.00003476841366E+01,    8.00013607367031E+01,    1.79999123674816E+02,     
        3.20001487465965E+02,    5.00000601144540E+02,    7.20000390195303E+02,    9.80000854619977E+02,
        1.27999806893725E+03,    1.61999988411616E+03,    1.99999844919269E+03,    2.42000161513650E+03,
        2.88000153098418E+03,    3.37999819673933E+03,    3.91999946337258E+03,    4.50000140540479E+03,
        5.12000009735717E+03,    5.77999946471790E+03,    6.47999950749217E+03,    7.22000022568546E+03,
        8.00000161930355E+03,    8.81999976286921E+03,    9.67999858187221E+03,    1.05799980763193E+04,
        1.15199982462175E+04,    1.24999990915741E+04,    1.35200006123970E+04,    1.45807996818154E+04,
        1.56946398824287E+04,    1.68753114372833E+04,    1.81386261933466E+04,    1.95030103694309E+04,
        2.09901875904244E+04,    2.26260817484206E+04,    2.44582854036469E+04,    2.65836402305355E+04,
        2.92190802158774E+04,    3.29086930549692E+04,    3.92548335760000E+04]    
    b_theta_38 = [ 0.99771646211176, 0.990881509969853, 0.97954257198, 
        0.963777064495466, 0.943695500215124, 0.919438385933089, 
        0.891177995326388, 0.85911834077804, 0.823493502026291, 
        0.784570542447329, 0.74264622270758, 0.698050213339009, 
        0.651142687869227, 0.602314440652335, 0.551988701332177, 
        0.500619965219651, 0.448693382264548, 0.396725773497944, 
        0.345265282996542, 0.294891377882665, 0.24621507084444, 
        0.199878208516722, 0.156554224084806, 0.116947873779608, 
        0.0817952368776663, 0.0518637157011444, 0.0279368176797157, 
        0.0107164793473159, 0.00130179089664594, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    #
    #THETA 85 models levels
    #
    a_theta_85 = [1.99999985e+01,    5.33333350e+01,    1.00000035e+02,    1.60000005e+02,    2.33333330e+02,     
        3.20000010e+02,    4.19999960e+02,    5.33333350e+02,    6.59999925e+02,    7.99999940e+02,    9.53333650e+02,
        1.11999995e+03,    1.30000020e+03,    1.49333355e+03,    1.70000000e+03,    1.91999955e+03,    2.15333305e+03,
        2.39999965e+03,    2.65999935e+03,    2.93333300e+03,    3.21999975e+03,    3.51999960e+03,    3.83333340e+03,
        4.16000030e+03,    4.49999945e+03,    4.85333340e+03,    5.21999960e+03,    5.59999975e+03,    5.99333300e+03,
        6.39999935e+03,    6.81999965e+03,    7.25333305e+03,    7.69999955e+03,    8.16000085e+03,    8.63333950e+03,
        9.12000700e+03,    9.62001950e+03,    1.01333685e+04,    1.06600795e+04,    1.12001610e+04,    1.17536385e+04,
        1.23205460e+04,    1.29009345e+04,    1.34948805e+04,    1.41024775e+04,    1.47238785e+04,    1.53592365e+04,
        1.60088150e+04,    1.66729030e+04,    1.73519000e+04,    1.80462905e+04,    1.87567035e+04,    1.94838870e+04,
        2.02287760e+04,    2.09925265e+04,    2.17765070e+04,    2.25823920e+04,    2.34121620e+04,    2.42681800e+04,
        2.51532255e+04,    2.60705880e+04,    2.70241095e+04,    2.80182610e+04,    2.90582275e+04,    3.01500185e+04,
        3.13005360e+04,    3.25177105e+04,    3.38105945e+04,    3.51895240e+04,    3.66662375e+04,    3.82540290e+04,
        3.99679265e+04,    4.18248535e+04,    4.38438330e+04,    4.60462085e+04,    4.84558310e+04,    5.10993480e+04,
        5.40064245e+04,    5.72100150e+04,    6.07467035e+04,    6.46569585e+04,    6.89855240e+04,    7.37817680e+04,
        7.91000140e+04,    8.50000000e+04]
    b_theta_85 = [ 0.997741275086705, 0.99398240804637, 0.988731908699181, 
        0.982001705304877, 0.973807111131548, 0.964166854107058, 
        0.953103076620072, 0.940641297950216, 0.926810489201526, 
        0.911642970668284, 0.895174468640199, 0.877444259574719, 
        0.858494762021074, 0.838372034595903, 0.817125449992819, 
        0.794807785858902, 0.771475140430401, 0.747187188302877, 
        0.722006922200895, 0.696000659671116, 0.669238286587803, 
        0.641793010753004, 0.613741369675018, 0.585163459855583, 
        0.556142775803562, 0.526765930463967, 0.49712337151126, 
        0.467308599396241, 0.437418726741245, 0.407554201410229, 
        0.377818817181541, 0.348319895568412, 0.31916809951228, 
        0.290477393373827, 0.262365117818521, 0.234952656271582, 
        0.208363419272906, 0.182725622550246, 0.158169250547604, 
        0.134828751469694, 0.112841468088624, 0.0923482445245987, 
        0.0734933451640281, 0.0564245779947533, 0.0412940283317234, 
        0.0282576550958496, 0.0174774681160646, 0.00912047120300465, 
        0.0033616981617407, 0.000384818409824841, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    #
    #RHO 38 model levels
    #
    a_rho_38 = [ 9.99820611180720E+00,    4.99988815257512E+01,    1.30000232353639E+02,     
        2.49998333112114E+02,    4.10001034767890E+02,    6.10000486354252E+02,    8.50000613354558E+02,
        1.13000141576881E+03,    1.44999896811365E+03,    1.81000112135578E+03,    2.21000002452851E+03,
        2.64999960311518E+03,    3.12999985711579E+03,    3.65000078653035E+03,    4.20999846587549E+03,
        4.81000074611793E+03,    5.44999977629097E+03,    6.12999948187794E+03,    6.84999986287886E+03,
        7.61000091929372E+03,    8.40999872563917E+03,    9.25000113288192E+03,    1.01300002900553E+04,
        1.10500001226425E+04,    1.20100006306438E+04,    1.30100018140589E+04,    1.40504001467072E+04,
        1.51377197819288E+04,    1.62849736970543E+04,    1.75069688153084E+04,    1.88208202441304E+04,
        2.02465989799277E+04,    2.18081366321642E+04,    2.35421835760337E+04,    2.55209608543495E+04,
        2.79013582604648E+04,    3.10638885981650E+04,    3.60817633154846E+04]
    b_rho_38 = [ 0.998858128870133, 0.99429627308894, 0.985203884572179, 
        0.971644051455324, 0.953709854688251, 0.931527464394211, 
        0.90525305068319, 0.875074548755025, 0.841211628160905, 
        0.803914038358856, 0.763464495026274, 0.720175811165861, 
        0.674392534131669, 0.626490534323373, 0.576877345793496, 
        0.52599078844921, 0.47430136562304, 0.422309896216029, 0.370548862782066, 
        0.319582070920665, 0.270004882299482, 0.222443261047234, 
        0.177555424379176, 0.136030233980892, 0.0985881076061108, 
        0.0659807860541821, 0.038982389975312, 0.0183146872952392, 
        0.00487210933346041, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    #
    #RHO 85 model levels
    #
    a_rho_85 = [1.00000035e+01,    3.66666710e+01,    7.66666680e+01,    1.30000020e+02,    1.96666625e+02,     
        2.76666670e+02,    3.69999985e+02,    4.76666655e+02,    5.96666595e+02,    7.29999975e+02,    8.76667050e+02,
        1.03666680e+03,    1.20999965e+03,    1.39666645e+03,    1.59666635e+03,    1.81000020e+03,    2.03666630e+03,
        2.27666635e+03,    2.52999950e+03,    2.79666660e+03,    3.07666680e+03,    3.37000010e+03,    3.67666650e+03,
        3.99666600e+03,    4.33000030e+03,    4.67666685e+03,    5.03666650e+03,    5.40999925e+03,    5.79666595e+03,
        6.19666660e+03,    6.60999950e+03,    7.03666635e+03,    7.47666630e+03,    7.93000020e+03,    8.39666805e+03,
        8.87666900e+03,    9.37000900e+03,    9.87669400e+03,    1.03967240e+04,    1.09301245e+04,    1.14769040e+04,
        1.20370880e+04,    1.26107360e+04,    1.31979075e+04,    1.37986790e+04,    1.44131780e+04,    1.50415575e+04,
        1.56840300e+04,    1.63408590e+04,    1.70124015e+04,    1.76990995e+04,    1.84014970e+04,    1.91202910e+04,
        1.98563315e+04,    2.06106555e+04,    2.13845210e+04,    2.21794495e+04,    2.29972770e+04,    2.38401710e+04,
        2.47106985e+04,    2.56119110e+04,    2.65473530e+04,    2.75211895e+04,    2.85382485e+04,    2.96041230e+04,
        3.07252815e+04,    3.19091190e+04,    3.31641525e+04,    3.45000635e+04,    3.59278850e+04,    3.74601375e+04,
        3.91109820e+04,    4.08963900e+04,    4.28343390e+04,    4.49450165e+04,    4.72510240e+04,    4.97775895e+04,
        5.25528905e+04,    5.56082240e+04,    5.89783550e+04,    6.27018310e+04,    6.68212455e+04,    7.13836375e+04,
        7.64408910e+04,    8.20500070e+04]
    b_rho_85 = [ 0.998870317837861, 0.995860954349445, 0.991355422277142, 
        0.985363933974855, 0.977900121158012, 0.968980988270818, 
        0.958626984732528, 0.946861936587443, 0.933713093743072, 
        0.919211083274653, 0.90338992880522, 0.886287195763284, 0.86794369819746, 
        0.848403612683474, 0.827714699862468, 0.805927948677273, 
        0.783098012936815, 0.75928260663815, 0.734543106256622, 
        0.708944126120491, 0.682553850296594, 0.655443784110242, 
        0.627688837840218, 0.599367326718778, 0.57056082582786, 0.54135468360624, 
        0.51183735600282, 0.482100778280808, 0.452240226064344, 
        0.422354451035778, 0.392545730622496, 0.362919497662862, 
        0.333584775043384, 0.304653875166948, 0.276242576002774, 
        0.248470108785575, 0.221458737823793, 0.195334209369465, 
        0.17022603408794, 0.146266031653404, 0.123590464169748, 
        0.102338524880911, 0.0826521044402928, 0.0646774270996039, 
        0.04856467862905, 0.0344676779549617, 0.0225453993391377, 
        0.0129621701410525, 0.00588912822920522, 0.00150532135773878, 0, 0, 0, 0, 
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
        0, 0, 0, 0, 0, 0, 0]
    #
    # 38 model levels (ESM)
    if zgrid=='theta' and mod_levs==38:
        a_vals = a_theta_38
        b_vals = b_theta_38
        min_vals = [0] + a_rho_38[1:]
        max_vals = a_rho_38[1:] + [4.24279038365e+04]
        min_b = [1.0] + b_rho_38[1:]
        max_b = b_rho_38[1:] + [0.0]
    if zgrid=='rho' and mod_levs==38:
        a_vals = a_rho_38
        b_vals = b_rho_38
        min_vals = [0] + a_theta_38[:-1]
        max_vals = a_theta_38
        min_b = [1.0] + b_theta_38[:-1]
        max_b = b_theta_38
    #
    # 85 model levels (CM2)
    if zgrid=='theta' and mod_levs==85:
        a_vals = a_theta_85
        b_vals = b_theta_85
        min_vals = [0] + a_rho_85[1:]
        max_vals = a_rho_85[1:] + [8.7949993e+04]
        min_b = [1.0] + b_rho_85[1:]
        max_b = b_rho_85[1:] + [0.0]
    if zgrid=='rho' and mod_levs==85:
        a_vals = a_rho_85
        b_vals = b_rho_85
        min_vals = [0] + a_theta_85[:-1]
        max_vals = a_theta_85
        min_b=[1.0] + b_theta_85[:-1]
        max_b = b_theta_85
    #
    return a_vals,b_vals,np.column_stack((min_vals, max_vals)),np.column_stack((min_b,max_b))

def areacella(nlat):
    if nlat == 145:
        f = xr.open_dataset(f'{ancillary_path}esm_areacella.nc')
    elif nlat == 144:
        f = xr.open_dataset(f'{ancillary_path}cm2_areacella.nc')
    vals = np.float32(f.areacella[:,:])
    f.close()
    return vals

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

#level values and bounds for soil depths in moses
def mosesSoilLevels():
    levels = np.array([0.05, 0.225 ,0.675, 2.000])
    boundmin = np.array([0.0,0.10, 0.350 ,1.0])
    boundmax = np.array([0.10, 0.350 ,1.0, 3.0])
    bounds = np.column_stack((boundmin,boundmax))
    return levels,bounds

#level values and bounds for soil depths in cable
def cableSoilLevels():
    levels = np.array([ 0.011 ,  0.051 ,  0.157 ,  0.4385,  1.1855,  2.872 ],dtype=np.float32)
    boundmin = np.array([0,0.022,  0.08 ,  0.234,  0.643,  1.728],dtype=np.float32)
    boundmax = np.array([ 0.022,  0.08 ,  0.234,  0.643,  1.728,  4.6  ],dtype=np.float32)
    bounds = np.column_stack((boundmin,boundmax))
    return levels,bounds

def calc_mrfso(var,model):
    '''
    Calculate values for mrfso using values in var (field8230) and 
    soil level thicknesses for cable or moses.
    '''
    if model=='cable':
        lev,bounds = cableSoilLevels()
    elif model=='moses':
        lev,bounds = mosesSoilLevels()
    else: 
        raise Exception('model passed must be cable or moses for calculation of mrfso')
    thickness = bounds[:,1] - bounds[:,0]
    #compute weighted sum of var*thickness along z-axis of var
    [tn,zn,yn,xn] = np.shape(var)
    out = np.ma.zeros([tn,yn,xn],dtype=np.float32)
    #Note using density of water not ice (1000kg/m^3)
    for z in range(len(thickness)):
        out += var[:,z,:,:] * thickness[z] * 1000
    return out

def calc_global_ave_ocean(var,rho_dzt,area_t):
    mass = rho_dzt * area_t
    print(np.shape(var))
    try: 
        vnew = np.average(var,axis=(1,2,3),weights=mass)
    except: 
        vnew = np.average(var,axis=(1,2),weights=mass[:,0,:,:])
    return vnew
 
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

def calc_hemi_seaice_vars(var, tarea, lat):
    '''
    Assign var, tarea, and lat to a numpy array for later use.
    '''
    lat = np.array(lat,dtype='float32')
    var = np.array(np.ma.filled(var[0],0),dtype='float64')
    tarea = np.array(tarea,dtype='float64')
    return lat, var, tarea

def calc_hemi_seaice_area_vol(var, tarea, lat, hemi):
    '''
    Calculate the hemi seaice area volume.
    '''
    lat, var, tarea = calc_hemi_seaice_vars(var, tarea, lat)

    nhlati = np.where(lat>=0.)
    shlati = np.where(lat<0.)
    var = var[:] * tarea[:]

    if hemi.find('north') != -1:
        var = var[nhlati]
    elif hemi.find('south') != -1:
        var = var[shlati]
    varn = np.sum(var)
    return varn

def calc_hemi_seaice_extent(aice, tarea, lat, hemi):
    '''
    Calculate the hemi seaice extents.
    '''
    lat, aice, tarea = calc_hemi_seaice_vars(aice, tarea, lat)

    nhlatiext = np.where((aice>=.15)&(aice<=1.)&(lat>=0.))
    shlatiext = np.where((aice>=.15)&(aice<=1.)&(lat<0.))

    if hemi.find('north') != -1:
        var = tarea[nhlatiext]
    elif hemi.find('south') != -1:
        var = tarea[shlatiext]
    varn = np.sum(var)
    return varn

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

def topsoil(var):
    soil = var[:,0,:,:]+var[:,1,:,:]+var[:,2,:,:]*.012987
    return soil
    
def topsoil_tsl(var):
    soil_tsl = (var[:,0,:,:]+var[:,1,:,:])/2
    return soil_tsl

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

def ocean_floor(var):
    var = np.ma.asarray(var)
    lv = (~np.isnan(var)).sum(axis=1)-1
    vout = np.take_along_axis(var,lv[:,None,:,:],axis=1).squeeze()
    return vout
    
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

def maskSeaIce(var,sic):
    #return np.ma.masked_where((np.array(var[1])==0).__and__(landFrac()==0),var[0])
    vout = np.ma.masked_where(sic==0,var)
    return vout

def sithick(hi,aice):
    aice = np.ma.masked_where(aice<=1e-3,aice)
    vout = hi/aice
    return vout

def sisnconc(sisnthick):
    #sisnthick=np.ma.masked_where(sisnthick<=1e-2,sisnthick)
    vout = 1-np.exp(-0.2*330*sisnthick)
    return vout

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


def plev19():
    #PP not urgetn but probably should be defined in config file??
    # I imagine the levels are always the same for CMIP6
    # but what if someone wants different pressure levels?
    plev19 = np.array([100000, 92500, 85000, 70000, 
        60000, 50000, 40000, 30000, 
        25000, 20000, 15000, 10000, 
        7000, 5000, 3000, 2000, 
        1000, 500, 100],dtype=np.float32)
    plev19min = np.array([-1.0000e+02, 3.0000e+02, 7.5000e+02, 1.5000e+03, 2.5000e+03,
        4.0000e+03, 6.0000e+03, 8.5000e+03, 1.2500e+04, 1.7500e+04, 
        2.2500e+04, 2.7500e+04, 3.5000e+04, 4.5000e+04, 5.5000e+04, 
        6.5000e+04, 7.7500e+04, 8.8750e+04, 9.6250e+04],dtype=np.float32)
    plev19max = np.array([3.0000e+02, 7.5000e+02, 1.5000e+03, 2.5000e+03,
        4.0000e+03, 6.0000e+03, 8.5000e+03, 1.2500e+04, 1.7500e+04, 
        2.2500e+04, 2.7500e+04, 3.5000e+04, 4.5000e+04, 5.5000e+04, 
        6.5000e+04, 7.7500e+04, 8.8750e+04, 9.6250e+04, 1.0375e+05],dtype=np.float32)
    plev19b = np.column_stack((plev19min,plev19max))
    return np.flip(plev19), plev19b

def calc_clwvi(var):
    '''
    calculate clwvi by integrating over water collumn.
    assumes only one time step.
    '''
    press = var[0]
    #out = np.ma.zeros([1]+list(press.shape[2:]),dtype=np.float32)
    #for z in range(press.shape[1]-1):
    #    mix = np.ma.zeros(press.shape[2:],dtype=np.float32)        
    #    for v in var[1:]:
    #        mix[:,:] += v[0,z,:,:]
    #    out[:,:] += mix * (press[0,z,:] - press[0,z+1,:])

    # Rewrote the above code to take advantage of NumPy's vectorization capabilities.
    # Removed the intermediate array mix and directly summed the contributions from 
    # each variable along the vertical axis.
    # np.zeros is default dtype=np.float32 so I removed that.
    # Code needs testing to prove it's the same. 
    out = np.ma.zeros(press.shape[2:])
    #PP can use xarray of numpy shift as press.shift(secondim=-1) - press.shift(seconddim=1)
    # might had to adjust or cut off the first/last term
    dz = press[0, :-1, :] - press[0, 1:, :]
    for v in var[1:]:
        out += v[0, :-1, :] * dz
    vout = out.sum() * 0.101972
    return vout

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
def extract_lvl(var, lvl):
    vout = var[:,lvl,:,:]
    return vout

def tropoz(o3plev,o3mod,troppres,plev):
    '''
    SG: Added some optimization to this function, not 100% sure what the function
    is doing though.
    '''
    t,z,y,x = np.shape(o3plev)
    print(t,z,y,x)

    # Removed the following 2 lines from the for loop
    # They are calculating the same thing each time
    plev_rev = plev[::-1]
    o3plev_rev = o3plev[:, ::-1, :, :]
    # Potential optimization for o3mod_max = np.max(o3mod, axis=1)
    # Needs testing as I'm not 100% sure it's the same thing
    #o3mod_max = np.max(o3mod, axis=1) 

    vout = np.zeros([t,y,x],dtype=np.float32)
    for k in range(t):
        for i in range(x):
            for j in range(y):
                vint = np.interp(troppres[k,j,i], plev_rev, o3plev_rev[k, :, j, i])
                vout[k,j,i] = np.max(o3mod[k,0,j,i]) - vint
    return vout

def toz(o3):
    # PP I'm wondering how efficient are these. If using xarray you would be reading the values rather 
    # than just selecting (I'm assuming the surface level) Using sel would be clearer
    if len(np.shape(o3)) == 4:
        return o3[:,0,:,:]
    else:
        return o3

def mc_gravity(var):
    t,z,y,x = np.shape(var)
    if z == 85:
        a_theta,b_theta,dim_val_bounds_theta,b_bounds_theta = getHybridLevels('theta',85)
    elif z == 38:
        a_theta,b_theta,dim_val_bounds_theta,b_bounds_theta = getHybridLevels('theta',38)
    else: sys.exit('levels undefined in mc_gravity')
    grav_h = np.ma.zeros([len(a_theta)],dtype=np.float32)
    for i in range(len(a_theta)):
        grav_h[i] = 9.8*(R_e / (R_e + a_theta[i])) ** 2
    mc = np.ma.zeros([t,z,y,x],dtype=np.float32)
    for k in range(z):
        mc[:,k,:,:] = var[:,k,:,:] / grav_h[k]
    return mc
        
def det_landtype(mod):
    """PP this should become a dictionary , possibly loaded form a json file
       so definitons can be checked outside the code
    """
    if mod == 'typebare':
        landtype='bare_ground'
    elif mod == 'typeburnt':
        landtype='burnt_vegetation'
    elif mod == 'typec3crop':
        landtype='crops_of_c3_plant_functional_types'
    elif mod == 'typec3natg':
        landtype='natural_grasses_of_c3_plant_functional_types'
    elif mod == 'typec3pastures':
        landtype='pastures_of_c3_plant_functional_types'
    elif mod == 'typec4pft':
        landtype='c4_plant_functional_types'
    elif mod == 'typec4crop':
        landtype='crops_of_c4_plant_functional_types'
    elif mod == 'typec4natg':
        landtype='natural_grasses_of_c4_plant_functional_types'
    elif mod == 'typec4pastures':
        landtype='pastures_of_c4_plant_functional_types'
    elif mod == 'typec4pft':
        landtype='c4_plant_functional_types'
    elif mod == 'typecloud':
        landtype='cloud'
    elif mod == 'typecrop':
        landtype='crops'
    elif mod == 'typefis':
        landtype='floating_ice_shelf'
    elif mod == 'typegis':
        landtype='grounded_ice_sheet'
    elif mod == 'typeland':
        landtype='land'
    elif mod == 'typeli':
        landtype='land_ice'
    elif mod == 'typemp':
        landtype='sea_ice_melt_pond'
    elif mod == 'typenatgr':
        landtype='natural_grasses'
    elif mod == 'typenwd':
        landtype='herbaceous_vegetation'
    elif mod == 'typepasture':
        landtype='pastures'
    elif mod == 'typepdec':
        landtype='primary_deciduous_trees'
    elif mod == 'typepever':
        landtype='primary_evergreen_trees'
    elif mod == 'typeresidual':
        landtype='residual'
    elif mod == 'typesdec':
        landtype='secondary_decidous_trees'
    elif mod == 'typesea':
        landtype='sea'
    elif mod == 'typesever':
        landtype='secondary_evergreen_trees'
    elif mod == 'typeshrub':
        landtype='shrubs'
    elif mod == 'typesi':
        landtype='sea_ice'
    elif mod == 'typesirdg':
        landtype='sea_ice_ridges'
    elif mod.startswith('typetree'):
        landtype='trees'
    elif mod == 'typeveg':
        landtype='vegetation'
    elif mod == 'typewetla':
        landtype='wetland'
    else:
        landtype='unknown'
    return landtype

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


# SG: Not really sure why this plotting function exists when it's 
# better/easier to plot data in a notebook (i.e. ARE)
# I've modified it to work better anyway.
def plotVar(outpath,ret,cmip_table,vcmip,parent_source_id,experiment_id):
    '''
    Give a sample plot of a variable
    '''

# Using with to open the file ensures that the file is closed properly even if an error occurs.
    with xr.open_dataset(ret) as f:

        var = f.fld_s00i033
        dims = var.shape  # don't need this
        units = var.units

        print('plotting...')

        #3D field: 
        #plot lat-lon plots at different levels, 
        #for first time step only
        if var.ndim == 4:

            fig, axes = plt.subplots(nrows=2, ncols=2)
            axes = axes.flatten()

            #3D- plot contour at first z level, 
            axes[0].set_title('first level')
            axes[0].imshow(var[0,0,:,:],origin='lower')
            axes[0].colorbar()

            #last level
            axes[1].set_title('last level')
            axes[1].imshow(var[0,-1,:,:],origin='lower')
            axes[1].colorbar()

            #middle level
            lev = dims[1] // 2
            axes[2].set_title('middle level')
            axes[2].imshow(var[0,lev,:,:],origin='lower')
            axes[2].colorbar()

            #average
            axes[3].set_title('ave over z')
            ave = np.ma.average(var[0,:],axis=0)
            axes[3].imshow(ave,origin='lower')
            axes[3].colorbar()

            plt.tight_layout()

        #2D field:
        elif var.ndim == 3:

            plt.figure()
            #plot lat-lon contour at first time
            plt.imshow(var[0,:,:],origin='lower')
            plt.colorbar()

        #2D field:(fixed time variables)
        elif var.ndim == 2:
            
            plt.figure()
            #plot lat-lon contours 
            plt.imshow(var[:,:],origin='lower')
            plt.colorbar()
        else: 
        #assume scalar
        #plot line plot, variable vs. time
            plt.plot(np.array(var[:]))

        #Set a super title for whole image
        plt.suptitle('Plot of '+vcmip +' ('+units+')')    

        # Make output directory and save figure:
        folder = outpath+'/plots/'+parent_source_id+'/'+experiment_id+'/'+cmip_table
        if not os.path.isdir(folder):
            os.makedirs(folder)
        plt.savefig(folder+'/'+vcmip+'.png')
        figloc = folder+'/'+vcmip+'.png'
        print(f"saved figure for variable '{vcmip}': {figloc}")
        #cleanup
        plt.clf()

#-------------------------------------------
# Time functions that Paola could have bypassed by now:
'''
def calcRefDate(time):
    ref = re.search('\d{4}-\d{2}-\d{2}', time.units).group(0).split('-')
    vout = datetime.date(int(ref[0]), int(ref[1]), int(ref[2]))
    return vout

# returns days in month for time variable
def daysInMonth(time):
    tvals = time[:]
    refdate = calcRefDate(time)
    if len(tvals)==1:
        d = datetime.timedelta(int(tvals[0]))+refdate
        dummy,days = calendar.monthrange(d.year,d.month)
    else:
        days = []
        for t in tvals:
            d = datetime.timedelta(int(t))+refdate
            dummy,d = calendar.monthrange(d.year,d.month)
            days.append(d)
    return days   

#convert daily time values to monthly
#returns time values, 
#and the min and max time for each month (bounds)
#assumes time is in the gregorian calandar, relative to date reference date
def day2mon(tvals,ref):
    datelist = []
    for t in tvals: 
        datelist.append(datetime.datetime(ref,1,1)+ datetime.timedelta(t))
    #get month, value of first date
    month = datelist[0].month
    val_sum = tvals[0]
    count = 1
    tmonth = [] #List of average time value for each month
    tmin = [] #list of start of month bounds
    tmax = [] #list of end of month bounds
    tmin.append(np.floor(tvals[0]))
    #loop over all dates
    for index, d in enumerate(datelist[1:]):
        if d.month==month: #same month 
            count += 1
            val_sum += tvals[index+1] #add value to sum
        else: #new month
            tmonth.append(val_sum/count) #calculate average for previous month and add to list
            val_sum = tvals[index+1] #get first value for the new month
            count = 1
            month = d.month
            tmin.append(np.floor(tvals[index+1]))
            tmax.append(np.floor(tvals[index+1]))
    #append last month to list 
    tmonth.append(val_sum/count)
    print(tmonth)
    tmax.append(np.floor(tvals[index+1])+1)
    return np.array(tmonth), tmin,tmax

# SG: CAN THE FOLLOWING 2 FUNCTIONS BE COMBINED IN ANY WAY??
def mon2yr(tvals,refString):
    tyr = []
    tmin = []
    tmax = []
    starttime = cdtime.reltime(tvals[0],refString).tocomp(cdtime.DefaultCalendar)
    year = starttime.year
    tmin.append(cdtime.comptime(year,1,1).torel(refString,cdtime.DefaultCalendar).value)
    for i in tvals:
        yearnew = cdtime.reltime(i,refString).tocomp(cdtime.DefaultCalendar).year
        if yearnew==year:
            pass
        else:
            tyr.append(cdtime.comptime(year,1,1).torel(refString,cdtime.DefaultCalendar).value+(cdtime.comptime(year+1,1,1).torel(refString,cdtime.DefaultCalendar).value - cdtime.comptime(year,1,1).torel(refString,cdtime.DefaultCalendar).value)/2)
            year = yearnew
            tmin.append(cdtime.comptime(year,1,1).torel(refString,cdtime.DefaultCalendar).value)
            tmax.append(cdtime.comptime(year,1,1).torel(refString,cdtime.DefaultCalendar).value)
    tyr.append(cdtime.comptime(year,1,1).torel(refString,cdtime.DefaultCalendar).value + (cdtime.comptime(year+1,1,1).torel(refString,cdtime.DefaultCalendar).value - cdtime.comptime(year,1,1).torel(refString,cdtime.DefaultCalendar).value)/2)
    tmax.append(cdtime.comptime(year+1,1,1).torel(refString,cdtime.DefaultCalendar).value)
    tyr_ar = np.array(tyr)
    print(f'tvals: {tyr_ar}')
    return tyr_ar,tmin,tmax

def yrpoint(tvals,refString):
    tyr = []
    tmin = []
    tmax = []
    starttime = cdtime.reltime(tvals[0],refString).tocomp(cdtime.DefaultCalendar)
    year = starttime.year
    tmin.append(cdtime.comptime(year,1,1).torel(refString,cdtime.DefaultCalendar).value)
    for i,tval in enumerate(tvals):
        yearnew = cdtime.reltime(tval,refString).tocomp(cdtime.DefaultCalendar).year
        if yearnew==year:
            pass
        else:
            tyr.append(tvals[i-1])
            year = yearnew
            tmin.append(cdtime.comptime(year,1,1).torel(refString,cdtime.DefaultCalendar).value)
            tmax.append(cdtime.comptime(year,1,1).torel(refString,cdtime.DefaultCalendar).value)
    tyr.append(tvals[-1])
    tmax.append(cdtime.comptime(year+1,1,1).torel(refString,cdtime.DefaultCalendar).value)
    tyr_ar = np.array(tyr)
    print(f'tvals: {tyr_ar}')
    return tyr_ar,tmin,tmax
'''


'''
SG: These are unused functions:

#def daysInMonth(time):
#    tvals=time[:]
#        
#    days=np.zeros([len(tvals)])
#    for i,t in enumerate(tvals):    
#        d=datetime.timedelta(int(t))+refdate
#        dummy,days[i]=calendar.monthrange(d.year,d.month)
#    print days
#    return days[0]

idea use to reduce memory usage (doesn't seem to make a difference)
def calcBurdens(variables):
    theta = variables[0]
    pressure = variables[1]
    burden = calcBurden(theta,pressure,variables[2])
    for i in range(3,len(variables)):
        burden += calcBurden(theta,pressure,variables[i])
    return burden

calculates an average over southern or northern hemisphere
assumes 4D (3D+ time) variable
def hemi_ga(var,vol,southern):
    dims=var.shape
    mid=150
    if southern:
        total_vol=vol[:,:mid,:],.sum()
        v_ga=v[:,:mid,:]*vol[:,:mid,:]).sum()/total_vol
    else:
        total_vol=vol[:,mid:,:],.sum()
        v_ga=v[:,mid:,:]*vol[:,mid:,:]).sum()/total_vol
    return v_ga

def mcu_gravity(var):
    t,z,y,x = np.shape(var)
    a_theta_85,b_theta_85,dim_val_bounds_theta_85,b_bounds_theta_85 = getHybridLevels('theta',85)
    grav_h = np.ma.zeros([len(a_theta_85)],dtype=np.float32)
    for i in range(len(a_theta_85)):
        grav_h[i] = 9.8 * (R_e / (R_e + a_theta_85[i])) ** 2
    mcu = np.ma.zeros([t,z,y,x],dtype=np.float32)
    for k in range(z):
        mcu[:,k,:,:] = var[:,k,:,:] / grav_h[k]
    return mcu

#deprecated funtion, do not use. Use rho_from_theta instead
def rf_eos(S,T,P):
    t1 = [ 9.99843699e+2, 7.35212840e+0, -5.45928211e-2, 3.98476704e-4 ]
    s1 = [ 2.96938239e+0, -7.23268813e-3, 2.12382341e-3 ]
    p1 = [ 1.04004591e-2, 1.03970529e-7, 5.18761880e-6, -3.24041825e-8, -1.23869360e-11 ]

    t2 = [ 1.0, 7.28606739e-3, -4.60835542e-5, 3.68390573e-7, 1.80809186e-10 ]
    s2 = [ 2.14691708e-3, -9.27062484e-6, -1.78343643e-10, 4.76534122e-6, 1.63410736e-9 ]
    p2 = [ 5.30848875e-6, -3.03175128e-16, -1.27934137e-17 ]

    Pn = t1[0] + t1[1]*T + t1[2]*T**2 + t1[3]*T**3 + s1[0]*S + s1[1]*S*T + s1[2]*S**2 \
      + p1[0]*P + p1[1]*P*T**2 + p1[2]*P*S + p1[3]*P**2 + p1[4]*P**2*T**2

    Pd = t2[0] + t2[1]*T + t2[2]*T**2 + t2[3]*T**3 + t2[4]*T**4 \
      + s2[0]*S + s2[1]*S*T + s2[2]*S*T**3 + s2[3]*np.sqrt(S**3) + s2[4]*np.sqrt(S**3)*T**2 \
      + p2[0]*P + p2[1]*P**2*T**3 + p2[2]*P**3*T
    return Pn/Pd
'''
