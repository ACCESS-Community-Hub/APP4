import numpy as np
import csv
import glob
import os
import re
import sys
import argparse
import subprocess as sp
import netCDF4
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import json
import shutil
import psutil
import datetime
import cdtime
cdtime.DefaultCalendar=cdtime.GregorianCalendar

parser = argparse.ArgumentParser(description='Checks existence, compliance, and data quality of CMOR output')
parser.add_argument('--compliance', dest='compliance', default=False, action='store_true',
                    help='Complaince check CMOR output files')
parser.add_argument('--data', dest='data', default=False, action='store_true',
            help='Check data for validity of values wrt data request')
parser.add_argument('--timeseries', dest='timeseries', default=False, action='store_true',
            help='Create a netCDF4 time series file for each variable')
args = parser.parse_args()

successlists=os.environ.get('SUCCESS_LISTS')
multilist=os.environ.get('MULTI_LIST')
completedlist=os.environ.get('COMPLETED_LIST')
variablemapsdir=os.environ.get('VARIABLE_MAPS')
exptoprocess=os.environ.get('EXP_TO_PROCESS')
joboutputfile=os.environ.get('JOB_OUTPUT')
jobscriptsdir=os.environ.get('JOB_SCRIPTS')
app_dir=os.environ.get('APP_DIR')
qc_dir=os.environ.get('QC_DIR')
out_dir=os.environ.get('OUT_DIR')
pub_dir=os.environ.get('PUB_DIR')
cmip6tables=os.environ.get('CMIP6_TABLES')
experimentstable=os.environ.get('EXPERIMENTS_TABLE')
tabletoprocess=os.environ.get('TABLE_TO_PROCESS')

online_plot_dir=os.environ.get('ONLINE_PLOT_DIR')
if os.environ.get('PRINTERRORS').lower() in ['true','yes']: printerrors=True
else: printerrors=False
if os.environ.get('PUBLISH').lower() in ['true','yes']: publish=True
else: publish=False
if publish: summaryncdir=qc_dir
else: summaryncdir='{}/qc_files'.format(out_dir)
if publish and not args.compliance:
    sys.exit('E: if publish is on, compliance must also be on')

# Read in success and fail lists
try:
    successes=[]
    try:
        if os.path.exists('{}/{}_success.csv'.format(successlists,exptoprocess)):
            with open('{}/{}_success.csv'.format(successlists,exptoprocess),'r') as s:
                reader=csv.reader(s, delimiter=',')
                for row in reader:
                    try: row[0]
                    except: row='#'
                    if row[0].startswith('#'): pass
                    else:
                        if tabletoprocess == 'all': successes.append(row)
                        elif row[0] == tabletoprocess: successes.append(row)
            successes.sort()
            s.close()
        else: print 'W: no success list - will assume all variables failed'
    except: raise Exception('E: failed to read file {}/{}_success.csv'.format(successlists,exptoprocess))
    try:
        if os.path.exists(experimentstable):
            with open(experimentstable,'r') as f:
                reader=csv.reader(f, delimiter=',')
                for row in reader:
                    try: row[0]
                    except: row='#'
                    if row[0].startswith('#'): pass
                    elif row[0] == exptoprocess: 
                        jsonfile=row[2]
                        dreq=row[3]
                        refdate=row[4]
            f.close()
            with open(jsonfile) as j:
                json_dict=json.load(j)
            j.close()
            outpath_root=json_dict['outpath']
            dreq
        else: raise Exception('E: no experiments table found at {}'.format(experimentstable))
    except:
        if os.path.exists(experimentstable): raise Exception('E: check experiment and experiments table')
        else: raise Exception('E: no experiments table found at {}'.format(experimentstable))
except Exception, e: sys.exit('E: failed to read in required files: {}'.format(e))

if args.compliance:
    print '\n#### {}: CHECKING CMOR FILES FOR COMPLIANCE'.format(tabletoprocess)
    try:
        cmorerrs=[]
        cmorpasses=0
        publishable=[]
        for table,var,tstart,tend,cmorfile in successes:
            try:
                cmorerr=0
                try: outputp=sp.check_output(['PrePARE','--table-path',cmip6tables,cmorfile],stderr=sp.STDOUT)
                except Exception, e: outputp=e.output
                #try: outputc=sp.check_output(['cfchecks',cmorfile],stderr=sp.STDOUT)
                #except Exception, e: outputc=e.output
                #if outputp.find('This file is compliant with the CMIP6 specification and can be published in ESGF') == -1:
                if table == 'Oclim': print 'Oclim files failed PrePARE, allowing for now: {}'.format(cmorfile)
                elif outputp.find('Number of file with error(s): 0') == -1:
                    if printerrors: print outputp
                    else: print 'W: File {} failed PrePARE'.format(cmorfile)
                    cmorerr+=1
                else: pass
                #if outputc.find('ERRORS detected: 0') == -1:
                #    if printerrors: print outputc
                #    else: print 'W: File {} did not pass cfchecks'.format(cmorfile)
                #else: pass
                if cmorerr == 0: 
                    if publish:
                        if not os.path.exists(os.path.dirname(cmorfile.replace(outpath_root,pub_dir))):
                            os.makedirs(os.path.dirname(cmorfile.replace(outpath_root,pub_dir)))
                        shutil.copy2(cmorfile,cmorfile.replace(outpath_root,pub_dir))
                        publishable.append(cmorfile.replace(outpath_root,pub_dir))
                        os.remove(cmorfile)
                        insuccesslist=0
                        with open(completedlist,'a+') as c:
                            reader=csv.reader(c, delimiter=',')
                            for row in reader:
                                if row[0] == table and row[1] == var and row[2] == tstart and row[3] == tend: insuccesslist=1
                                else: pass
                            if insuccesslist == 0:
                                c.write('{},{},{},{}\n'.format(table,var,tstart,tend))
                                #print 'added \'{},{},{},{}\' to {}'.format(table,var,tstart,tend,os.path.basename(completedlist))
                            #elif insuccesslist == 1: print '\'{},{},{},{}\' already in {}...skipping'.format(table,var,tstart,tend,os.path.basename(completedlist))
                        c.close()
                    cmorpasses+=1
                else: 
                    cmorerrs.append(cmorfile)
            except Exception, e:
                print 'E - {}: failed to check file {} for compliance: {}'.format(tabletoprocess,cmorfile,e)
                cmorerrs.append(cmorfile)
        if cmorerrs == []: 
            if publish: print 'S - {}: all ({}) CMOR files passed compliance and were copied to {}'.format(tabletoprocess, cmorpasses,pub_dir)
            else: print 'S - {}: all ({}) CMOR files passed compliance'.format(tabletoprocess, cmorpasses)
        else: 
            if publish: print 'S - {}: ({}) files passed compliance and were copied to {}'.format(tabletoprocess,cmorpasses,pub_dir)
            else: print 'S - {}: ({}) files passed compliance'.format(tabletoprocess,cmorpasses)
            print 'W - {}: ({}) files did not pass compliance'.format(tabletoprocess,len(cmorerrs))
            print cmorerrs
    except Exception, e: print 'E - {}: failed to check CMOR files for compliance: {}'.format(tabletoprocess,e)

if args.data:
    print '\n#### {}: CHECKING DATA SETS FOR VALIDITY'.format(tabletoprocess)
    try:
        datapass=0
        dataerrs=0
        novalids=[]
        cmordirs=[]
        for table,var,tstart,tend,cmorfile in successes:
            if publish:
                cmorfile=cmorfile.replace(outpath_root,pub_dir)
            cmordir=os.path.dirname(cmorfile)
            if not cmordir in cmordirs:
                if publish and cmorfile in publishable: cmordirs.append(cmordir)
                elif publish and not cmorfile in publishable: pass
                else: cmordirs.append(cmordir)
        for cmordir in cmordirs:
            files=glob.glob('{}/*.nc'.format(cmordir))
            files.sort()
            cmorfile=files[0]
            table=os.path.basename(files[0]).split('_')[1]
            var=os.path.basename(files[0]).split('_')[0]
            try:
                datamsg=''
                novalidminmax=0
                novalidmean=0
                try: valid_min; del valid_min
                except: pass
                try: valid_max; del valid_max
                except: pass
                try: ok_min_mean_abs; del ok_min_mean_abs
                except: pass
                try: ok_max_mean_abs; del ok_max_mean_abs
                except: pass
                with open(dreq,'r') as d:
                    reader=csv.reader(d, delimiter='\t')
                    for row in reader:
                        if (row[0] == table) and (row[12] == var):
                            try: valid_min=float(re.sub('[^.0-9e-]','',row[23]))
                            except: pass
                            try: valid_max=float(re.sub('[^.0-9e-]','',row[24]))
                            except: pass
                            try: ok_min_mean_abs=float(re.sub('[^.0-9e-]','',row[25]))
                            except: pass
                            try: ok_max_mean_abs=float(re.sub('[^.0-9e-]','',row[26]))
                            except: pass
                d.close()
                try: valid_min; valid_max
                except: 
                    novalids.append([table,var])
                    continue
                dataset=netCDF4.Dataset(cmorfile)
                try: vals=dataset.variables[var][:]
                except KeyError: 
                    var=os.path.basename(cmorfile).split('_')[0]
                    vals=dataset.variables[var][:]
                try: 
                    if not vals.min() >= valid_min: datamsg+='W: min value not in valid range (min={}, valid_min={})\n'.format(vals.min(),valid_min)
                    else: pass
                except: novalidminmax=1
                try: 
                    if not (vals.max() <= valid_max) and (novalidminmax == 0): datamsg+='W: max value not in valid range (max={}, valid_max={})\n'.format(vals.max(),valid_max)
                    else: pass
                except: novalidminmax=1
                try: 
                    if not (vals.mean() >= ok_min_mean_abs) or not (vals.mean() <= ok_max_mean_abs) and (novalidminmax == 0): datamsg+='W: mean value not in valid range (mean={}, valid_range=({}, {}))\n'.format(vals.mean(),ok_min_mean_abs,ok_max_mean_abs)
                    else: pass
                except: novalidmean=1
                if datamsg == '':
                    if novalidminmax == 0: datapass+=1
                    else: novalids.append([table,var])
                elif datamsg != '':
                    print '{}, {}, ({},{}):'.format(table,var,tstart,tend)
                    print datamsg
                    dataerrs+=1
            except Exception, e: novalids.append([table,var])
            try: dataset.close()
            except: pass
        if dataerrs == 0 and len(novalids) == 0: print 'S - {}: all ({}) sets of data passed validity checks'.format(tabletoprocess,datapass)
        elif dataerrs == 0 and len(novalids) != 0: 
            print 'S - {}: ({}) sets of data passed validity checks; ({}) could not be checked for validity; none failed'.format(tabletoprocess,datapass,len(novalids))
            print '  W - {}: variables that could not be checked for validity:'.format(tabletoprocess)
            print '  {}'.format(novalids)
        else: 
            print 'W - {}: ({}) sets of data did not pass validity checks; ({}) did pass; ({}) could not be checked'.format(tabletoprocess,dataerrs,datapass,len(novalids))
            print '  W - {}: variables that could not be checked for validity:'.format(tabletoprocess)
            print '  {}'.format(novalids)
    except Exception, e: print 'E - {}: failed to check data sets for validity: {}'.format(tabletoprocess,e)

def saveonlineplot(online_plot_dir,exptoprocess,table,plotout):
    if not os.path.exists('{}/{}/{}'.format(online_plot_dir,exptoprocess,table)):
        os.makedirs('{}/{}/{}'.format(online_plot_dir,exptoprocess,table))
    os.chmod('{}/{}/{}'.format(online_plot_dir,exptoprocess,table),0755)
    if os.path.exists('{}/{}/{}/{}'.format(online_plot_dir,exptoprocess,table,plotout)):
        os.remove('{}/{}/{}/{}'.format(online_plot_dir,exptoprocess,table,plotout))
    plt.savefig('{}/{}/{}/{}'.format(online_plot_dir,exptoprocess,table,plotout))
    os.chmod('{}/{}/{}/{}'.format(online_plot_dir,exptoprocess,table,plotout),0755)

if args.timeseries:
    print '\n#### {}: PRODUCING DATA SUMMARY NETCDF4 FILES AND PLOTS'.format(tabletoprocess)
    try:
        if not os.path.exists(summaryncdir):
            os.makedirs(summaryncdir)
        #if not publish:
        #    if len(glob.glob('{}/*'.format(summaryncdir))) >= 1:
        #        for file in glob.glob('{}/*'.format(summaryncdir)):
        #            os.remove(file)
        ncfilecount=0
        plotcount=0
        summaryerr=0
        cmordirs=[]
        for table,var,tstart,tend,cmorfile in successes:
            if publish:
                cmorfile=cmorfile.replace(outpath_root,pub_dir)
            cmordir=os.path.dirname(cmorfile)
            if not cmordir in cmordirs:
                if publish and cmorfile in publishable: cmordirs.append(cmordir)
                elif publish and not cmorfile in publishable: pass
                else: cmordirs.append(cmordir)
        for cmordir in cmordirs:
            try:
                files=glob.glob('{}/*.nc'.format(cmordir))
                files.sort()
                cmorfile=files[0]
                table=os.path.basename(files[0]).split('_')[1]
                var=os.path.basename(files[0]).split('_')[0]
                outpath_structure=cmordir.replace('{}/'.format(pub_dir),'')
                endtimes=[]
                starttimes=[]
                scalar=0
                try:
                    for file in files:
                        starttimes.append(int(re.split('[_\.\-]',os.path.basename(file))[-3]))
                        endtimes.append(int(re.split('[_\.\-]',os.path.basename(file))[-2]))
                    #if endtimes[0]-starttimes[0] == 1:
                        #shortcut=True
                except: scalar=1
                if scalar == 0:
                    starttime=starttimes[0]
                    endtime=endtimes[-1]
                    filebase=os.path.basename(files[0]).replace(os.path.basename(files[0]).split('_')[-1],'')[0:-1]
                else:
                    filebase=os.path.basename(files[0]).replace('.nc','')
                if publish: 
                    outbase='{}/{}/{}'.format(qc_dir,outpath_structure,filebase)
                    if not os.path.exists('{}/{}'.format(summaryncdir,outpath_structure)):
                        os.makedirs('{}/{}'.format(summaryncdir,outpath_structure))
                else: outbase='{}/{}'.format(summaryncdir,filebase)
                #
                # plotting first timestep
                if scalar == 0: plotout='{}_{}.png'.format(outbase,starttime)
                else: plotout='{}.png'.format(outbase)
                if os.path.exists(plotout):
                    os.remove(plotout)
                plotfile=files[0]
                plotfileopen=netCDF4.Dataset(plotfile,'r')
                plotvar=plotfileopen.variables[var]
                dims=plotvar.shape
                units=plotvar.units
                plt.figure()
                if len(dims) == 4:
                    plotdata=plotvar[0].mean(axis=plotvar.ndim-2)
                    plt.imshow(plotdata,origin='lower')
                    if dims[1] == 50: plt.gca().invert_yaxis()
                    plt.colorbar()
                elif len(dims) == 3:
                    if plotvar.dimensions[0] != 'time':
                        plotdata=plotvar[:].mean(axis=plotvar.ndim-1)
                        plt.imshow(plotdata,origin='lower')
                        if dims[0] == 50: plt.gca().invert_yaxis()
                    else: plt.imshow(plotvar[0,:,:],origin='lower')
                    plt.colorbar()
                elif len(dims) == 2:
                    plt.imshow(plotvar[:,:],origin='lower')
                    plt.colorbar()
                else:
                    plt.plot(np.array(plotvar[:]))
                if scalar == 0: plt.suptitle('Plot of {}, time {} ({}) at first timestep'.format(var,starttime,units))
                else: plt.suptitle('Plot of {} ({}) at first timestep'.format(var,units))
                plt.savefig(plotout)
                if publish: saveonlineplot(online_plot_dir,exptoprocess,table,os.path.basename(plotout))
                plt.close()
                plotfileopen.close()
                plotcount+=1
                #
                # make summary netcdf file of timeseries
                if scalar == 0: ncout='{}_{}-{}.qc.nc'.format(outbase,starttime,endtime)
                else: ncout='{}.qc.nc'.format(outbase)
                if os.path.exists(ncout):
                    os.remove(ncout)
                dsout=netCDF4.Dataset(ncout,'w',format='NETCDF4_CLASSIC')
                if len(files) >= 2:
                    dsinfirst=netCDF4.Dataset(files[0])
                    dsinmulti=netCDF4.MFDataset(files)
                else: 
                    dsinfirst=netCDF4.Dataset(files[0])
                    dsinmulti=netCDF4.Dataset(files[0])
                if 'time' in dsinfirst.variables.items()[0]:
                    for dname, the_dim in dsinfirst.dimensions.items():
                            dsout.createDimension(dname, len(the_dim) if not the_dim.isunlimited() else None)
                else:
                    dsout.createDimension('value',1)
                # create netcdf variables
                if 'time' in dsinfirst.variables.items()[0]:
                    for v_name, varin in dsinmulti.variables.items():
                        if v_name == 'time':
                            timeout = dsout.createVariable(v_name, varin.dtype, varin.dimensions)
                        elif v_name == var:
                            maskout = dsout.createVariable(v_name, varin.dtype, varin.dimensions[0])
                            meanout = dsout.createVariable('{}_mean'.format(v_name), varin.dtype, varin.dimensions[0])
                            minout = dsout.createVariable('{}_min'.format(v_name), varin.dtype, varin.dimensions[0])
                            maxout = dsout.createVariable('{}_max'.format(v_name), varin.dtype, varin.dimensions[0])
                            stdevout = dsout.createVariable('{}_stdev'.format(v_name), varin.dtype, varin.dimensions[0])
                            masknumout = dsout.createVariable('{}_masknum'.format(v_name), varin.dtype, varin.dimensions[0])
                else:
                    for v_name, varin in dsinmulti.variables.items():
                        if v_name == var:
                            maskout = dsout.createVariable(v_name, varin.dtype, 'value')
                            meanout = dsout.createVariable('{}_mean'.format(v_name), varin.dtype, 'value')
                            minout = dsout.createVariable('{}_min'.format(v_name), varin.dtype, 'value')
                            maxout = dsout.createVariable('{}_max'.format(v_name), varin.dtype, 'value')
                            stdevout = dsout.createVariable('{}_stdev'.format(v_name), varin.dtype, 'value')
                            masknumout = dsout.createVariable('{}_masknum'.format(v_name), varin.dtype, 'value')
                # copy attributes for maskout and time
                for v_name, varin in dsinfirst.variables.items():
                    if v_name == 'time':
                        for k in varin.ncattrs():
                            if k in ['_FillValue']: continue
                            timeout.setncattr(k,varin.getncattr(k))
                    elif v_name == var:
                        try:
                            for att in varin.ncattrs():
                                if att in ['missing_value']:
                                    missingval=varin.getncattr(att)
                            missingval
                        except:
                            for att in varin.ncattrs():
                                if att in ['_FillValue']:
                                    missingval=varin.getncattr(att)
                        for k in varin.ncattrs():
                            if k in ['_FillValue']: continue
                            maskout.setncattr(k,varin.getncattr(k))
                        maskout.setncattr('original_data',cmorfile)
                dsinfirst.close()
                dsinmulti.close()
                # create data arrays
                vartime=[]
                varempty=[]
                varmean=[]
                varmin=[]
                varmax=[]
                varstdev=[]
                varmasknum=[]
                vard=[]
                varn=[]
                for file in files:
                    dsin=netCDF4.Dataset(file)
                    if 'time' in dsin.variables.items()[0]:
                        for v_name, varin in dsin.variables.items():
                            if v_name == 'time':
                                vartime=np.ma.append(vartime,varin[:])
                            elif v_name == var:
                                vard=np.ma.masked_invalid(varin[:])
                                varn=vard.ndim
                                varempty=np.ma.append(varempty,np.ma.masked_all(0))
                                # mean
                                varmean=np.ma.append(varmean,vard.mean(axis=tuple(range(1, varn))))
                                # min
                                varmin=np.ma.append(varmin,vard.min(axis=tuple(range(1, varn))))
                                # max
                                varmax=np.ma.append(varmax,vard.max(axis=tuple(range(1, varn))))
                                # stdev
                                varstdev=np.ma.append(varstdev,vard.std(axis=tuple(range(1, varn))))
                                # missing
                                varmasknum=np.ma.append(varmasknum,np.ma.count_masked(vard,axis=tuple(range(1, varn))))
                    else:
                        for v_name, varin in dsin.variables.items():
                            if v_name == var:
                                vard=np.ma.masked_invalid(varin[:])
                                varn=vard.ndim
                                varempty=np.ma.masked_all(0)
                                # mean
                                varmean=vard.mean()
                                # min
                                varmin=vard.min()
                                # max
                                varmax=vard.max()
                                # stdev
                                varstdev=vard.std()
                                # missing
                                varmasknum=np.ma.count_masked(vard)
                    del vard
                    dsin.close()
                # write data arrays to netcdf output file
                try: timeout[:] = vartime
                except: pass
                maskout[:] = varempty
                meanout[:] = varmean
                minout[:] = varmin
                maxout[:] = varmax
                stdevout[:] = varstdev
                masknumout[:] = varmasknum
                dsout.close()
                ncfilecount+=1
                if scalar != 0: continue
                # Create timeseries plots
                if scalar == 0: 
                    ncmeanout='{}_{}-{}_meanminmax.png'.format(outbase,starttime,endtime)
                    ncstdevout='{}_{}-{}_stdev.png'.format(outbase,starttime,endtime)
                    ncmasknumout='{}_{}-{}_masknum.png'.format(outbase,starttime,endtime)
                else: 
                    ncmeanout='{}_meanminmax.png'.format(outbase)
                    ncstdevout='{}_stdev.png'.format(outbase)
                    ncmasknumout='{}_masknum.png'.format(outbase)
                if os.path.exists(ncmeanout):
                    os.remove(ncmeanout)
                if os.path.exists(ncstdevout):
                    os.remove(ncstdevout)
                if os.path.exists(ncmasknumout):
                    os.remove(ncmasknumout)
                vartimerel=[]
                for time in vartime:
                    vartimerel.append(datetime.datetime(int(refdate),1,1)+datetime.timedelta(time))
                # mean,min,max
                plt.figure()
                plt.plot(vartimerel,varmean)
                plt.plot(vartimerel,varmin)
                plt.plot(vartimerel,varmax)
                plt.ylabel(units)
                plt.suptitle('Timeseries of {} mean/min/max, time {}-{}'.format(var,starttime,endtime))
                plt.savefig(ncmeanout)
                if publish: saveonlineplot(online_plot_dir,exptoprocess,table,os.path.basename(ncmeanout))
                plt.close()
                plotcount+=1
                # stdev
                plt.figure()
                plt.plot(vartimerel,varstdev)
                plt.ylabel(units)
                plt.suptitle('Timeseries of {} stdev, time {}-{}'.format(var,starttime,endtime))
                plt.savefig(ncstdevout)
                if publish: saveonlineplot(online_plot_dir,exptoprocess,table,os.path.basename(ncstdevout))
                plt.close()
                plotcount+=1
                # masknum
                plt.figure()
                plt.plot(vartimerel,varmasknum)
                plt.ylabel(units)
                plt.suptitle('Timeseries of {} masknum, time {}-{}'.format(var,starttime,endtime))
                plt.savefig(ncmasknumout)
                if publish: saveonlineplot(online_plot_dir,exptoprocess,table,os.path.basename(ncmasknumout))
                plt.close()
                plotcount+=1
            except Exception, e: 
                print 'E - {}: failed to create summary netCDF4 files and plots for data in output directory {}: {}'.format(tabletoprocess,cmordir,e)
                summaryerr+=1
            try: plotfileopen.close()
            except: pass
            try: dsin.close()
            except: pass
            try: dsinfirst.close()
            except: pass
            try: dsmulti.close()
            except: pass
            try: dsout.close()
            except: pass
        if summaryerr == 0: print 'S - {}: all ({}) summary netCDF4 files and all ({}) plots created in {}'.format(tabletoprocess,ncfilecount,plotcount,summaryncdir)
        else: 
            print 'W - {}: ({}) variables were unable to have their netCDF summary files and plots created'.format(tabletoprocess,summaryerr)
            print 'S - {}: ({}) summary netCDF4 files and ({}) plots created in {}'.format(tabletoprocess,ncfilecount,plotcount,summaryncdir)
    except Exception, e: print 'E - {}: failed to create summary netCDF4 files: {}'.format(tabletoprocess,e)
