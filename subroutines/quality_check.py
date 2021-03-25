import numpy as np
import csv
import glob
import os
import pathlib2
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
import multiprocessing as mp
cdtime.DefaultCalendar=cdtime.GregorianCalendar

parser = argparse.ArgumentParser(description='Checks existence, compliance, and data quality of CMOR output')
parser.add_argument('--compliance', dest='compliance', default=False, action='store_true',
                    help='Complaince check CMOR output files')
parser.add_argument('--timeseries', dest='timeseries', default=False, action='store_true',
            help='Create a netCDF4 time series file for each variable')
args = parser.parse_args()

try: ncpus=int(os.environ.get('NCPUS'))
except: ncpus=1
successlists=os.environ.get('SUCCESS_LISTS')
multilist=os.environ.get('MULTI_LIST')
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

online_plot_dir=os.environ.get('ONLINE_PLOT_DIR')
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
                        successes.append(row)
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

def compliance_check(success):
    try:
        table,var,tstart,tend,cmorfile=success
        cmorfilebase=os.path.basename(cmorfile)
        cmorfiledir=os.path.dirname(cmorfile)
        cmorerr=0
        try: outputp=sp.check_output(['PrePARE','--table-path',cmip6tables,cmorfile],stderr=sp.STDOUT)
        except Exception, e: outputp=e.output
        if table == 'Oclim': pass #Oclim files all fail PrePARE
        elif outputp.find('Number of file with error(s): 0') == -1:
            cmorerr=1
        else: pass
        #print(cmorfilebase,cmorerr)
        if cmorerr == 0: 
            if publish:
                pathlib2.Path.mkdir(pathlib2.Path(cmorfiledir.replace(outpath_root,pub_dir)),parents=True,exist_ok=True)
                return [cmorfile,0]
        else: 
            return [cmorfile,1,e]
    except Exception, e:
        return [cmorfile,2,e]

def saveonlineplot(online_plot_dir,exptoprocess,table,plotout):
    pathlib2.Path.mkdir(pathlib2.Path('{}/{}/{}'.format(online_plot_dir,exptoprocess,table)),parents=True,exist_ok=True)
    os.chmod('{}/{}/{}'.format(online_plot_dir,exptoprocess,table),0755)
    if os.path.exists('{}/{}/{}/{}'.format(online_plot_dir,exptoprocess,table,plotout)):
        os.remove('{}/{}/{}/{}'.format(online_plot_dir,exptoprocess,table,plotout))
    plt.savefig('{}/{}/{}/{}'.format(online_plot_dir,exptoprocess,table,plotout))
    os.chmod('{}/{}/{}/{}'.format(online_plot_dir,exptoprocess,table,plotout),0755)

def create_timeseries(cmordir):
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
        except: scalar=1
        if scalar == 0:
            starttime=starttimes[0]
            endtime=endtimes[-1]
            filebase=os.path.basename(files[0]).replace(os.path.basename(files[0]).split('_')[-1],'')[0:-1]
        else:
            filebase=os.path.basename(files[0]).replace('.nc','')
        if publish: 
            outbase='{}/{}/{}'.format(qc_dir,outpath_structure,filebase)
            pathlib2.Path.mkdir(pathlib2.Path('{}/{}'.format(summaryncdir,outpath_structure)),parents=True,exist_ok=True)
        else: outbase='{}/{}'.format(summaryncdir,filebase)
        #
        # plotting first timestep
        if scalar == 0: plotout='{}_{}.png'.format(outbase,starttime)
        else: plotout='{}.png'.format(outbase)
        if os.path.exists(plotout): os.remove(plotout)
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
        if scalar == 0: plt.suptitle('{}, time {} ({})'.format(var,starttime,units))
        else: plt.suptitle('{} ({})'.format(var,units))
        plt.savefig(plotout)
        if publish: saveonlineplot(online_plot_dir,exptoprocess,table,os.path.basename(plotout))
        plt.close()
        plotfileopen.close()
        #
        # make summary netcdf file of timeseries
        if scalar == 0: ncout='{}_{}-{}.qc.nc'.format(outbase,starttime,endtime)
        else: ncout='{}.qc.nc'.format(outbase)
        if os.path.exists(ncout): os.remove(ncout)
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
                       if att in ['missing_value']: missingval=varin.getncattr(att)
                    missingval
                except:
                    for att in varin.ncattrs():
                        if att in ['_FillValue']: missingval=varin.getncattr(att)
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
        if scalar != 0: return [cmordir,0]
        # Create timeseries plots
        ncmeanout='{}_{}-{}_meanminmax.png'.format(outbase,starttime,endtime)
        ncstdevout='{}_{}-{}_stdev.png'.format(outbase,starttime,endtime)
        ncmasknumout='{}_{}-{}_masknum.png'.format(outbase,starttime,endtime)
        if os.path.exists(ncmeanout): os.remove(ncmeanout)
        if os.path.exists(ncstdevout): os.remove(ncstdevout)
        if os.path.exists(ncmasknumout): os.remove(ncmasknumout)
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
        # stdev
        plt.figure()
        plt.plot(vartimerel,varstdev)
        plt.ylabel(units)
        plt.suptitle('Timeseries of {} stdev, time {}-{}'.format(var,starttime,endtime))
        plt.savefig(ncstdevout)
        if publish: saveonlineplot(online_plot_dir,exptoprocess,table,os.path.basename(ncstdevout))
        plt.close()
        # masknum
        plt.figure()
        plt.plot(vartimerel,varmasknum)
        plt.ylabel(units)
        plt.suptitle('Timeseries of {} masknum, time {}-{}'.format(var,starttime,endtime))
        plt.savefig(ncmasknumout)
        if publish: saveonlineplot(online_plot_dir,exptoprocess,table,os.path.basename(ncmasknumout))
        plt.close()    
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
        return [cmordir,0]
    except Exception, e: 
        return [cmordir,1,e]

def pool_handler_comp(successes):
    p=mp.Pool(ncpus)
    results_comp=p.imap_unordered(compliance_check,((success) for success in successes))
    p.close()
    p.join()
    return results_comp

def pool_handler_timeser(cmordirs):
    p=mp.Pool(ncpus)
    results_timeser=p.imap_unordered(create_timeseries,((cmordir) for cmordir in cmordirs))
    p.close()
    p.join()
    return results_timeser

def check_compliance_results(results_comp):
    compliant=[]
    noncompliant=[]
    errors=[]
    for r in results_comp: 
        if r[1] == 0: compliant.append(r[0])
        elif r[1] == 1: noncompliant.append([r[0],r[2]])
        elif r[1] == 2: errors.append([r[0],r[2]])
        else: pass
    print '\nCompliant files:'
    for comp in compliant:
        print os.path.basename(comp)
        shutil.copy2(comp,comp.replace(outpath_root,pub_dir))
        os.remove(comp)
    print '\nNon-compliant files:'
    for noncomp in noncompliant:
        print os.path.basename(noncomp[0]),noncomp[1]
    print '\nError files:'
    for err in errors:
        print os.path.basename(err[0]),err[1]

def check_timeseries_results(results_timeser):
    succ_timeser=[]
    fail_timeser=[]
    for r in results_timeser: 
        if r[1] == 0: succ_timeser.append(r[0])
        elif r[1] == 1: fail_timeser.append([r[0],r[2]])
        else: pass
    print '\nGenerated timeseries for:'
    for succ in succ_timeser:
        print succ
    print '\nFailed to create timeseries for:'
    for fail in fail_timeser:
        print fail


def main():
    print "\nstarting quality_check..."
    print 'local experiment being checked: {}'.format(exptoprocess)
    print 'ncpus = {}'.format(ncpus)
    if args.compliance:
        print '\n#### CHECKING CMOR FILES FOR COMPLIANCE'
        results_comp=pool_handler_comp(successes)
        print '\nCompliance check complete. Results:'
        check_compliance_results(results_comp)
    #sys.exit('exiting, line 383')
    if args.timeseries:
        print '\n#### PRODUCING DATA SUMMARY NETCDF4 FILES AND PLOTS'
        pathlib2.Path.mkdir(pathlib2.Path(summaryncdir),parents=True,exist_ok=True)
        cmordirs=[]
        for table,var,tstart,tend,cmorfile in successes:
            if publish: cmorfile=cmorfile.replace(outpath_root,pub_dir)
            if os.path.exists(cmorfile): cmordir=os.path.dirname(cmorfile)
            else: continue
            if not cmordir in cmordirs: cmordirs.append(cmordir)
        results_timeser=pool_handler_timeser(cmordirs)
        print '\nTimeseries generation complete. Results:'
        check_timeseries_results(results_timeser)
        
if __name__ == "__main__":
    main()
