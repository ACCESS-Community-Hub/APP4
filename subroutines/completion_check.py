# porting to python3: paola.petrelli@utas.edu.au

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

parser = argparse.ArgumentParser(description='Checks existence, compliance, and data quality of CMOR output')
parser.add_argument('--multi', dest='multi', default=False, action='store_true',
                    help='For use when running APP in parallel')
args = parser.parse_args()

successlists=os.environ.get('SUCCESS_LISTS')
variablemapsdir=os.environ.get('VARIABLE_MAPS')
exptoprocess=os.environ.get('EXP_TO_PROCESS')
joboutputfile=os.environ.get('JOB_OUTPUT')
app_dir=os.environ.get('APP_DIR')
out_dir=os.environ.get('OUT_DIR')
experimentstable=os.environ.get('EXPERIMENTS_TABLE')
mode=os.environ.get('MODE')

#qc_dir=os.environ.get('QC_DIR')
#pub_dir=os.environ.get('PUB_DIR')
#cmiptables=os.environ.get('CMIP_TABLES')
#multilist=os.environ.get('MULTI_LIST')

print(successlists)

# Read in success and fail lists
try:
    successes=[]
    try:
        fsuccess = f"{successlists}/{exptoprocess}_success.csv"
        ffail = f"{successlists}/{exptoprocess}_failed.csv"
        if os.path.exists(fsuccess):
            with open(fsuccess,'r') as s:
                reader=csv.reader(s, delimiter=',')
                for row in reader:
                    try:
                        row[0]
                    except:
                        row='#'
                    if row[0].startswith('#'):
                        pass
                    else:
                        successes.append(row)
            successes.sort()
            s.close()
        else:
            print(f"W: no success list - will assume all variables failed")
    except:
        raise Exception(f"E: failed to read file {fsuccesslist}")
    fails=[]
    try:
        if os.path.exists(ffail)
            with open(ffail)
                reader=csv.reader(f, delimiter=',')
                for row in reader:
                    try:
                        row[0]
                    except:
                        row='#'
                    if row[0].startswith('#'):
                        pass
                    else:
                        fails.append(row)
            fails.sort()
            f.close()
        else:
            print("W: no failed list - will assume all variables succeeded")
    except:
        raise Exception(f"E: failed to read file {ffail}")
    variablesmapped=[]
    try:
        if os.path.exists(variablemapsdir):
            for file in glob.glob(f"{variablemapsdir}/*.csv"):
                table=os.path.basename(file).split('.')[0]
                with open(file,'r') as v:
                    reader=csv.reader(v, delimiter=',')
                    for row in reader:
                        try:
                            row[0]
                        except:
                            row='#'
                        if row[0].startswith('#'):
                            pass
                        else: 
                            variablesmapped.append([table,row[0]])
                v.close()
        else:
            raise Exception(f"E: no variable map directory found at {variablemapsdir}")
    except:
        raise Exception(f"E: failed to read variable maps in {variablemapsdir}")
    if mode == 'production':
        try:
            if os.path.exists(experimentstable):
                with open(experimentstable,'r') as f:
                    reader=csv.reader(f, delimiter=',')
                    for row in reader:
                        try:
                            row[0]
                        except:
                            row='#'
                        if row[0].startswith('#'):
                            pass
                        elif row[0] == exptoprocess: 
                            jsonfile = row[2]
                            #dreq = row[3]
                f.close()
                with open(jsonfile) as j:
                    json_dict=json.load(j)
                j.close()
                outpath_root=json_dict['outpath']
                #dreq
            else:
                raise Exception(f"E: no experiments table found at {experimentstable}")
        except:
            if os.path.exists(experimentstable):
                raise Exception("E: check experiment and experiments table")
            else: 
                raise Exception(f"E: no experiments table found at {experimentstable}")
    elif mode == 'custom':
        outpath_root=os.environ.get('OUTPUT_LOC')
except Exception as e:
    sys.exit(f"E: failed to read in required files: {e}")

print("\n#### CHECKING VARIABLE MAPS AGAINST SUCCESS/FAIL LISTS")
try:
    vnotfounds=[]
    vfails=[]
    vfailsuc=[]
    sfounds=0
    ffounds=0
    for table,var in variablesmapped:
        sfound=0
        ffound=0
        for stable,svar,tstart,tend,cmorfile in successes:
            if table == stable and var == svar: 
                sfound=1
            else: pass
        for ftable,fvar,tstart,tend in fails:
            if table == ftable and var == fvar: 
                ffound=1
            else: pass
        if sfound == 0 and ffound == 0: vnotfounds.append([table,var])
        elif sfound == 0 and ffound == 1: vfails.append([table,var])
        elif sfound == 1 and ffound == 0: 
            sfounds+=1
        elif sfound == 1 and ffound == 1: 
            vfailsuc.append([table,var])
            #print("W: variable {var} failed for some years, check success/fail lists and job_output for details")
    print("  I: ({len(variablesmapped)}) variables were mapped...")
    if vnotfounds != []:
        print("  W: some ({len(vnotfounds)}) variables that were mapped are not in the success/fail lists!:")
        print(vnotfounds)
    print("  S: ({sfounds}) variables were successfully post-processed for all years")
    print("  I: ({len(vfailsuc)}) variables failed for only some years and successed for others:")
    print(vfailsuc)
    print("F: ({len(vfails)}) variables failed post-processing (check job_output file for details):")
    print(vfails)
except Exception as e:
    print("E: failed to compare variable maps to success/fail lists: {e}")
