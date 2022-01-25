from app import app
#from app_functions import plotVar
import sqlite3
import traceback
import csv
import os,sys
import time
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from datetime import datetime
import ast
import multiprocessing as mp

exp=os.environ.get('EXP_TO_PROCESS')
table=os.environ.get('TABLE_TO_PROCESS')
var=os.environ.get('VARIABLE_TO_PROCESS')
successlists=os.environ.get('SUCCESS_LISTS')
out_dir=os.environ.get('OUT_DIR')
varlogs=os.environ.get('VAR_LOGS')
try: ncpus=int(os.environ.get('NCPUS'))
except: ncpus=1
#open database    
database=os.environ.get('DATABASE')
print database
if not database:
    #default database
    database='{}/database.db'.format(out_dir)
conn=sqlite3.connect(database,timeout=200.0)
conn.text_factory=str
cursor=conn.cursor()
database_updater='{}/database_updater.py'.format(out_dir)
if os.environ.get('MODE').lower() == 'custom': mode='custom'
elif os.environ.get('MODE').lower() == 'ccmi': mode='ccmi'
else: mode='cmip6'

#options
#
if os.environ.get('OVERRIDEFILES').lower() in ['true','yes']: overRideFiles=True
else: overRideFiles=False
#if os.environ.get('PLOT').lower() == 'true': plot=True
#else: plot=False
if os.environ.get('DREQ_YEARS').lower() == 'true': dreq_years=True
else: dreq_years=False
print 'dreq years = ',dreq_years

#
#function to process set of rows in the database
#if overRideFiles is true, write over files that already exist
#otherwise they will be skipped
#
def process_row(row):
    #set version number
    #date=datetime.today().strftime('%Y%m%d')
    #set location of cmor tables
    cmip_table_path=os.environ.get('CMIP_TABLES')
    #
    #First map entries from database row to variables
    #
    experiment_id=row[0]
    realization_idx=row[1]
    initialization_idx=row[2]
    physics_idx=row[3]
    forcing_idx=row[4]
    infile=row[5]
    outpath=row[6]
    file_name=row[7]
    vin=row[8].split()
    vcmip=row[9]
    table=row[10]
    cmip_table='CMIP6_{}'.format(row[10])
    frequency=row[11]
    tstart=row[12]
    tend=row[13]
    status=row[14]
    file_size=row[15]
    local_exp_id=row[16]
    calculation=row[17]
    axes_modifier=row[18]
    in_units=row[19]
    positive=row[20]
    timeshot=row[21]
    try: years=ast.literal_eval(row[22])
    except: years=row[22]
    var_notes=row[23]
    cfname=row[24]
    activity_id=row[25]
    institution_id=row[26]
    source_id=row[27]
    grid_label=row[28]
    access_version=row[29]
    json_file_path=row[30]
    reference_date=row[31]
    version=row[32]
    rowid=row[33]
    notes='Exp: {e1}-{e2}; Local ID: {le}; Variable: {v1} ({v2})'.format(e1=access_version,e2=experiment_id,le=local_exp_id,v1=vcmip,v2=vin)
    if dreq_years:
        try:
            int(years[0])
            if tstart >= years[0]:
                pass
            elif (tstart < years[0]) and (tend >= years[0]):
                tstart=years[0]
            else:
                return 'years requested for variable are outside specified period: {},{},{},{}'.format(table,vcmip,tstart,tend)
            if tend <= years[-1]:
                pass
            elif (tend > years[-1]) and (tstart <= years[-1]):
                tend=years[-1]
            else:
                return 'years requested for variable are outside specified period: {},{},{},{}'.format(table,vcmip,tstart,tend)
        except:
            pass
    else: pass
    #
    print '\n#---------------#---------------#---------------#---------------#\nprocessing row with details:\n'
    print '{},{}'.format(cmip_table,vcmip)
    print 'vcmip = {}'.format(vcmip)
    print 'vin = {}'.format(vin)
    print 'cfname = {}'.format(cfname)
    print 'cmip_table = {}'.format(cmip_table)
    print 'calculation = {}'.format(calculation)
    print 'in_units = {}'.format(in_units)
    print 'axes_modifier = {}'.format(axes_modifier)
    print 'positive = {}'.format(positive)
    print 'timeshot = {}'.format(timeshot)
    print 'frequency = {}'.format(frequency)
    try:
        int(years[0])
        print 'years = {}-{}'.format(years[0],years[-1])
    except: print 'years = {}'.format(years)
    print 'var_notes = {}'.format(var_notes)
    print 'local_exp_id = {}'.format(local_exp_id)
    print 'reference_date = {}'.format(reference_date)
    print 'tstart = {}'.format(tstart)
    print 'tend = {}'.format(tend)
    print 'access_version = {}'.format(access_version)
    print 'infile = {}'.format(infile)
    print 'outpath = {}'.format(outpath)
    print 'activity_id = {}'.format(activity_id)
    print 'institution_id = {}'.format(institution_id)
    print 'source_id = {}'.format(source_id)
    print 'experiment_id = {}'.format(experiment_id)
    print 'grid_label = {}'.format(grid_label)
    print 'version = {}'.format(version)
    print 'realization_idx = {}'.format(realization_idx)
    print 'initialization_idx = {}'.format(initialization_idx)
    print 'physics_idx = {}'.format(physics_idx)
    print 'forcing_idx = {}'.format(forcing_idx)
    print 'json_file_path = {}'.format(json_file_path)
    print 'expected file name = {}'.format(file_name)
    print 'status = {}'.format(status)
    #
    try:
        #Do the processing:
        #
        expected_file=file_name
        #if not os.path.exists(outpath):
        #    print 'creating outpath directory: {}'.format(outpath)
        #    os.makedirs(outpath)
        if overRideFiles or not os.path.exists(expected_file):
            #if file doesn't already exist (and we're not overriding), run the app
            #
            #version_number='v{date}'.format(date=version)
            dictionary={'vcmip':vcmip,'vin':vin,'cmip_table':cmip_table,'infile':infile,'tstart':tstart,'tend':tend,\
            'notes':notes,'cmip_table_path':cmip_table_path,'frequency':frequency,\
            'calculation':calculation,'axes_modifier':axes_modifier,'in_units':in_units,'positive':positive,\
            'json_file_path':json_file_path,'timeshot':timeshot,'access_version':access_version,\
            'reference_date':reference_date,'mode':mode}
            #process the file,
            ret=app(dictionary)
            try: os.chmod(ret,0644)
            except: pass
            print '\nreturning to app_wrapper...'
            #
            #check different return codes from the APP. 
            #
            if ret == 0:
                msg='\ndata incomplete for variable: {}\n'.format(vcmip)    
                with open(database_updater,'a+') as dbu:
                    dbu.write("setStatus('data_Unavailable',{})\n".format(rowid))
                dbu.close()
            elif ret == -1:
                msg='\nreturn status from the APP shows an error\n'
                with open(database_updater,'a+') as dbu:
                    dbu.write("setStatus('unknown_return_code',{})\n".format(rowid))
                dbu.close()
            else:
                insuccesslist=0
                with open('{}/{}_success.csv'.format(successlists,exp),'a+') as c:
                    reader=csv.reader(c, delimiter=',')
                    for row in reader:
                        if row[0] == table and row[1] == vcmip and row[2] == tstart and row[3] == tend: insuccesslist=1
                        else: pass
                    if insuccesslist == 0:
                        c.write('{},{},{},{},{}\n'.format(table,vcmip,tstart,tend,ret))
                        print 'added \'{},{},{},{},...\' to {}/{}_success.csv'.format(table,vcmip,tstart,tend,successlists,exp)
                    else: pass
                c.close()
                #Assume processing has been successful
                #Check if output file matches what we expect
                #
                print 'output file:   {}'.format(ret)
                if ret == expected_file:
                    print 'expected and cmor file paths match'
                    msg='\nsuccessfully processed variable: {},{},{},{}\n'.format(table,vcmip,tstart,tend)
                    #modify file permissions to globally readable
                    #os.chmod(ret,493)
                    with open(database_updater,'a+') as dbu:
                        dbu.write("setStatus('processed',{})\n".format(rowid))
                    dbu.close()
                    #plot variable
                    #try:
                    #    if plot:
                    #        plotVar(outpath,ret,cmip_table,vcmip,source_id,experiment_id)
                    #except: 
                    #    msg='{},plot_fail: '.format(msg)
                    #    traceback.print_exc()
                else :
                    print 'expected file: {}'.format(expected_file)
                    print 'expected and cmor file paths do not match'
                    msg='\nproduced but file name does not match expected: {},{},{},{}\n'.format(table,vcmip,tstart,tend)
                    with open(database_updater,'a+') as dbu:
                        dbu.write("setStatus('file_mismatch',{})\n".format(rowid))
                    dbu.close()
        else :
            #
            #we are not processing because the file already exists.     
            #
            msg='\nskipping because file already exists for variable: {},{},{},{}\n'.format(table,vcmip,tstart,tend)
            print 'file: {}'.format(expected_file)
            with open(database_updater,'a+') as dbu:
                dbu.write("setStatus('processed',{})\n".format(rowid))
            dbu.close()
    except     Exception, e: #something has gone wrong in the processing
        print e
        traceback.print_exc()
        infailedlist=0
        with open('{}/{}_failed.csv'.format(successlists,exp),'a+') as c:
            reader=csv.reader(c, delimiter=',')
            for row in reader:
                if row[0] == vcmip and row[1] == table and row[2] == tstart and row[3] == tend:infailedlist=1
                else: pass
            if infailedlist == 0:
                c.write('{},{},{},{}\n'.format(table,vcmip,tstart,tend))
                print 'added \'{},{},{},{}\' to {}/{}_failed.csv'.format(table,vcmip,tstart,tend,successlists,exp)
            else: pass
        c.close()
        msg='\ncould not process file for variable: {},{},{},{}\n'.format(table,vcmip,tstart,tend)
        with open(database_updater,'a+') as dbu:
            dbu.write("setStatus('processing_failed',{})\n".format(rowid))
        dbu.close()
    print msg
    return msg

def process_experiment(row):
    varlogfile=varlogs+'/varlog_{}_{}_{}-{}.txt'.format(row[10],row[9],row[12],row[13])
    sys.stdout = open(varlogfile, 'w')
    sys.stderr = open(varlogfile, 'w')
    print 'process: ',mp.Process()
    t1=time.time()
    print 'start time: {}'.format(time.time()-t1)
    print 'processing row:'
    print row
    msg=process_row(row)
    print 'end time: {}'.format(time.time()-t1)
    return msg

def pool_handler(rows):
    p=mp.Pool(ncpus)
    results=p.imap_unordered(process_experiment,((row) for row in rows))
    p.close()
    p.join()
    return results

#
#Main method to select and process variables
#
def main():
    print '\nstarting app_wrapper...'
    print 'local experiment being processed: {}'.format(exp)
    print 'cmip6 table being processed: {}'.format(table)
    print 'cmip6 variable being processed: {}'.format(var)
    #process only one file per mp process
    cursor.execute('select *,ROWID  from file_master where status==\'unprocessed\'\
                    and local_exp_id==?',[exp])
    #fetch rows
    try: rows=cursor.fetchall()
    except: print 'no more rows to process'
    conn.commit()
    #process rows
    print 'number of rows: ',len(rows)
    results=pool_handler(rows)
    print 'app_wrapper finished!\n'
    #summarise what was processed:
    print "RESULTS:"
    for r in results: print r

if __name__ == "__main__":
    main()
