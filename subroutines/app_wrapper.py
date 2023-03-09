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
try:
    ncpus=int(os.environ.get('NCPUS'))
except:
    ncpus=1
#open database    
database=os.environ.get('DATABASE')
print(database)
if not database:
    #default database
    database=f"{out_dir}/database.db"
conn=sqlite3.connect(database,timeout=200.0)
conn.text_factory=str
cursor=conn.cursor()
database_updater=f"{out_dir}/database_updater.py"
if os.environ.get('MODE').lower() == 'custom':
    mode='custom'
elif os.environ.get('MODE').lower() == 'ccmi':
    mode='ccmi'
else: 
    mode='cmip6'

#options
#
if os.environ.get('OVERRIDEFILES').lower() in ['true','yes']:
    overRideFiles=True
else:
    overRideFiles=False
#if os.environ.get('PLOT').lower() == 'true': plot=True
#else: plot=False
if os.environ.get('DREQ_YEARS').lower() == 'true':
    dreq_years=True
else:
    dreq_years=False
print(f"dreq years = {dreq_years}")

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
    cmip_table=f"CMIP6_{row[10]}"
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
    try: 
        years=ast.literal_eval(row[22])
    except: 
        years=row[22]
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
    notes=f"Local exp ID: {local_exp_id}; Variable: {vcmip} ({vin})"
    try:
        exp_description=os.environ.get('EXP_DESCRIPTION')
    except: 
        exp_description=f"Exp: {experiment_id}"
    if dreq_years:
        try:
            msg_return = "years requested for variable are outside " +
                         "specified period: {table}, {vcmip}, {tstart}, {tend}"
            int(years[0])
            if tstart >= years[0]:
                pass
            elif (tstart < years[0]) and (tend >= years[0]):
                tstart=years[0]
            else:
                return msg_return
            if tend <= years[-1]:
                pass
            elif (tend > years[-1]) and (tstart <= years[-1]):
                tend=years[-1]
            else:
                return msg_return
        except:
            pass
    else:
        pass
    #
    print("\n#---------------#---------------#---------------#---------------#\nprocessing row with details:\n")
    print(f"{cmip_table},{vcmip}"
    print(f"vcmip = {vcmip}")
    print(f"vin = {vin}")
    print(f"cfname = {cfname}")
    print(f"cmip_table = {cmip_table}")
    print(f"calculation = {calculation}")
    print(f"in_units = {in_units}")
    print(f"axes_modifier = {axes_modifier}")
    print(f"positive = {positive}")
    print(f"timeshot = {timeshot}")
    print(f"frequency = {frequency}")
    try:
        int(years[0])
        print(f"years = {years[0]}-{years[-1]}")
    except:
        print(f"years = {years}")
    print(f"var_notes = {var_notes}")
    print(f"local_exp_id = {local_exp_id}")
    print(f"reference_date = {reference_date}")
    print(f"tstart = {tstart}")
    print(f"tend = {tend}")
    print(f"access_version = {access_version}")
    print(f"infile = {infile}")
    print(f"outpath = {outpath}")
    print(f"activity_id = {activity_id}")
    print(f"institution_id = {institution_id}")
    print(f"source_id = {source_id}")
    print(f"experiment_id = {experiment_id}")
    print(f"grid_label = {grid_label}")
    print(f"version = {version}")
    print(f"realization_idx = {realization_idx}")
    print(f"initialization_idx = {initialization_idx}")
    print(f"physics_idx = {physics_idx}")
    print(f"forcing_idx = {forcing_idx}")
    print(f"json_file_path = {json_file_path}")
    print(f"exp_description = {exp_description}")
    print(f"expected file name = {file_name}")
    print(f"status = {status}")
    #
    try:
        #Do the processing:
        #
        expected_file=file_name
        #if not os.path.exists(outpath):
        #    print(f"creating outpath directory: {outpath}")
        #    os.makedirs(outpath)
        if overRideFiles or not os.path.exists(expected_file):
            #if file doesn't already exist (and we're not overriding), run the app
            #
            #version_number = f"v{version}"
            dictionary={'vcmip': vcmip,
                        'vin': vin,
                        'cmip_table': cmip_table,
                        'infile': infile,
                        'tstart': tstart,
                        'tend': tend,
                        'notes': notes,
                        'cmip_table_path': cmip_table_path,
                        'frequency': frequency,
                        'calculation': calculation,
                        'axes_modifier': axes_modifier,
                        'in_units': in_units,
                        'positive': positive,
                        'json_file_path': json_file_path,
                        'timeshot': timeshot,
                        'access_version': access_version,
                        'reference_date': reference_date,
                        'mode': mode,
                        'exp_description': exp_description}
            #process the file,
            ret = app(dictionary)
            try:
                os.chmod(ret,0644)
            except:
                pass
            print("\nreturning to app_wrapper...")
            #
            #check different return codes from the APP. 
            #
            if ret == 0:
                msg = f"\ndata incomplete for variable: {vcmip}\n"
                with open(database_updater,'a+') as dbu:
                    dbu.write(f"setStatus('data_Unavailable',{rowid})\n")
                dbu.close()
            elif ret == -1:
                msg="\nreturn status from the APP shows an error\n"
                with open(database_updater,'a+') as dbu:
                    dbu.write(f"setStatus('unknown_return_code',{rowid})\n")
                dbu.close()
            else:
                insuccesslist=0
                with open(f"{successlists}/{exp}_success.csv",'a+') as c:
                    reader=csv.reader(c, delimiter=',')
                    for row in reader:
                        if (row[0] == table and row[1] == vcmip and
                            row[2] == tstart and row[3] == tend): insuccesslist=1
                        else:
                            pass
                    if insuccesslist == 0:
                        c.write(f"{table},{vcmip},{tstart},{tend},{ret}\n")
                        print(f"added \'{table},{vcmip},{tstart},{tend},...\'" +
                              f"to {successlists}/{exp}_success.csv")
                    else:
                        pass
                c.close()
                #Assume processing has been successful
                #Check if output file matches what we expect
                #
                print(f"output file:   {ret}")
                if ret == expected_file:
                    print(f"expected and cmor file paths match")
                    msg=f"\nsuccessfully processed variable: {table},{vcmip},{tstart},{tend}\n"
                    #modify file permissions to globally readable
                    #os.chmod(ret,493)
                    with open(database_updater,'a+') as dbu:
                        dbu.write(f"setStatus('processed',{rowid})\n")
                    dbu.close()
                    #plot variable
                    #try:
                    #    if plot:
                    #        plotVar(outpath,ret,cmip_table,vcmip,source_id,experiment_id)
                    #except: 
                    #    msg = f"{msg},plot_fail: "
                    #    traceback.print_exc()
                else :
                    print("expected file: {expected_file}")
                    print("expected and cmor file paths do not match")
                    msg=f"\nproduced but file name does not match expected: {table},{vcmip},{tstart},{tend}\n"
                    with open(database_updater,'a+') as dbu:
                        dbu.write(f"setStatus('file_mismatch',{rowid})\n")
                    dbu.close()
        else :
            #
            #we are not processing because the file already exists.     
            #
            msg=f"\nskipping because file already exists for variable: {table},{vcmip},{tstart},{tend}\n"
            print(f"file: {expected_file}")
            with open(database_updater,'a+') as dbu:
                dbu.write(f"setStatus('processed',{rowid})\n")
            dbu.close()
    except     Exception, e: #something has gone wrong in the processing
        print(e)
        traceback.print_exc()
        infailedlist=0
        with open(f"{successlists}/{exp}_failed.csv",'a+') as c:
            reader=csv.reader(c, delimiter=',')
            for row in reader:
                if row[0] == vcmip and row[1] == table and row[2] == tstart and row[3] == tend:infailedlist=1
                else: pass
            if infailedlist == 0:
                c.write(f"{table},{vcmip},{tstart},{tend}\n")
                print(f"added '{table},{vcmip},{tstart},{tend}' to {successlists}/{exp}_failed.csv"
            else: pass
        c.close()
        msg=f"\ncould not process file for variable: {table},{vcmip},{tstart},{tend}\n"
        with open(database_updater,'a+') as dbu:
            dbu.write("setStatus('processing_failed',{rowid})\n")
        dbu.close()
    print(msg)
    return msg


def process_experiment(row):
    varlogfile=f"{varlogs}/varlog_{row[10]}_{row[9]}_{row[12]}-{row[13]}.txt"
    sys.stdout = open(varlogfile, 'w')
    sys.stderr = open(varlogfile, 'w')
    print(f"process: {mp.Process()}")
    t1=time.time()
    print(f"start time: {time.time()-t1}")
    print(f"processing row:")
    print(row)
    msg=process_row(row)
    print(f"end time: {time.time()-t1}")
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
    print("\nstarting app_wrapper..."
    print(f"local experiment being processed: {exp}")
    print(f"cmip6 table being processed: {table}")
    print(f"cmip6 variable being processed: {var}")
    #process only one file per mp process
    cursor.execute('select *,ROWID  from file_master where status==\'unprocessed\'\
                    and local_exp_id==?',[exp])
    #fetch rows
    try:
       rows=cursor.fetchall()
    except:
       print("no more rows to process")
    conn.commit()
    #process rows
    print(f"number of rows: {len(rows)}")
    results=pool_handler(rows)
    print("app_wrapper finished!\n")
    #summarise what was processed:
    print("RESULTS:")
    for r in results: print(r)

if __name__ == "__main__":
    main()
