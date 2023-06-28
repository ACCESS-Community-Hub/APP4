#
import numpy as np
import csv
import glob
import os
import re
import sys
import ast
import argparse
np.set_printoptions(threshold=sys.maxsize)

table='all' #'all' or CMIP6_table

# Check for vars in 'xcheck1' that are not in 'xcheck2' (/xcheck3/xcheck4)

xcheck1='PMIP/cmvme_c4.cd.cm.da.om.rf.sc_lig127k_3_3.csv'
xcheck2='PMIP/cmvme_c4.cd.cm.pm.rf.sc_lig127k_1_2.csv'
#xcheck3='RFMIP/cmvme_cm.fa.om.rf.sc.si_piClim-aer_1_2.csv'
#xcheck4='cm2/cmvme_cm.fa.om.rf.sc.si_ssp245_1_2.csv'

xcheck1_list=[]
xcheck1_tabs=[]
xcheck1_prio=[]
xcheck1_long=[]
xcheck1_rows=[]
with open(xcheck1,'r') as h:
    reader=csv.reader(h, delimiter='\t')
    for row in reader:
        try:
            row[12]
            if table == 'all':
                if row[6] != '':
                    xcheck1_list.append(row[6])
                    xcheck1_tabs.append(row[0])
                    xcheck1_prio.append(row[1])
                    xcheck1_long.append(row[7])
                    xcheck1_rows.append(row)
            else:
                if (row[0] == table) and (row[6] != ''):
                    xcheck1_list.append(row[6])
                    xcheck1_tabs.append(row[0])
                    xcheck1_prio.append(row[1])
                    xcheck1_long.append(row[7])
                    xcheck1_rows.append(row)
        except: pass

xcheck2_list=[]
xcheck2_tabs=[]
xcheck2_prio=[]
xcheck2_long=[]
xcheck2_rows=[]
with open(xcheck2,'r') as s:
    reader=csv.reader(s, delimiter='\t')
    for row in reader:
        try:
            row[12]
            if table == 'all':
                if row[6] != '':
                    xcheck2_list.append(row[6])
                    xcheck2_tabs.append(row[0])
                    xcheck2_prio.append(row[1])
                    xcheck2_long.append(row[7])
                    xcheck2_rows.append(row)
            else:
                if (row[0] == table) and (row[6] != ''):
                    xcheck2_list.append(row[6])
                    xcheck2_tabs.append(row[0])
                    xcheck2_prio.append(row[1])
                    xcheck2_long.append(row[7])
                    xcheck2_rows.append(row)
        except: pass
        
try:
    xcheck3
    xcheck3_list=[]
    xcheck3_tabs=[]
    xcheck3_prio=[]
    xcheck3_long=[]
    xcheck3_rows=[]
    with open(xcheck3,'r') as s:
        reader=csv.reader(s, delimiter='\t')
        for row in reader:
            try:
                row[12]
                if table == 'all':
                    if row[6] != '':
                        xcheck3_list.append(row[6])
                        xcheck3_tabs.append(row[0])
                        xcheck3_prio.append(row[1])
                        xcheck3_long.append(row[7])
                        xcheck3_rows.append(row)
                else:
                    if (row[0] == table) and (row[6] != ''):
                        xcheck3_list.append(row[6])
                        xcheck3_tabs.append(row[0])
                        xcheck3_prio.append(row[1])
                        xcheck3_long.append(row[7])
                        xcheck3_rows.append(row)
            except: pass
except: pass

try:
    xcheck4
    xcheck4_list=[]
    xcheck4_tabs=[]
    xcheck4_prio=[]
    xcheck4_long=[]
    xcheck4_rows=[]
    with open(xcheck4,'r') as s:
        reader=csv.reader(s, delimiter='\t')
        for row in reader:
            try:
                row[12]
                if table == 'all':
                    if row[6] != '':
                        xcheck4_list.append(row[6])
                        xcheck4_tabs.append(row[0])
                        xcheck4_prio.append(row[1])
                        xcheck4_long.append(row[7])
                        xcheck4_rows.append(row)
                else:
                    if (row[0] == table) and (row[6] != ''):
                        xcheck4_list.append(row[6])
                        xcheck4_tabs.append(row[0])
                        xcheck4_prio.append(row[1])
                        xcheck4_long.append(row[7])
                        xcheck4_rows.append(row)
            except: pass
except: pass

try:
    xcheck4
    for idx,var in enumerate(xcheck1_list):
        if (var not in xcheck2_list) and (var not in xcheck3_list) and (var not in xcheck4_list):
            print var, xcheck1_tabs[idx], xcheck1_prio[idx], xcheck1_long[idx]
            #print '\t'.join(hist_rows[hist_list.index(var)])
except:
    try:
        xcheck3
        for idx,var in enumerate(xcheck1_list):
            if (var not in xcheck2_list) and (var in xcheck3_list):
                print var, xcheck1_tabs[idx], xcheck1_prio[idx], xcheck1_long[idx]
                #print '\t'.join(hist_rows[hist_list.index(var)])
    except:
        for idx,var in enumerate(xcheck1_list):
            if var not in xcheck2_list:
                print var, xcheck1_tabs[idx], xcheck1_prio[idx], xcheck1_long[idx]
               #print '\t'.join(hist_rows[hist_list.index(var)])

