# Create an index/gallery page of the QC figs.
# File names like
# zos_Omon_ACCESS-CM2_historical_r1i1p1f1_gn_185001.png

# porting to python3: paola.petrelli@utas.edu.au

# var_table_

import glob
import collections
import os
import sys

try:
    corepath = os.environ.get('ONLINE_PLOT_DIR')
except:
    sys.exit('no corepath defined')
exp = os.environ.get('EXP_TO_PROCESS')

path = corepath+'/'+exp

if not os.path.exists(path):
    sys.exit()

for tablepath in glob.glob(f"{path}/*"):
    #if os.isdir(tablepath):
    figs = collections.defaultdict(list)
    for f in glob.glob(f"{tablepath}/*.png"):
        f = os.path.basename(f)
        s = f.split('_')
        var, table, model, expt  = s[:4]
        figs[var].append(f)
    # Now create a html file
    g = open(f"{tablepath}/index.html", 'w')
    g.write("""
<!DOCTYPE html>
<html>
<head>
<style>
BODY {background-color: #f5efdf; font-family: Verdana, Arial, Helvetica, sans-se
rif; margin: 0px}

#titlebar {
    color: #555;
    background-color: #f5e1ab; 
        top: 3px; 
        padding:6px;
        position: relative;
        text-align: center;
        font-weight: bold;
        font-size: 120%;
}
#image-grid {
    padding-left: 20px;
        padding-right: 20px;
        padding-top: 30px;
        padding-bottom:15px
}
</style>
<title>ACCESS-CM2</title>
</head>

<body class="Body">
""")
    g.write('<div id="titlebar"><b>%s %s simulation QC: Table %s </b></div>' % (model, expt, table))
    g.write("""
<div id="image-grid">
<table width="100%">
""")

    # Create links for each variable index
    for var in sorted(figs):
        link = f"{var}_index.html"
        g.write(f'<p><h4><a href="{link}">Variable {var}</a></h4>\n')
    g.write("""
</var>
</div>
</body>
</html>
""")
    g.close()

    for var in sorted(figs):
        # Now create a html file
        g = open(f"{tablepath}/{var}_index.html", 'w')
        g.write("""
<!DOCTYPE html>
<html>
<head>
<style>
BODY {background-color: #f5efdf; font-family: Verdana, Arial, Helvetica, sans-se
rif; margin: 0px}

#titlebar {
    color: #555;
    background-color: #f5e1ab; 
        top: 3px; 
        padding:6px;
        position: relative;
        text-align: center;
        font-weight: bold;
        font-size: 120%;
}
#image-grid {
    padding-left: 20px;
        padding-right: 20px;
        padding-top: 30px;
        padding-bottom:15px
}
</style>
<title>ACCESS-CM2</title>
</head>

<body class="Body">
""")
        g.write(f'<div id="titlebar"><b>{model} {expt} simulation QC: Table {table}, Variable {var}</b></div>')
        g.write("""
<div id="image-grid">
<table width="100%">
""")
        for k, f in enumerate(sorted(figs[var])):
            if k%2 == 0:
                g.write("<tr>\n")
            # PP why the format? f isn't referenced anywhere 
            g.write('<td width="50%"><a href="{0}"><img src="{0}" style="display:block" width="100%"></a></td>\n'.format(f))
            if k%2 == 1:
                g.write("</tr>\n")
    
                
        g.write("""
</var>
</div>
</body>
</html>
""")
        g.close()
