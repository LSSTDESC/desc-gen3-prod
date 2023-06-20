#!/usr/bin/env python3

# Start the clock for timing out.
import time
time0 = time.time()

import os
import sys
import dg3prod
import pandas

statfilename = 'current-status.txt'
doInit = False
doProc = False
doFina = False
doQgReport = False
blockJob=True      # Set false for interactive running.
showStatus = False
showParslTables = False
doWorkflow = True
doTest = False
tmax = 1000

thisdir = os.getcwd()
haveQG = False

pg_pickle_path = None     # Full path to the pg pickle file
pg = None                 # ParlslGraph used for processing
pgro = None               # ParslGraph for checking status

def get_pg():
    global pg
    if pg is None:
        pg = ParslGraph.restore(pg_pickle_path)
    return pg

def get_pgro():
    global pgro
    if pgro is None:
        pgro = ParslGraph.restore(pg_pickle_path, use_dfk=False)
    return pgro

def get_haveQG(pg):
    try:
        if pg.qgraph is None:
            return False
    except:
        statlogmsg("Check of quantum graph raised an exception.")
    return True

def logmsglist(msgs, update_status=False):
    out = open('runapp-g3wfpipe.log', 'a')
    dmsg = time.strftime('%Y-%m-%d %H:%M:%S:')
    rmsg = msgs[0] if len(msgs) else ''
    for msg in msgs[1:]:
        rmsg += ' ' + str(msg)
    dmsg += ' ' + rmsg
    out.write(dmsg + '\n')
    out.close()
    print(dmsg, flush=True)
    if update_status:
        fstat = open(statfilename, 'w')
        fstat.write(rmsg + '\n')

def logmsg(*msgs, update_status=False):
    logmsglist(list(msgs), update_status)

def statlogmsg(*msgs):
    logmsglist(list(msgs), update_status=True)

logmsg(f"Executing {__file__}")
statlogmsg(f"Running g3wfpipe version: {dg3prod.version()}")
for opt in sys.argv[1:]:
    logmsg("Processing argument", opt)
    if opt in ["-h", "help"]:
        print('Usage:', sys.argv[0], '[OPTS]')
        print('  Supported valuse for OPTS:')
        print('    init - Create QG (QuandunGraph).')
        print('    proc - Process tasks in the existing QG.')
        print('    status - Report on the status of processing for the existing QG.')
        print('    qgre - Prepare report describing the existing QG.')
        print('    tables - Prepare report describing the parsl tables.')
        print('    finalize - Register output datasets for existing QG.')
        print('    tmax - Timeout in sec or tttU with U in {s, m, h, d}.')
        sys.exit()
    elif opt == 'init':
        doInit = True
    elif opt == 'proc':
        doProc = True
    elif opt == 'finalize':
        doFina = True
    elif opt == 'qgre':
        doQgReport = True
    elif opt == 'status':
        showStatus = True
    elif opt == 'tables':
        showParslTables = True
    elif opt[0:5] == 'tmax:':
        stmax = opt[5:]
        unit = stmax[-1]
        utims = {'s':1, 'm':60, 'h':3600, 'd':86400}
        if unit in utims:
            tmax = utims[unit]*float(stmax[0:-1])
        else:
            tmax = float(stmax)
        logmsg(f"Timeout set to {tmax:.1f} seconds.")
    elif opt == 'test': doTest = True
    elif opt == 'path':
        for dir in os.getenv('PYTHONPATH').split(':'): print(dir)
        exit(0)
    else:
        statlogmsg(f"Invalid option: '{opt}'")
        sys.exit(1)

import parsl
from desc.wfmon import MonDbReader
import desc.sysmon

logmsg(f"parsl version is {parsl.VERSION}")
logmsg(f"parsl location is {parsl.__file__}")

if showParslTables:
  dbr = MonDbReader()
  dbr.tables(2)
  sys.exit()

from desc.gen3_workflow import start_pipeline
from desc.gen3_workflow import ParslGraph

bpsfile = 'config.yaml'
pickname = 'parsl_graph_config.pickle'
pickpath = ''

# Look for parsl graph pickle files.
pg_pickle_paths = []
pg_pickle_path = ''
for path, dirs, files in os.walk(thisdir, followlinks=True):
    if pickname in files:
        pg_pickle_paths += [f"{path}/{pickname}"]
if len(pg_pickle_paths) == 1:
    pg_pickle_path = pg_pickle_paths[0]
    statlogmsg(f"Found parsl pickle file: {pg_pickle_path}")
elif len(pg_pickle_paths):
    statlogmsg(f"Found {len(pg_pickle_paths)} parsl pickle files")
    for path in pg_pickle_paths:
        logmsg(f"    {path}")
    sys.exit(1)
elif not doInit:
    statlogmsg(f"Parsl pickle file not found: {pickname}")
    sys.exit(1)

if doTest:
    logmsg('test')

if doInit:
    if len(pickpath):
        statlogmsg("Remove existing job before starting a new one.")
        sys.exit(1)
    else:
        logmsg()
        statlogmsg("Creating quantum graph.")
        pg = start_pipeline(bpsfile)
        #qgid = pg.qgraph.graphID
        #statlogmsg(f"Created QG. ID is {qgid}")
        haveQG = get_haveQG(pg)
        if haveQG:
            statlogmsg("Quantum graph was created.")
        else:
            statlogmsg("Quantum graph was not created.")
else:
    if pg_pickle_path == '':
        statlogmsg("Parsl pickle file not found.")
        sys.exit(1)
    logmsg()
    logmsg(time.ctime(), "Using existing pipeline:", pg_pickle_path)
    haveQG = get_haveQG(pg)

if doQgReport:
    if not haveQG:
        statlogmsg("ERROR: Quantum graph not found.")
        sys.exit(1)
    fnam = 'qg-report.txt'
    ofil = open(fnam, 'w')
    ofil.write(f"          Graph ID: {pg.qgraph.graphID}\n")
    ofil.write(f"  Input node count: {len(pg.qgraph.inputQuanta)}\n")
    ofil.write(f" Output node count: {len(pg.qgraph.oputputQuanta)}\n")

if doProc:
    logmsg()
    statlogmsg('Starting workflow')
    get_pg()
    if not haveQG:
        statlogmsg("ERROR: Quantum graph not found.")
        sys.exit(1)
    pg.run()
    futures = [job.get_future() for job in pg.values() if not job.dependencies]
    ntsk = len(futures)
    statlogmsg(f"Workflow task count: {ntsk}")
    ndone = 0
    while ndone < ntsk:
        ndone = 0
        for fut in futures:
            if fut.done(): ndone += 1
        statlogmsg(f"Finished {ndone} of {ntsk} tasks.")
        time.sleep(10)
        dtim = time.time() - time0
        if dtim > tmax:
            statlogmsg(f"Timing out after {dtim:.1f} sec. {ndone}/{ntsk} tasks completed.")
            pg.shutdown()
            sys.exit(1)
    statlogmsg(f"Workflow complete: {ndone}/{ntsk} tasks.")

if doFina:
    statlogmsg()
    statlogmsg('Finalizing job...')
    get_pg()
    pg.finalize()
    statlogmsg('Finalizing done')

if showStatus:
    logmsg()
    statlogmsg("Fetching status")
    get_pgro()
    pgro.status()
    statlogmsg("Evaluating status summary.")
    df = pgro.df.query("task_type.notnull()")
    pandas.set_option('display.max_rows', 500)
    pandas.set_option('display.max_columns', 50)
    pandas.set_option('display.width', 1000)
    print(df)
    print(df.task_type)
    ntot = len(df)
    npen = len(df.query('status == "pending"'))
    nsch = len(df.query('status == "scheduled"'))
    nrun = len(df.query('status == "running"'))
    nsuc = len(df.query('status == "succeeded"'))
    nfai = len(df.query('status == "failed"'))
    nxdn = len(df.query('status == "exec_done"'))
    nrem = npen + nsch
    logmsg(f"    Total: {ntot:10}")
    logmsg(f"Exec_done: {nxdn:10}")
    logmsg(f"  Success: {nsuc:10}")
    logmsg(f"   Failed: {nfai:10}")
    logmsg(f"   Remain: {nrem:10}")
    statlogmsg(f"Finished {nxdn} of {ntot} tasks.")

logmsg("All steps completed.")
