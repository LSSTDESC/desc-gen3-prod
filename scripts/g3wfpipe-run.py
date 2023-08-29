#!/usr/bin/env python3

# Start the clock for timing out.
import time
time0 = time.time()

import os
import sys
import dg3prod
import pandas
import signal
from desc.wfmon import MonDbReader
import traceback

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
doButlerTest = False

thisdir = os.getcwd()
haveQG = False
monexp_udated = False

pickname = 'parsl_graph_config.pickle'
pg_pickle_path = None     # Full path to the pg pickle file
pg = None                 # ParlslGraph used for processing
pgro = None               # ParslGraph for checking status

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

# Look for parsl graph pickle files.
def get_pg_pickle_path():
    global pg_pickle_path
    pg_pickle_paths = []
    pg_pickle_path = ''
    for path, dirs, files in os.walk(thisdir, followlinks=True):
        if pickname in files:
            pg_pickle_paths += [f"{path}/{pickname}"]
    if len(pg_pickle_paths) == 1:
        pg_pickle_path = pg_pickle_paths[0]
        statlogmsg(f"Found parsl pickle file.")
        logmsg(f"Parsl pickle file: {pg_pickle_path}")
    elif len(pg_pickle_paths):
        statlogmsg(f"Found {len(pg_pickle_paths)} parsl pickle files")
        for path in pg_pickle_paths:
            logmsg(f"    {path}")
        sys.exit(1)

def get_haveQG(pg, noisy=False):
    try:
        if pg.qgraph is None:
            return False
    except:
        if noisy: statlogmsg("Check of quantum graph raised an exception.")
    return True

def get_pg(readonly=False, require=True):
    global pg
    global pgro
    global haveQG
    if pg is None:
        get_pg_pickle_path()
        if require and len(pg_pickle_path) == 0:
            statlogmsg(f"Parsl pickle file not found: {pickname}")
            sys.exit(1)
        if len(pg_pickle_path) > 0:
            if readonly:
                pgro = ParslGraph.restore(pg_pickle_path, use_dfk=False)
                haveQG = get_haveQG(pgro)
                return pgro
            else:
                pg = ParslGraph.restore(pg_pickle_path)
    haveQG = get_haveQG(pg)
    return pg

def update_monexp():
    myname = "update_monexp"
    try:
        logmsg(f"Fetching the parsl run ID.")
        dbr = MonDbReader('runinfo/monitoring.db', fix=False, dodelta=False)
        if dbr is None:
            print(f"{myname}: Unable to open monitoring DB.")
            return False
        run_id = dbr.latest_run_id()
        if run_id is None:
            logmsg(f"{myname}: Run ID not found.")
            return False
        else:
            logmsg(f"Run ID: {run_id}")
            line = f"monexp.run_id = '{run_id}'"
        ofil = open('monexp.py', 'a')
        ofil.write(f"{line}\n")
        ofil.close()
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return False
    return True

#################################################################################

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
    elif opt == 'test': doTest = True
    elif opt == 'path':
        for dir in os.getenv('PYTHONPATH').split(':'): print(dir)
        exit(0)
    elif opt == 'butler':
        doButlerTest = True
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

if doTest:
    logmsg('test')

if doButlerTest:
    statlogmsg("Setting up to test the butler.")
    import lsst.daf.butler as daf_butler
    repo = '/global/cfs/cdirs/lsst/production/gen3/DC2/Run2.2i/repo'
    butler = daf_butler.Butler(repo)
    statlogmsg("Fetching butler collections.")
    collections = butler.registry.queryCollections()
    statlogmsg(f"Butler has {len(collections)} collections.")

if doInit:
    get_pg(require=False)
    if len(pg_pickle_path):
        statlogmsg("Remove existing job before starting a new one.")
        sys.exit(1)
    else:
        statlogmsg("Creating quantum graph.")
        # The log for the QG generati is at submit/u/<user>/<name>/<timestamp>/quantumGraphGeneration.out
        # Also see execution_butler_creation.out in that same dir.
        # There should messages reporting daset transfers after the quanta are reported.
        pg = start_pipeline(bpsfile)
        #qgid = pg.qgraph.graphID
        #statlogmsg(f"Created QG. ID is {qgid}")
        statlogmsg("Checking quantum graph.")
        haveQG = get_haveQG(pg)
        if haveQG:
            statlogmsg("Quantum graph was created.")
        else:
            statlogmsg("Quantum graph was not created.")

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
    statlogmsg('Fetching workflow QG.')
    get_pg()
    if not haveQG:
        statlogmsg("ERROR: Quantum graph not found.")
        sys.exit(1)
    statlogmsg('Starting workflow')
    pg.run()
    count = 0
    futures = None
    statlogmsg('Retrieving futures.')
    while futures is None:
        count += 1
        try:
            ntskall = len(pg.values())
            statlogmsg(f"Try {count} task count: {ntskall}")
            futures = [job.get_future() for job in pg.values() if not job.dependencies]
        except Exception as e:
            logmsg(f"Try {count} raised exception: {e}")
            if count > 10:
                statlogmsg("Unable to retrieve futures.")
                sys.exit(1)
            futures = None
    ntsk = len(futures)
    statlogmsg(f"Ready/total task count: {ntsk}/{ntskall}")
    ndone = 0
    while ndone < ntsk:
        if not monexpUpdated: monexpUpdated = update_monexp()
        ndone = 0
        for fut in futures:
            if fut.done(): ndone += 1
        statlogmsg(f"Finished {ndone} of {ntsk} tasks.")
        time.sleep(10)
    statlogmsg(f"Workflow complete: {ndone}/{ntsk} tasks.")
    if not monexpUpdated: monexpUpdated = update_monexp()

if doFina:
    statlogmsg()
    statlogmsg('Finalizing job...')
    get_pg()
    pg.finalize()
    statlogmsg('Finalizing done')

if showStatus:
    logmsg()
    statlogmsg("Fetching status")
    get_pg(readonly=True)
    if pgro is None:
        statlogmsg('Workflow has not started.')
    else:
        pgro.status()
        statlogmsg("Evaluating status summary.")
        df = pgro.df[pgro.df['task_type'].isin(pgro._task_list)]
        if False:
            pandas.set_option('display.max_rows', 500)
            pandas.set_option('display.max_columns', 50)
            pandas.set_option('display.width', 1000)
            pandas.set_option('display.max_colwidth', 500)
            print(df)
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
        msg = f"Finished {nxdn} of {ntot} tasks."
        if nfai: msg += f" ({nfai} failed.)"
        statlogmsg(msg)

logmsg("All steps completed.")
