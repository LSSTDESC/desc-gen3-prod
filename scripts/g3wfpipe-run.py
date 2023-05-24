#!/usr/bin/env python3

import os
import sys
import time

statfilename = 'current-status.txt'
doInit = False
doProc = False
doFina = False
blockJob=True      # Set false for interactive running.
showStatus = False
showParslTables = False
doWorkflow = True
doTest = False

thisdir = os.getcwd()

def logmsg(*msgs, update_status=False):
    out = open('runapp-g3wfpipe.log', 'a')
    dmsg = time.strftime('%Y-%m-%d %H:%M:%S:')
    for msg in msgs:
        dmsg += ' ' + str(msg)
    out.write(dmsg + '\n')
    out.close()
    print(dmsg, flush=True)
    if update_status:
        fstat = open(statfilename, 'w')
        fstat.write(dmsg + '\n')

def statlogmsg(*msgs):
    logmsg(msgs, update_status=True)

statlogmsg(f"Executing {__file__}")
for opt in sys.argv[1:]:
    logmsg("Processing argument", opt)
    if opt in ["-h", "help"]:
        logmsg('Usage:', sys.argv[0], '[OPTS]')
        logmsg('  OPTS = init, proc, finalize, status, tables, help')
        sys.exit()
    elif opt == 'init':
        doInit = True
    elif opt == 'proc':
        doProc = True
    elif opt == 'finalize':
        doFina = True
    elif opt == 'status':
        showStatus = True
    elif opt == 'tables':
        showParslTables = True
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
# Check if pickle file already exists.
for path, dirs, files in os.walk('submit'):
    if pickname in files:
        pickpath = path + '/' + pickname
        logmsg(f"Workflow pickle path: {pickpath}")
    else:
        logmsg(f"Workflow pickle not found: {pickpath}")

if doTest:
    logmsg('test')

if doInit:
    if len(pickpath):
        statlogmsg("Remove existing job before starting a new one.")
        sys.exit(1)
    else:
        logmsg()
        statlogmsg("Creating quantum graph with " + bpsfile)
        pg = start_pipeline(bpsfile)
        try:
            if pg.qgraph() is None:
                statlogmsg("Quantum graph was not created.")
            else:
                statlogmsg("Quantum graph was created.")
        except:
            pass

if len(pickpath):
    pickpath = f"{thisdir}/{pickpath}"
    logmsg()
    logmsg(time.ctime(), "Using existing pipeline:", pickpath)
    pg = ParslGraph.restore(pickpath)

if doProc:
    logmsg()
    statlogmsg('Starting workflow')
    pg.run()
    futures = [job.get_future() for job in pg.values() if not job.dependencies]
    ntsk = len(futures)
    statlogmsg(f"Workflow task count: {ntsk}")
    ndone = 0
    time0 = time.time()
    tmax = 500
    while ndone < ntsk:
        ndone = 0
        for fut in futures:
            if fut.done(): ndone += 1
        statlogmsg(f"Finished {ndone} of {ntsk} tasks.")
        time.sleep(10)
        dtim = time.time() - time0
        if dtim > tmax:
            statlogmsg(f"Timing out after {dtim} seconds with {ndone}/{ntsk} tasks completed.")
            sys.exit(1)
    statlogmsg(f"Workflow complete: {ndone}/{ntsk} tasks.")

if doFina:
    statlogmsg()
    logmsg('Finalizing job...')
    pg.finalize()
    statlogmsg('Finalizing done')

if showStatus:
    logmsg()
    logmsg("Fetching status")
    pg.status()


