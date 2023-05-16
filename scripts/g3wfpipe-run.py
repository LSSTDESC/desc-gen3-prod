import os
import sys
import time
import parsl
from desc.wfmon import MonDbReader
import desc.sysmon

startJob = False
restartJob = False
finalizeJob = False
blockJob=True      # Set false for interactive running.
showStatus = False
showParslTables = False
doWorkflow = True
doTest = False

thisdir = os.getcwd()

def logmsg(*msgs):
    out = open('dopipe.log', 'a')
    dmsg = time.strftime('%Y-%m-%d %H:%M:%S:')
    for msg in msgs:
        dmsg += ' ' + str(msg)
    out.write(dmsg + '\n')
    out.close()
    print(dmsg, flush=True)

for opt in sys.argv[1:]:
    logmsg("ARG", opt)
    if opt in ["-h", "help"]:
        logmsg('Usage:', sys.argv[0], '[OPTS]')
        logmsg('  OPTS = start, restart, status, tables, help')
        sys.exit()
    elif opt == 'start': startJob = True
    elif opt == 'restart': restartJob = True
    elif opt == 'finalize': finalizeJob = True
    elif opt == 'status': showStatus = True
    elif opt == 'tables':
        showParslTables = True
    elif opt == 'test': doTest = True
    else:
        logmsg('Invalid option', opt)
        sys.exit()

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
        logmsg(f"Pickle path: {pickpath}")

if doTest:
    logmsg('test')

if startJob:
    if len(pickpath):
        logmsg("Remove existing job before starting a new one.")
        logmsg("Rename submit.")
    else:
      logmsg()
      logmsg("Starting pipeline with " + bpsfile)
      #sysmon = desc.sysmon.reporter(fnam='sysmon.csv', log='sysmon.log', dt=1, thr=True)
      pg = start_pipeline(bpsfile)
      logmsg()
      msuf = "with"
      if not finalizeJob: msuf = msuf + 'out'
      msuf = msuf + ' finalization'
      logmsg(f"Starting workflow {msuf}")
      pg.run(block=blockJob, finalize=finalizeJob)
      finalizeJob = False
      restartJob = False
      logmsg("Workflow started.")

if len(pickpath):
    pickpath = f"{thisdir}/{pickpath}"
    logmsg()
    logmsg(time.ctime(), "Using existing pipeline:", pickpath)
    pg = ParslGraph.restore(pickpath)

if restartJob:
    logmsg()
    logmsg('Restarting job')
    pg.run()
    logmsg("Workflow restarted.")

if finalizeJob:
    logmsg()
    logmsg('Finalizing job...')
    pg.run(block=True, finalize=True)
    logmsg('Finalizing done')

if showStatus:
    logmsg()
    logmsg("Fetching status")
    pg.status()


