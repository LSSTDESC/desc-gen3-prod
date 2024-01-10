#!/usr/bin/env python3

# Start the clock for timing out.
import time
time0 = time.time()

import os
import sys
import shutil
import dg3prod
import pandas
import signal
from desc.wfmon import MonDbReader
import traceback
import subprocess

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
getStatusFromLog = True  # If true, task status is retrieved from the task log file

thisdir = os.getcwd()
haveQG = False
monexpUpdated = False

pickname = 'parsl_graph_config.pickle'
pg_pickle_path = None     # Full path to the pg pickle file
pg = None                 # ParlslGraph used for processing
pgro = None               # ParslGraph for checking status

# Send a list of messages to the job log and optionally to the status log.
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

# Send a message to the job log.
def logmsg(*msgs, update_status=False):
    logmsglist(list(msgs), update_status)

# Send a message to the job log and status log.
def statlogmsg(*msgs):
    logmsglist(list(msgs), update_status=True)

# Update monitor log fnam.
def logmon(fnam, msg):
    myname = 'logmon'
    try:
        with open(fnam, 'a') as fil:
            fil.write(f"{time.time():18.6f} {msg}\n")
    except Exception as e:
        logmsg(f"{myname}: ERROR: {e}")

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
                com = 'g3wf-run-task'
                logmsg(f"get_pg: Location for {com}: {shutil.which(com)}")
                pg = ParslGraph.restore(pg_pickle_path)
    haveQG = get_haveQG(pg)
    return pg

def update_monexp():
    myname = "update_monexp"
    global monexpUpdated
    if monexpUpdated: return True
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
        monexpUpdated = True
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return False
    return True

# Return the directory holding the output task data for this job.
_task_output_data_dir = None
_task_log_dir = None
_task_timestamp = None
def task_output_data_dir():
    myname = 'task_output_data_dir'
    global _task_output_data_dir
    global _task_timestamp
    global _task_log_dir
    if _task_output_data_dir is None:
        import yaml
        doc = yaml.load(open('config.yaml'), Loader=yaml.SafeLoader)
        pnam = doc['payload']['payloadName']
        onam = doc['operator']
        tldir = f"{thisdir}/submit/u/{onam}/{pnam}"
        ret = subprocess.run(['ls', tldir], capture_output=True)
        if ret.returncode:
            logmsg(f"{myname}: Error {ret.returncode}: {ret.stderr}")
            return None
        if ret.stdout is None:
            logmsg(f"{myname}: Log base directory is empty: {tldir}")
            return None
        _task_timestamp = ret.stdout.decode().strip()
        _task_log_dir = f"{tldir}/{_task_timestamp}"
        _task_output_data_dir = f"{doc['payload']['butlerConfig']}/u/{onam}/{pnam}/{_task_timestamp}"
        logmsg(f"{myname}: Task output data dir: {_task_output_data_dir}")
        logmsg(f"{myname}: Task log dir: {_task_log_dir}")
    return _task_output_data_dir

# Return the space used in the output data dir.
def task_output_data_size():
    myname = 'task_output_data_size'
    tdir = task_output_data_dir()
    try:
        ret = subprocess.run(['du', '-bs', tdir], capture_output=True)
        nbyte = int(ret.stdout.decode().split()[0])
    except Exception as e:
        logmsg(f"{myname}: WARNING: {e}")
        nbyte = 0
    return nbyte

# Return the space free in the output data dir.
def task_output_data_df(unitin='kiB'):
    myname = 'task_output_data_free'
    tdir = task_output_data_dir()
    if unitin in ['MiB', 'GiB', 'TiB']:
        unit = unitin
    else:
        unit = 'kiB'
    out = {'dir':tdir, 'unit':unit}
    cfac = None
    if unit == 'MiB':
        cfac = 1024
    elif unit == 'GiB':
        cfac = 1024*1024
    elif unit == 'TiB':
        cfac = 1024*1024*1024
    elif unit == 'PiB':
        cfac = 1024*1024*1024*1024
    try:
        ret = subprocess.run(['df', '-k', tdir], capture_output=True)
        out['time'] = time.time()
        vals = ret.stdout.decode().split('\n')[1].split()
        out['filesystem'] = vals[0]
        out['total'] = int(vals[1])
        out['used'] = int(vals[2])
        out['free'] = int(vals[3])
        out['mount'] = vals[5]
        if unit != 'byte':
            for nam in ['total', 'used', 'free']:
                out[nam] /= cfac
    except Exception as e:
        out['error'] = str(e)
    return out

#################################################################################

logmsg(f"Executing {__file__}")
statlogmsg(f"Running g3wfpipe version: {dg3prod.version()}")
for opt in sys.argv[1:]:
    logmsg("Processing argument", opt)
    if opt in ["-h", "help"]:
        print('Usage:', sys.argv[0], '[OPTS]')
        print('  Supported valuse for OPTS:')
        print('    init - Create QG (QuantumGraph).')
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

doProc0 = False
doProc1 = False
doProc2 = doProc

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
        com = 'g3wf-run-task'
        logmsg(f"doInit: Location for {com}: {shutil.which(com)}")
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
        update_monexp()

if doQgReport:
    if not haveQG:
        statlogmsg("ERROR: Quantum graph not found.")
        sys.exit(1)
    fnam = 'qg-report.txt'
    ofil = open(fnam, 'w')
    ofil.write(f"          Graph ID: {pg.qgraph.graphID}\n")
    ofil.write(f"  Input node count: {len(pg.qgraph.inputQuanta)}\n")
    ofil.write(f" Output node count: {len(pg.qgraph.oputputQuanta)}\n")

if doProc2:
    logmsg()
    monexpUpdate = False
    statlogmsg('Fetching workflow QG.')
    get_pg()
    if not haveQG:
        statlogmsg("ERROR: Quantum graph not found.")
        sys.exit(1)
    statlogmsg('Starting new workflow')
    typenames = pg._task_list
    type_tasknames = dict()
    all_tasknames = []
    for typename in typenames:
        type_tasknames[typename] = pg.get_jobs(typename)
        all_tasknames += type_tasknames[typename]
    ntask = len(all_tasknames)
    end_tasknames = []
    for taskname in all_tasknames:
        task = pg[taskname]
        if not task.dependencies:
            end_tasknames.append(taskname)
    logmsg(f"Total task count is {len(all_tasknames)}")
    logmsg(f"Endpoint task count is {len(end_tasknames)}")
    nend_start = 0
    task_output_data_dir()
    for taskname in end_tasknames:
        task = pg[taskname]
        task.get_future()
        nend_start += 1
    task_output_data_dir()
    logmsg(f"Endpoint start count is {len(end_tasknames)}")
    ndone = 0
    nsucc = 0
    nfail = 0
    nlbad = 0
    rem_tasknames = all_tasknames
    logmsg(f"Monitoring DB: {pg.monitoring_db}")
    tsleep = 10
    last_counts = []
    nsame_counts = 0
    dfmap_last = None
    while True:
        newrems = []
        try:
            pg._update_status()
        except:
            logmsg(f"WARNING: Unable to update status for ParlsGraph.")
            time.sleep(tsleep)
            continue
        tstats = pg.df.set_index('job_name').status.to_dict()
        npend = 0
        nlaun = 0
        nrunn = 0
        for tnam in rem_tasknames:
            tstat = tstats[tnam]
            if tstat in ('exec_done'):
                ndone += 1
                if getStatusFromLog:
                    task = pg[tnam]
                    log_tstat = task.status
                    if log_tstat == 'succeeded':
                        nsucc += 1
                    elif log_tstat == 'failed':
                        nfail += 1
                    else:
                        nlbad += 1
                        logmsg(f"WARNING: Unexpected log task status: {log_tstat}")
            else:
                if tstat == 'pending':
                    npend += 1
                elif tstat == 'launched':
                    nlaun += 1
                elif tstat == 'running':
                    nrunn += 1
                else:
                    logmsg(f"WARNING: Unexpected task status: {tstat}")
                newrems.append(tnam)
        rem_tasknames = newrems
        msg = f"Finished {ndone} of {ntask} tasks."
        counts = [nfail,    nlbad,     npend,     nlaun,      nrunn]
        clabs =  ['failed', 'bad log', 'pending', 'launched', 'running']
        for i in range(len(counts)):
            if counts[i]:
                msg += f" {counts[i]} {clabs[i]}."
        statlogmsg(msg)
        nbyte = task_output_data_size()
        ngib = nbyte/(1024*1024*1024)
        dfmap = task_output_data_df('GiB')
        dfmap['task'] = ngib
        ngibfree = -1
        if 'free' in dfmap:
            ngibfree = dfmap['free']
            freemsg = f"available: {dfmap['free']:.0f} {dfmap['unit']} on {dfmap['mount']}"
        else:
            freemsg = dfmap['error']
        rate = 0
        if dfmap_last is not None:
            try:
                dngib = dfmap['task'] - dfmap['task']
                dtime = dfmap['time'] - dfmap['time']
                rate = dngib/dtime
            except Exception as e:
                logmsg(f"Error calculating outpur rate: {e}")
            dfmap_last = dfmap
        ratemsg = f"rate: {rate:7.3f} GiB/sec)'"
        logmsg(f"Task output size: {ngib:10.3f} GiB, {ratemsg}, {freemsg}'")
        logmon('task-output-size.log', f"{ngib:13.6f} {ngibfree:15.6f}")
        update_monexp()
        if len(rem_tasknames) == 0: break
        if counts == last_counts:
            nsame_counts += 1
            if  nsame_counts > 5 and nlaun == 0 and nrunn == 0:
                logmsg(f"Aborting job because state is not changing and no tasks are active.")
                break
        else:
            nsame_counts = 0
            last_counts = counts
        task_output_data_dir()
        time.sleep(tsleep)

if doProc1:
    logmsg()
    monexpUpdate = False
    statlogmsg('Fetching workflow QG.')
    get_pg()
    if not haveQG:
        statlogmsg("ERROR: Quantum graph not found.")
        sys.exit(1)
    statlogmsg('Starting new workflow')
    tasks = pg.values()
    ntask = len(tasks)
    endpoints = [task for task in tasks if not task.dependencies]
    for task in endpoints:
        task.get_future()
    ndone = 0
    nfail = 0
    remtasks = tasks
    while True:
        newrems = []
        for task in remtasks:
            tstat = task.status
            if tstat in ('succeeded', 'failed'):
                if tstat == 'failed': nfail += 1
                ndone += 1
            else:
                if tstat not in ('pending', 'scheduled', 'running'):
                    logmsg(f"WARNING: Unexpected task status: {tstat}")
                newrems += [task]
        remtasks = newrems
        msg = f"Finished {ndone} of {ntask} tasks."
        if nfail:
            msg += f" {nfail} failed."
        statlogmsg(msg)
        update_monexp()
        if len(remtasks) == 0: break
        time.sleep(10)

if doProc0:
    logmsg()
    monexpUpdate = False
    statlogmsg('Fetching workflow QG.')
    get_pg()
    if not haveQG:
        statlogmsg("ERROR: Quantum graph not found.")
        sys.exit(1)
    statlogmsg('Starting old workflow')
    pg.run()
    count = 0
    futures = None
    statlogmsg('Retrieving futures.')
    while futures is None:
        count += 1
        try:
            ntskall = len(pg.values())
            statlogmsg(f"Try {count} task count: {ntskall}")
            #futures = [job.get_future() for job in pg.values() if not job.dependencies]
            futures = [job.get_future() for job in pg.values()]
            #traceback.print(e.__traceback__)
        except Exception as e:
            logmsg(f"Try {count} raised exception: {e}")
            if count > 100:
                statlogmsg("Unable to retrieve futures.")
                sys.exit(1)
            futures = None
    ntsk = len(futures)
    statlogmsg(f"Ready/total task count: {ntsk}/{ntskall}")
    ndone = 0
    while ndone < ntsk:
        update_monexp()
        ndone = 0
        for fut in futures:
            if fut.done(): ndone += 1
        statlogmsg(f"Finished {ndone} of {ntsk} tasks.")
        time.sleep(10)
    statlogmsg(f"Workflow complete: {ndone}/{ntsk} tasks.")
    update_monexp()

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
