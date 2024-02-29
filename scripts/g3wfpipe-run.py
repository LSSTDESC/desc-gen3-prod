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

######## Logging helpers ########

# Messages at level 1 are sent to stdout.
# All messages are sent to the runapp log runapp-g3wfpipe.log.
# All messages are prepended with the ate and time.

# Send a list of messages to the job log and optionally to the status log.
# ff any messge has line separators, each of those sub-lines is printed
# on a separate line.
loglev = 1   # Use 2 to get debugging messages
def logmsglist(amsgs, lev=1, update_status=False):
    dostd = lev <= 1 or lev <= loglev
    dolog = True
    msgs = amsgs if type(amsgs) is list else [amsgs]
    out = open('runapp-g3wfpipe.log', 'a')
    dmsg = time.strftime('%Y-%m-%d %H:%M:%S:')
    lines = []
    for msg in msgs:
        if type(msg) is str:
            if msg.find('\n'):
                lines += msg.split('\n')
                if lines[-1] == '':
                    lines = lines[0:-1]
            else:
                lines.append(msg.rstrip())
        else:
            lines.append(str(msg))
    for line in lines:
        fline = f"{dmsg} {line}"
        if dolog: out.write(fline + '\n')
        if dostd: print(fline, flush=True)
    out.close()
    if update_status:
        fstat = open(statfilename, 'w')
        fstat.write(lines[0] + '\n')

# Log a level 1 message.
def logmsg(*msgs):
    logmsglist(list(msgs), 1, False)

# Log a level 2 message.
def dbglogmsg(*msgs):
    logmsglist(list(msgs), 2, False)

# Log a level 1 message and append it to the status log.
def statlogmsg(*msgs):
    logmsglist(list(msgs), 0, True)

# Update monitor log fnam.
def logmon(fnam, msg):
    myname = 'logmon'
    try:
        with open(fnam, 'a') as fil:
            fil.write(f"{time.time():18.6f} {msg}\n")
    except Exception as e:
        logmsg(f"{myname}: ERROR: {e}")

######## Starting task code ########

# Fetch the starting tasks for the subgraph of pg including task tnam.
def get_starting_tasks(tnam, pg):
    tnams1 = {tnam}
    outnams = set()
    while True:
        tnams2 = set()
        done = True
        for tnam1 in tnams1:
            prqs = pg[tnam1].prereqs
            if len(prqs):
                done = False
                for prq in prqs:
                    prqnam = prq.gwf_job.name
                    assert(type(prqnam) is str)
                    tnams2.add(prqnam)
            else:
                outnams.add(tnam1)
        if len(tnams2):
            tnams1 = tnams2
        else:
            break
    return outnams

######## Config and globals ########

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

######## Parsl graph helpers ########

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

######## Task directory helpers ########

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

######## Main code ########

logmsg(f"Executing {__file__}")
statlogmsg(f"Running g3wfpipe version: {dg3prod.version()}")
for opt in sys.argv[1:]:
    logmsg(f"Processing argument {opt}")
    if opt in ["-h", "help"]:
        print('Usage:', sys.argv[0], '[OPTS]')
        print('  Supported valuse for OPTS:')
        print('    init - Create QG (QuantumGraph).')
        print('    proc - Process tasks in the existing QG.')
        print('    status - Report on the status of processing for the existing QG.')
        print('    qgre - Prepare report describing the existing QG.')
        print('    tables - Prepare report describing the parsl tables.')
        print('    finalize - Register output datasets for existing QG.')
        print('    maxcst=VAL - Set maxcst to VAL.')
        print('    maxact=VAL - Set maxact to VAL.')
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
        sys.exit(0)
    elif opt == 'butler':
        doButlerTest = True
    elif opt[0:7] == 'maxcst=':
        maxcst = int(opt[7:])
    elif opt[0:7] == 'maxact=':
        maxact = int(opt[7:])
    else:
        statlogmsg(f"Invalid option: '{opt}'")
        sys.exit(1)

import parsl
from desc.wfmon import MonDbReader
import desc.sysmon

logmsg(f"parsl version is {parsl.VERSION}")
logmsg(f"parsl location is {parsl.__file__}")
try:
    ret = subprocess.run(['work_queue_worker', '-v'], capture_output=True)
    assert(ret.returncode == 0)
    logmsg(ret.stdout.decode().split('\n')[0])
except:
    logmsg("Unable to determine work_queue version")

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
    logmsg()
    statlogmsg("Setting up to test the butler.")
    import lsst.daf.butler as daf_butler
    repo = '/global/cfs/cdirs/lsst/production/gen3/DC2/Run2.2i/repo'
    butler = daf_butler.Butler(repo)
    statlogmsg("Fetching butler collections.")
    collections = butler.registry.queryCollections()
    statlogmsg(f"Butler has {len(collections)} collections.")

if doInit:
    logmsg()
    statlogmsg("Initializing pipeline.")
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
    logmsg()
    statlogmsg("Creating pipeline QG report.")
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
    statlogmsg(f"Processing pipeline: maxact={maxact}, maxcst={maxcst}.")
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
    start_tasknames = []
    end_tasknames = []
    for taskname in all_tasknames:
        task = pg[taskname]
        if not task.dependencies:
            end_tasknames.append(taskname)
        if not task.prereqs:
            start_tasknames.append(taskname)
    ntask_start = len(start_tasknames)
    ntask_end = len(end_tasknames)
    logmsg(f"Total task count is {len(all_tasknames)}")
    logmsg(f"Starting task count is {ntask_start}")
    logmsg(f"Endpoint task count is {ntask_end}")
    from lsst.ctrl.bps import GenericWorkflowJob
    from lsst.ctrl.bps import GenericWorkflowExec
    from desc.gen3_workflow import ParslJob
    ipst = 0
    # Loop until all tasks are complete or a problem arises.
    ndone = 0
    nsucc = 0
    nfail = 0
    nlbad = 0
    nfail_update = 0
    maxfail_update = 2
    ndone_start = 0
    rem_tasknames = all_tasknames
    logmsg(f"Monitoring DB: {pg.monitoring_db}")
    tsleep = 10
    last_counts = []
    nsame_counts = 0
    dfmap_last = None
    maxfail = 100
    nchain_rem = len(end_tasknames)
    nactive_chain = 0           # Number currently active
    nactivated_chain = 0        # Number ever activated
    nactive_chain_at_start = 0  # Number active that have not finished their first task
    # Loop until processing completes.
    while True:
        # Fetch the current processing status for all tasks.
        try:
            pg._update_status()
        except Exception as e:
            logmsg(f"WARNING: Unable to update status for ParlsGraph: {str(e)}")
            dbglogmsg(traceback.format_exc())
            nfail_update += 1
            if nfail_update >= maxfail_update:
                statlogmsg(f"Aborting job for too many parsl graph update failures: {nfail_update} >= {maxfail_update}.")
                os._exit(102)
            logmsg(f"Checking parsl graph job names.")
            logmsg(f"Count is {len(pg)}.")
            icnt = 0
            for tnam in pg:
                if type(tnam) is not str:
                    print(f"ERROR: Task has unexpected type: {type(tnam)}")
                print(f"{icnt:>4}: {tnam}")
                icnt += 1
            time.sleep(tsleep)
            continue
        tstats = pg.df.set_index('job_name').status.to_dict()
        # Use this to fetch the number of tasks in each state and update
        # the remaining list of tasks to be processed.
        newrems = []
        npend = 0
        nlaun = 0
        nrunn = 0
        npend_start = 0
        nlaun_start = 0
        nrunn_start = 0
        nstat_diff = 0
        for tnam in rem_tasknames:
            tstat = tstats[tnam]
            task = pg[tnam]
            tstat_pj = task.status
            #if tstat != tstat_pj:
            #    dbglogmsg(f"{nstat_diff:3}: Status differs in table and object: {tstat} != {tstat_pj}")
            #    nstat_diff += 1
            is_start = tnam in start_tasknames  # Is this a starting task?
            is_end = tnam in end_tasknames  # Is this an ending task?
            is_done = False
            if tstat in ('exec_done'):
                dbglogmsg(f"Finished task {tnam}")
                ndone += 1
                if getStatusFromLog:
                    log_tstat = task.status
                    if log_tstat == 'succeeded':
                        nsucc += 1
                    elif log_tstat == 'failed':
                        nfail += 1
                    else:
                        nlbad += 1
                        logmsg(f"WARNING: Unexpected log task status: {log_tstat}")
            elif tstat in ('failed', 'dep_fail'):
                logmsg(f"WARNING: Task {tnam} failed with status: {tstat}")
                nfail += 1
            else:
                if tstat == 'pending':
                    npend += 1
                    if is_start: npend_start += 1
                elif tstat == 'launched':
                    nlaun += 1
                    if is_start: nlaun_start += 1
                elif tstat == 'running':
                    nrunn += 1
                    if is_start: nrunn_start += 1
                else:
                    logmsg(f"WARNING: Task {tnam} has unexpected status: {tstat}")
                newrems.append(tnam)
            if is_done:
                if is_start:
                    ndone_start += 1
                    nactive_chain_at_start -= 1
                if is_end:
                    nactive_chain -= 1
        rem_tasknames = newrems
        # Display the processing status.
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
                dngib = dfmap['task'] - dfmap_last['task']
                dtime = dfmap['time'] - dfmap_last['time']
                rate = dngib/dtime
            except Exception as e:
                logmsg(f"Error calculating outpur rate: {e}")
        dfmap_last = dfmap
        ratemsg = f"rate: {rate:7.3f} GiB/sec"
        logmsg(f"Task output size: {ngib:10.3f} GiB, {ratemsg}, {freemsg}")
        logmon('task-output-size.log', f"{ngib:13.6f} {ngibfree:15.6f}")
        update_monexp()
        # Exit if there are too many failures.
        if nfail >= maxfail:
            logmsg(f"Aborting job for too many task failures: {nfail} >= {maxfail}.")
            os._exit(101)
        # Exit loop if processing is done.
        if len(rem_tasknames) == 0: break
        # Exit if we are not making progress.
        if counts == last_counts:
            nsame_counts += 1
            if  nsame_counts > 5 and nlaun == 0 and nrunn == 0:
                logmsg(f"Aborting job because state is not changing and no tasks are active.")
                sys.exit(102)
        else:
            nsame_counts = 0
            last_counts = counts
        # Activate new chains.
        nactivate = ntask_end - nactivated_chain
        dbglogmsg(f"Chain counts:")
        dbglogmsg(f"  nactive_chain_at_start: {nactive_chain_at_start}")
        dbglogmsg(f"           nactive_chain: {nactive_chain}")
        dbglogmsg(f"        nactivated_chain: {nactivated_chain}")
        dbglogmsg(f"       Initial nactivate: {nactivate}")
        if maxact > 0:
            max_activate = maxact - nactive_chain
            dbglogmsg(f"           ACT nactivate: {max_activate}")
            if max_activate < nactivate: nactivate = max_activate
        if maxcst > 0:
            max_activate = maxcst - nactive_chain_at_start
            dbglogmsg(f"           CST nactivate: {max_activate}")
            if max_activate < nactivate: nactivate = max_activate
        dbglogmsg(f"         Final nactivate: {nactivate}")
        for iend in range(nactivated_chain, nactivated_chain + nactivate):
            taskname = end_tasknames[iend]
            task = pg[taskname]
            dbglogmsg(f"Activating chain {iend:4}: {taskname}")
            task.get_future()
            nactive_chain += 1
            nactive_chain_at_start += 1
            nactivated_chain += 1
        # Sleep.
        time.sleep(tsleep)
if doFina:
    logmsg()
    statlogmsg("Finalizing pipeline.")
    get_pg()
    pg.finalize()
    statlogmsg('Finalizing done')

if showStatus:
    logmsg()
    statlogmsg("Fetching pipeline status")
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
