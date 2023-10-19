import os
import sys
from lsst.pipe.base import QuantumGraph
import lsst.daf.butler as modbutler

def myexit(rc):
    exit(rc)

myname = 'qgread'
maxfil = 8
xfer = True

outrepo=''
maxqua = 0
if len(sys.argv) > 1:
    outrepo = sys.argv[1]
if len(sys.argv) > 2:
    maxqua = int(sys.argv[2])
if len(outrepo) == 0 or outrepo == '-h':
    print(f"Usage: python qgread.py REPO")
    myexit(0)

repo = "/global/cfs/cdirs/lsst/production/gen3/DC2/Run2.2i/repo"
butler = modbutler.Butler(repo)

showCollections = True
if showCollections:
    cnams = sorted(butler.registry.queryCollections())
    print(f"{myname}: Repo has {len(cnams)} collections:")
    #for cnam in cnams:
    #    print(f"{myname}:   {cnam}")

show = True
user = 'dladams'
run = 1066
srun = str(run)
while len(srun) < 6: srun = '0' + srun
topdir = f"/pscratch/sd/{user[0:1]}/{user}/descprod-out/jobs/job{srun}/submit"
qnams = []
for path, dnams, fnams in os.walk(topdir):
    for fnam in fnams:
        if fnam.endswith('.qgraph'):
            qnams.append(f"{path}/{fnam}")
if len(qnams) == 0:
    print(f"{myname}: No quantum graphs found.")
    myexit(1)
if len(qnams) > 1:
    print(f"{myname}: Multiple quantum graphs found:")
    for qnam in qnams:
        print(f"{myname}:   {qnam}")
    myexit(2)
qfnam = qnams[0]

print('Loading quantum graph...')
qg = QuantumGraph.loadUri(qfnam)

if show:
    print(f"Graph ID: {qg.graphID}")

dtyps = {}
rawfilnam = outrepo +  '/raw.txt'
rawfil = open(rawfilnam, 'w')
for dnam in qg.allDatasetTypes:
    try:
        dtyps[dnam] = butler.registry.getDatasetType(dnam)
    except:
        print(f"{myname}: Ignoring unknown dataset type {dnam}")
print(f"{myname}: Found {len(dtyps)} of {len(qg.allDatasetTypes)} dataset types:")
for dnam, dtyp in dtyps.items():
    print(f"{dnam:>10}: {dtyp}")
    if xfer:
        outbutler = modbutler.Butler(outrepo, writeable=True)
        outbutler.registry.registerDatasetType(dtyp)

print()
nqua = 0
for qnode in qg.inputQuanta:
    nqua = nqua + 1
ssub = ''
if maxqua:
    ssub = f"{maxqua} of "
print(f"{myname}: Looping over {ssub}{nqua} inputquanta.")
irawfil = 0
ifil = 0
idup = 0
drefs = []
keeptypes = []
quit = False
iqua = 0
for qnode in qg.inputQuanta:
    quantum = qnode.quantum
    iqua = iqua + 1
    did = quantum.dataId
    for qdrefs in quantum.inputs.values():
        if quit: break
        for dref in qdrefs:
            if dref in drefs:
                idup = idup + 1
                continue
            ifil = ifil + 1
            drefs.append(dref)
            dtyp = dref.datasetType.name
            if len(keeptypes):
                if dtyp not in keeptypes: continue
            dfbutler = modbutler.Butler(repo, collections=dref.run)
            dfuri = dfbutler.getURI(dref).geturl()
            if dfuri[0:7] != 'file://':
                print(f"{myname}: WARNING: Dataset type {dtyp} URI is not a file: {furi}")
            dfnam = dfuri[7:]
            if dtyp == 'raw':
                irawfil = irawfil + 1
                rawfil.write(f"{dfnam}\n")
                print(f"{iqua:>8}: Recording {dtyp} {dref}")
                continue
            if xfer:
                outbutler = modbutler.Butler(outrepo, run=dref.run)
                if outbutler.stored(dref):
                    print(f"{myname}:  Skipping {dref}")
                    continue
                print(f"{iqua:>8}: Inserting {dref}")
                dfbutler = modbutler.Butler(repo, collections=dref.run)
                outref = outbutler.registry.insertDatasets(dref.datasetType, [dref.dataId], dref.run)[0]
                dst = butler.get(dref)
                outbutler.put(dst, outref)
                print(f"         {outbutler.getURI(outref).geturl()[7:]}")
    if maxqua > 0 and iqua >= maxqua:
         print(f"{myname}: Quantum limit {maxqua} has been reached.")
         break

print(f"{myname}: Processed {iqua} quanta with")
print(f"{myname}: {ifil:>8} datasets")
print(f"{myname}: {irawfil:>8} raw datasets")
print(f"{myname}: {idup:>8} duplicate datasets")

print('Bye')
