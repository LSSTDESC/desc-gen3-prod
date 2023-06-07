# Application g3wfpipe
The application g3wfpipe carries out LSST pipeline processing.
There are three main steps in typical processing:
* An LSST processing DAG (aka quantum graph or QG) is constructed from a user-supplied data query and
specification of the pipeline process steps to carry out.
* The DAG processing is carried out with parsl.
* The output data is registered with the Butler (finalization).

The first and can take a while but runs in a single processs and so are appropriate for a login or workflow machine.
The second typically uses a lot of CPU and memory and can be parallelized and so is usually run in batch.
The primary step flags, *init*, *proc* and *finalize*, indicate which of these steps is
to be carried out.
There are also secondary step flags, *qgre* to check the QG and *status* to repoert on the processing status.

The application can be invoked with the command
```
runapp-g3wfquery CFG HFG
```
or submitted using appname g3wfpipe with CFG and HFG in the [DESCprod server](https://www.descprod.org/home).
The first argument, the config string, specifies what action is to be taken and the second, the howfig string, provides instruction or hints on  how to carrye it out.
Each of these is a dash-delimited sequence of flags: FLG1-FLG2-...-FLGn specifying the properties as described below.

The config fields are

| Field | Meaning | Example |
|---|---|---|
| wYYWW | Indicates which LSST weekly release w_20YY_WW should be used. | w2318 |
| visit:VV | Query for visit VV only. | visit:231 |
| visit:VV1:VV2 | Query for all visits in the range [vv1, VV2]. | visit:10001:20000 |
| job:JJJ | Continue the processing for job JJJ. | job:12345 |
| pipe:TT1:... | List of task names or groups to run. | pipe:isr |
| init | Generate DAG from query and tasks. | init |
| proc | Process the DAG. | proc |
| finalize | Register output data. | finalize |
| qgre | Check the QG and display its properties. | qgre |
| status | Report the status of processing, e.g. what fraction is complete. | status |

The howfig fields are

| Field | Meaning | Example |
|---|---|---|
| cvmfs | Use the cvmfs installtion of the LSST software. | cvmfs |
| shifter | use the shifter installation of the software. | shifter |
| tpNN | Parsl ThreadPool executor with NN concurrent processes. | tp:100 |
| wqMM | Parsl WorQueue executor with totl memeory MM GB. | wq128 |

For cvmfs, gen3-workflow and its required extra products are automatically installed in the user's home area at $HOME/gen3workflow/lsst_distrib on top of the specified LSST release.
This can lead to some delay the first time a release is used and there will likely be problems if another jobs is started during installation.

For shifter, a docker image must be created in advance and installed in the user's shifter image area.
Instructions and code for this may be found at [xx](../docker).