# Application g3wfquery
The application g3wfquery makes a query of the DC2 data and returns the number of matching raw datasets (CCD images).
It can be invoked directly with

```
runapp-g3wfquery CFG HFG
```
or submitted using appname g3wfquery with CFG and HFG in the [DESCprod server](https://www.descprod.org/home).
The first argument, the config string, specifies what action is to be taken and the second, the howfig string, provides instruction or hints on  how to carrye it out.
Each of these is a dash-delimited sequence of fields: FLD1-FLD2-...-FLDn specifying the properties as described below.

The config fields are

| Field | Meaning | Example |
|---|---|---|
| wYYWW | Indicates which LSST weekly release w_20YY_WW should be used. | w2318 |
| visit:VV | Query for visit VV only. | visit:231 |
| visit:VV1:VV2 | Query for all visits in the range [vv1, VV2]. | visit:10001:20000 |

The howfig fields are

| Field | Meaning | Example |
|---|---|---|
| cvmfs | Use the cvmfs installtion of the LSST software. | cvmfs |
| shifter | use the shifter installation of the software. | shifter |

For cvmfs, gen3-workflow and its required extra products are automatically installed in the user's home area at $HOME/gen3workflow/lsst_distrib on top of the specified LSST release.
This can lead to some delay the first time a release is used and there will likely be problems if another jobs is started during installation.

For shifter, a docker image must be created in advance and installed in the user's shifter image area.
Instructions and code for this may be found at [xx](../docker).
