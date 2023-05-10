# Application g3wfquery
The applicatoin g3wfquery makes a query of the DC2 data and returns the number of matching raw datasets (CCD images).
It can be invoked directly with

```
runapp-g3wfquery CFG HFG
```
or submitted using appname g3wfquery with CFG and HFG in the [DESCprod server](https://www.descprod.org/home).
The first argument, the config string, specifies what action is to be taken and the second, the howfig string, provides instruction or hints on  how to carrye it out.
Each of these is a dash-delimited sequence of fields: FLD1-FLD2-...-FLDn specifyin various properties.

The config fields are

```
|---|---|---|
| wYYWW | Indicates LSST weekly release w_20YY_WW should be used. |
| visit:VV | Query for visit VV only (visit = VV) |
| visit:VV1:VV2 | Query for all visits in the range {vv1, VV2]. |
```
