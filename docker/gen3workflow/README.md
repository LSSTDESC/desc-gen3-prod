# images/descprod

This directory contains scripts and configs (dockerfiles) to 
create and run docker images for running DESC pipeline jobs.

The user must first install the docker desktop--see https://docs.docker.com/get-docker.

Below LVER is an LSST version tag (e.g. w\_2023\_15) and XX represents a version tag.
Replace them with your values.

## Creating and using version LVER-XX

First create ./dockerfile-XX most likely by copying one of the
dockerfiles here and making modifications.

Build the image with
<pre>
> ./build LVER XX
</pre>

Test the image by running locally.
<pre>
> ./start LVER XX
lksst:LVER> com1
lksst:LVER> com2
descprod> exit
>
</pre>

Push the image to dockerhub.
<pre>
./push LVER XX
</pre>

Pull the image on another machine.
Replace dladams with the docker username of the installer.
<pre>
> docker pull dladams/gen3workflow-LVER:XX
</pre>
Test as above.

On perlmutter or cori, install with shifter:
<pre>
> shifterimg -v pull dladams/gen3workflow-LVER:XX
</pre>

## Version history 
00 - LSST installation on Centos 7 plus vim.  
01 - Preceding plus the gen3 pip installs for LSST.  
02 - Preceding plus conda install of ndcctools 7.3.5  
03 - Same as preceding with mamba conda solver  
04 - Same as 02 except thread protections are added to startup.  
06 - Working?
07 - Working?
09 - Failed attempt to add parsl build with dependencies.
00 - Another failed attempt.
09 - Another failed attempt.
11 - Another failed attempt.
12 - Switch to Jim's RX to for w2332 via g3wf-install-parsl-w2332: fails with missing IPython
13 - Try to fix 12 but the new parsl is not picked up?
14 - Start from 09 and add workqueue
