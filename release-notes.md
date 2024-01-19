# Release notes for desc-gen3-prod
The sections below are labeled I.J.K (I, J, K integers) for a commit of that name without a tag.
If a tag is made, it and the section are named vI.J.K.
Development tags leading up to commit I.J.K are typically named I.J.K.dev1, I.J.K.dev2, ....

Users who want some stability and validation are advised to use the most recent tag which does not have disqualifying comments.
Those that want to keep up with the very latest developments can use the the main branch (also the latest commit).

## v0.0.50
January 2024, [issue 5](/../../issues/5)  
Fixes _g3wfpipe_ notebook installation broken in 0.0.49.

## 0.0.49
January 2024, [issue 5](/../../issues/5)  
Re-enables the use for WorkQueue in _g3wfpipe_. This was broken in a recent commit.

## 0.0.48
January 2024, [issue 5](/../../issues/5)  
This commit along with version 17.16 of the _monexp_ notebook adds the descprod configuration to the notebook plot titles.

## 0.0.47
December 2023, [issue 5](/../../issues/5)  
More _g3wfpipe_ task wrapper and runapp options are added with much of the code moved to the wrapper.
The _strace_ file activity summary _iotrace_ is added to to the wrapper and runapp

To provide the best performance, all wrapper add-ons except _time_ are now disabled by default.
Howfig options van be used 
