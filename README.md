ryanm_hackday_2013
==================

1. Aim

Use Disco Project (http://discoproject.org) to setup a local MapReduce "cluster" (1 master, 1 worker), load some VC OBS data, use MapReduce to do some stats


2. Tools
 
Look at http://discoproject.org/doc/disco/start/install.html#long-version for instructions to setup. Note that the download link has no downloads, you will need to get it from git:

git clone git://github.com/discoproject/disco.git

Notes on installation:
- It's not mentioned in the docs, but you need to set the enviornment variable "DISCO_HOME" to point to the base directory of where you cloned the repo above to.
- Make sure you follow all the instructions wrt setting up ssh keys, etc. I only did it on localhost, and never tried between machines.
- In the checked out repo, you also need to install the python library using the command
    python setup.py install
    in the lib direcory.

Python
