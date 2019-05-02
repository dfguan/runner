# runner
A simple python wrapper for HPC job submission, now supports LSF and Slurm. 

# Installation
```
python setup.py install 
```
Notice: this will install the package to a system python path, if you don't have admistrative privilege, try using "python setup.py install --prefix=LOCAL\_PATH" or simply run "python setup.py install --user".

## Done
1. support multiple HPC platforms, including Slurm, LSF etc. (Done, tested on LSF, Slurm). 
2. skip jobs that has been run successfuly before (Done, tested on LSF).
3. resubmit jobs that fails (Done, tested on LSF).
4. force functions/jobs to run (Done, tested on LSF)
5. use process watchers instead of wait func (Done, tested on LSF)

## To Do
- [ ] allow to select CPUs, HOSTs, etc..
- [ ] draft wiki pages

