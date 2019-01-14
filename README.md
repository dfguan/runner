# runner
A simple python wrapper for job submitting

# Installation
```
python setup.py install 
```
Notice: this will install modules to system python path, try "python setup.py install --prefix=LOCAL\_PATH" if you want to install them somewhere else.

## Done
1. support multiple HPC platforms, including Slurm, LSF etc. (Done, tested on LSF, Slurm). 
2. skip jobs that has been run successfuly before (Done, tested on LSF).
3. resubmit jobs that fails (Done, tested on LSF).
4. force functions/jobs to run (Done, tested on LSF)
5. use process watchers instead of wait func (Done, tested on LSF)

## To Do
- [ ] allow to select CPUs, HOSTs, etc..
- [ ] draft wiki pages

