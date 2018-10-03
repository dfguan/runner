from hpc import hpc
from manager import manager

if __name__ == '__main__':
    m = manager("./sys.config", retries=1)  
    procs = []
    # p = hpc(cmd="ls -l > files")
    p = hpc(cmd=["ls", '-l'], out="files")
    # p.speak()
    procs.append(p)
    m.start(procs)
