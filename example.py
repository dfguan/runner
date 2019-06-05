from runner.hpc import hpc
from runner.manager import manager
from multiprocessing import Process

def func2(man):
    procs = []
    print ("func2 running")
    p = hpc("lsf", cmd="./test 1400000000", mem=1000, out="test_c2.o", err="test_c2.e" )
    procs.append(p)
    man.start(procs)
def func1(man):
    procs = []
    print ("func1 running")
    p = hpc("lsf", cmd="./test 130000", mem=1000, out="test_c1.o", err="test_c1.e")
    procs.append(p)
    man.start(procs)


if __name__ == '__main__':
    m = manager(wait=10,retries=5)  
    procs = []
    # p = hpc(cmd="ls -l > files")
    # p = hpc(cmd=["ls", '-l'], out="files")
    p = hpc("LSF", cmd="ls -l > fls", cpu="avx", hosts="bc-1-01-3", mem=1000, out="test_1.o", err="test_1.e")
    # p.chgq(m.sys[p.platform]["queues"])
    p.speak()
    procs.append(p)
    m.start(procs)
    # procs2 = []
    # p = Process(target=func2, args=(m,))
    # procs2.append(p)
    # p = Process(target=func1, args=(m,))
    # procs2.append(p)
    # for p in procs2:
        # p.start()
    # for p in procs2:
        # p.join()
