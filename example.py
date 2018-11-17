from hpc import hpc
from manager import manager
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
    m = manager("./sys.config", retries=5)  
    procs = []
    # p = hpc(cmd="ls -l > files")
    # p = hpc(cmd=["ls", '-l'], out="files")
    p = hpc("lsf", cmd="ls -l > fls", mem=1000, out="test_1.o", err="test_1.e")
    # p.speak()
    procs.append(p)
    m.start(procs, True)
    # procs2 = []
    # p = Process(target=func2, args=(m,))
    # procs2.append(p)
    # p = Process(target=func1, args=(m,))
    # procs2.append(p)
    # for p in procs2:
        # p.start()
    # for p in procs2:
        # p.join()
