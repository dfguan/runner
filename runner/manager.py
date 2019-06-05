#manage hpc jobs
import sys, json, os, hashlib, time
from datetime import datetime
from collections import OrderedDict

#Q1: not sure if mananger can be run parallelly
#Q2: access hpc members directly, is it good?
class manager:
    def __init__(self, conf="magic", wait=105, **kwargs):
        self.id = "I am the manager"
        self.retries = 0
        self.wait = wait
        if conf == "magic":
            conf = os.path.join(os.path.dirname(__file__), "sys.config")
        with open(conf, "r") as f:
            self.sys = json.load(f, object_pairs_hook=OrderedDict)
        f.close()
        # print (self.sys)
        if "retries" in kwargs:
            self.retries = kwargs["retries"]

    def start(self, jobq, force=False, skip=False):
        if skip:
            return 0
        rjobq = []
        for j in jobq:
            j.set_retries(self.retries)
            if force or not self.check_job(j): #force or not run sucessfully before
                if force:
                    self.rm_tag(j)
                rjobq.append(j)
                if j.run():
                    j.set_retries(0)
                    j.set_rtn(1)
        while True:
            for j in rjobq: # can be problem if job has been finished?
                j.check_status()
                # j.wait() # or to keep the return values and check next?
                if j.rtn is not None: # better not access j.rtn directly ?  
                    if j.rtn:
                        print ("command {0} failed, return code: {1}".format(j.cmd if type(j.cmd) == str else " ".join(j.cmd), str(j.rtn)))
                        if j.retries:
                            tp = self.check_errt(j)
                            if tp == 1: # these codes are related to your system, need a config file
                                print ("extend memory and try again")
                                j.ext_mem()
                                if j.adjq(self.sys[j.platform]["queues"]):
                                    j.run()    
                                    j.decre_retries()
                                else:
                                    print ("fail to find a suitable queue")
                                    j.reset_retries()
                            elif tp == 2: # these codes are related to your system, need a config file
                                print ("change job queue and try again")
                                if j.chgq(self.sys[j.platform]["queues"]):
                                    j.run()
                                    j.decre_retries()
                                else:
                                    print ("fail to find a suitable queue")
                                    j.reset_retries()
                            else:
                                print ("unkown error, please check error log")
                                j.reset_retries()
                    elif not j.suc:
                        print ("command {0} run successfully".format(j.cmd if type(j.cmd) == str else " ".join(j.cmd), str(j.rtn)))
                        self.tag_job(j)
                        j.set_suc() # in case of tag again, better way?
                        j.reset_retries()
            if all(j.retries == 0 and j.rtn is not None for j in rjobq):
                break
            time.sleep(self.wait)    
        suc = all(j.rtn == 0 for j in rjobq)
        # for j in rjobq:
            # print ("command {0} return code: {1}".format(j.cmd if type(j.cmd) == str else " ".join(j.cmd), str(j.rtn)))
        if suc:
            return 0
        else:
            return 1
        # for j in rjobq:
            # if j.rtn:
                # return j.rtn
        # return 0
        
        # return artn

    
    def check_errt(self, j):
        rtn = j.rtn
        if j.platform in self.sys:
            if self.sys[j.platform]["errors"]["MEM"] == rtn:
                return 1 #mem error
            elif self.sys[j.platform]["errors"]["RUNTIME"] == rtn:
                return 2 # runtime error
            else:
                return 3 # unkown maybe will set later
        else:
            return 4 # unkown



    
    def rm_tag(self, j):
        #tag a job if run sucessfully
        cmd = j.cmd if type(j.cmd) == str else " ".join(j.cmd)
        hashmark =hashlib.sha256(cmd.encode('utf-8')).hexdigest()
        
        dirn = os.path.dirname(j.out)
        if not dirn:
            dirn = '.'
        tag_fn = '{0}/.{1}.done'.format(dirn, hashmark) 
        # print ("{} tag ".format(cmd))
        if os.path.isfile(tag_fn):
            os.remove(tag_fn) # what if failed to remove file?
        # open(tag_fn,'w').close()

    def tag_job(self, j):
        #tag a job if run sucessfully
        cmd = j.cmd if type(j.cmd) == str else " ".join(j.cmd)
        hashmark =hashlib.sha256(cmd.encode('utf-8')).hexdigest()
        
        dirn = os.path.dirname(j.out)
        if not dirn:
            dirn = '.'
        tag_fn = '{0}/.{1}.done'.format(dirn, hashmark) 
        # print ("{} tag ".format(cmd))
        with open(tag_fn,'w') as f:
            f.write("CMD : {0}\nTIME: {1}\n".format(cmd, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            f.close() 

    def check_job(self, j):
        # check if a job has run before 
        cmd = j.cmd if type(j.cmd) == str else " ".join(j.cmd)
        hashmark =hashlib.sha256(cmd.encode('utf-8')).hexdigest()
        
        dirn = os.path.dirname(j.out)
        if not dirn:
            dirn = '.'
        tag_fn = '{0}/.{1}.done'.format(dirn, hashmark) 
        if os.path.isfile(tag_fn):
            print ("{} ran before, skip".format(cmd))
            return True 
        else:
            return False 


# Notes on LSF Platform
# only take two types of error
#yesterday	High priority queue (as in "I needed this run yesterday"). You can only run 10 jobs at a time in this queue.
# small	Queue for short running jobs (< 1 minute). Has job-chunking enabled (see the LSF docs). Such short jobs are generally a bad idea; try to make your jobs run for at least 10 minutes, if possible, and use other queues.
# normal	The default queue. For jobs running for up to an hour each. There is a hard limit of 12 hours, after which your job will be killed.
# hugemem	Queue for jobs which require > 196 GB of memory. You must specify how much memory you expect your job to use.
# teramem	Queue for jobs which require > 745 GB of memory. You must specify how much memory you expect your job to use.
# long	Queue for long running jobs (> 1 hour). There is a hard limit of 48 hours, after which the job will be killed. [1, 48] 
# basement	Queue for jobs which run for more than a day. You should consider checkpointing long running jobs [0,24]
#Parallel Queue for multi-node, multi-cpu jobs (ie PVM/MPI jobs, not threaded jobs).
