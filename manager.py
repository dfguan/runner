#manage hpc jobs
# only take two types of error
#yesterday	High priority queue (as in "I needed this run yesterday"). You can only run 10 jobs at a time in this queue.
# small	Queue for short running jobs (< 1 minute). Has job-chunking enabled (see the LSF docs). Such short jobs are generally a bad idea; try to make your jobs run for at least 10 minutes, if possible, and use other queues.
# normal	The default queue. For jobs running for up to an hour each. There is a hard limit of 12 hours, after which your job will be killed.
# hugemem	Queue for jobs which require > 196 GB of memory. You must specify how much memory you expect your job to use.
# teramem	Queue for jobs which require > 745 GB of memory. You must specify how much memory you expect your job to use.
# long	Queue for long running jobs (> 1 hour). There is a hard limit of 48 hours, after which the job will be killed. [1, 48] 
# basement	Queue for jobs which run for more than a day. You should consider checkpointing long running jobs [0,24]
#Parallel Queue for multi-node, multi-cpu jobs (ie PVM/MPI jobs, not threaded jobs).
import sys, json, os, hashlib
#Q1: not sure if mananger can be run parallelly
#Q2: access hpc members directly, is it good?
class manager:
    def __init__(self, conf, **kwargs):
        self.id = "I am the manager"
        self.retries = 0
        with open(conf, "r") as f:
            self.sys = json.load(f)
        f.close()
        print (self.sys)
        if "retries" in kwargs:
            self.retries = kwargs["retries"]

    def start(self, jobq):
        for j in jobq:
            if j.platform != "BASH":
                j.set_retries(self.retries)
            if self.check_job(j) or j.run():
                sys.exit(1)
        for j in jobq: # problem: what if job has been finished?
            rtn = j.wait()
            if rtn:
                print ("command {0} failed, return code: {1}".format(j.cmd if type(j.cmd) == str else " ".join(j.cmd), str(rtn)))
                while j.retries and rtn and j.platform in self.sys:
                    if self.sys[j.platform]["errors"][str(rtn)] == "MEM": # these codes are related to your system, need a config file
                        print ("extend memory and try again")
                        j.ext_mem()
                        if self.adj_queue(j, self.sys[j.platform]["queues"]):
                            # print ("re-run the job {}".format(j.cmd))
                            j.run()    
                            j.retries -= 1
                            rtn = j.wait()
                        else:
                            print ("fail to find a suitable queue")
                            j.retries = 0
                    elif self.sys[j.platform]["errors"][str(rtn)] == "RUNTIME": # these codes are related to your system, need a config file
                        print ("change job queue and try again")
                        if self.change_queue(j, self.sys[j.platform]["queues"]):
                            j.run()
                            j.retries -= 1
                            rtn = j.wait()
                        else:
                            print ("fail to find a suitable queue")
                            j.retries = 0
                    else:
                        print ("unkown error, please check error log")
                        j.retries = 0
                if rtn == 0:
                    self.tag_job(j)
            else:
                self.tag_job(j)
    def adj_queue(self, j, qs):
        [lm, hm] = [int(z) for z in qs[j.queue][0].split(" ")]
        found = False
        if j.mem >= hm:
            for q in qs:
                [lm, hm] = [int(z) for z in qs[q][0].split(" ")]
                if j.mem < hm:
                    j.set_queue(q)
                    found = True 
                    break
        else:
            found = True
        return found 
    
    def change_queue(self, j, qs):
        t = 2 * qs[j.queue][1]
        found = False
        for q in qs:
            [lm, hm] = [int(z) for z in qs[q][0].split(" ")]
            tl = qs[q][1]
            if t < tl and j.mem < hm:
                j.set_queue(q)
                found = True 
                break
        return found

    def tag_job(self, j):
        #tag a job if run sucessfully
        cmd = j.cmd if type(j.cmd) == str else " ".join(j.cmd)
        print (cmd)
        hashmark =hashlib.sha256(cmd.encode('utf-8')).hexdigest()
        
        dirn = os.path.dirname(j.out)
        if not dirn:
            dirn = '.'
        tag_fn = '{0}/.{1}.done'.format(dirn, hashmark) 
        # print ("{} tag ".format(cmd))
        open(tag_fn,'w').close()
    
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
