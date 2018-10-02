from subprocess import Popen, PIPE

class hpc:
    def __init__(self, pf="bash", **kwargs):
        #set default values
        self.cmd = ""
        self.mem = 1000
        self.core = 1
        self.jn = "job"
        self.err = "job.e" 
        self.out = "job.o"
        self.queue = "normal"
        self.platform = pf.upper()
        self.retries = 0
        if "cmd" in kwargs:
            set_cmd(kwargs["cmd"])
        if "mem" in kwargs:
            set_mem(kwargs["mem"])
        if "core" in kwargs:
            set_core(kwargs["core"])
        if "jn" in kwargs:
            set_jn(kwargs["jn"])
        if "err" in kwargs:
            set_err(kwargs["err"])
        if "out" in kwargs:
            set_out(kwargs["out"])
        if "queue" in kwargs:
            set_queue(kwargs["queue"])
    
    def run(self):
        if len(self.cmd) == 0:
            print ("command not found!")
            return 1

        if self.platform == 'BASH':
            self.sub_cmd = [self.cmd]
        elif self.platform == 'LSF':
            # self.sub_cmd = 'bsub -K -q{6} -M{0} -n{1} -R"select[mem>{0}] rusage[mem={0}] span[hosts=1]" -J{2} -o {3} -e {4} {5}'.format(str(self.mem), str(self.core), self.jn, self.out, self.err, self.cmd, self.queue)
            self.sub_cmd = ['bsub', '-K', '-q', self.queue, '-M', str(self.mem), '-n', str(self.core), '-R"select[mem>'+str(self.mem)+'] rusage[mem='+str(self.mem)+'] span[hosts=1]"', '-J', self.jn,  '-o', self.out, '-e', self.err, self.cmd]
        elif self.platform == 'SLURM':
            print ("not done yet")
        elif self.platform == 'MPM':
            print ("not done yet")
        # try:
        if self.platform == "BASH":
            self.fout = open(self.out, 'w')
            self.ferr = open(self.err, 'w')
            self.p = Popen(self.sub_cmd, stdout=fout, stderr=ferr)
        else:
            self.p = Popen(self.sub_cmd)
        return 0
        # except :
    def kill(self):
        self.p.kill()

    def wait(self):
        exit_code = self.p.wait()
        if self.platform == "BASH":
            self.fout.close()
            self.ferr.close()
        return self.p.wait()
    
    def speak(self):
        print (self.sub_cmd) 

    def set_cmd(self, cmd):
        self.cmd = cmd
    def set_jn(self, jn):
       self.jn = jn 
    def set_queue(self, q):
        self.queue = q
    def set_mem(self, mem):
        self.mem = mem
    def set_core(self, core):
        self.core = core
    def set_err(self, err_fl):
        self.err = err_fl
    def set_out(self, out_fl):
        self.out = out_fl
    def set_job(self, cmd, **kwargs):
        self.cmd = cmd
        if "jn" in kwargs:
            set_jn(kwargs["jn"])
        if "mem" in kwargs:
            set_mem(kwargs["mem"])
        if "queue" in kwargs:
            set_queue(kwargs["queue"])
        if "core" in kwargs:
            set_core(kwargs["core"])
        if "err" in kwargs:
            set_err(kwargs["err"])
        if "out" in kwargs:
            set_out(kwargs["out"])
    def set_retries(self, times):
        self.retries = times
