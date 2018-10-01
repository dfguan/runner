from subprocess import Popen, PIPE

class hpc:
    def __init__(self, pf="bash"):
        self.mem = 100
        self.core = 1
        self.jn = "job"
        self.err = "%J_%I.e" 
        self.out = "%J_%I.o"
        self.cmd = ""
        self.queue = "normal"
        self.platform = pf.upper()
    
    def submit(self):
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
        self.p = Popen(self.sub_cmd)
        # except :
    def kill(self):
        self.p.kill()

    def wait(self):
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


