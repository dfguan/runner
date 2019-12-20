from subprocess import Popen, PIPE

class hpc:
    def __init__(self, pf="bash", **kwargs):
        #set default values
        self.platform = pf.upper()
        self.retries = 0
        self.rtn = 1 
        self.suc = 0
         
        if "cmd" in kwargs:
            self.cmd = kwargs["cmd"]
        else:
            self.cmd = ""
        
        if "mem" in kwargs:
            self.mem = kwargs["mem"]
        else:
            self.mem = 1000
        
        if "time" in kwargs:
            self.time = kwargs["time"]
        else:
            self.time = "05:00:00"
        
        if "cpu" in kwargs:
            self.cpu = kwargs["cpu"]
        else:
            self.cpu = ""

        if "cpuf" in kwargs:
            self.cpuf = kwargs["cpu"]
        else:
            self.cpuf = 0
        
        if "hosts" in kwargs:
            self.hosts = kwargs["hosts"]
        else:
            self.hosts = ""
        
        if "ntasks" in kwargs:
            self.ntasks = kwargs["nt"]
        else:
            self.ntasks = 1 

        if "array" in kwargs:
            self.array = kwargs["nt"]
        else:
            self.array = 1 

        if "core" in kwargs:
            self.core = kwargs["core"]
        else:
            self.core = 1
       
        if "jn" in kwargs:
            self.jn = kwargs["jn"]
        else:
            self.jn = "job"
        
        if "err" in kwargs:
            self.err = kwargs["err"]
        else:
            self.err = "job.e" 
        
        if "out" in kwargs:
            self.out = kwargs["out"]
        else:
            self.out = "job.o"
        
        if "queue" in kwargs:
            self.queue = kwargs["queue"]
        else:
            self.queue = "normal"
        


    def run(self):
        if len(self.cmd) == 0 :  
            print ("command not found!")
            return 1
        if self.platform == 'BASH':
            sub_cmd = ['bash', '-c']
            cmd_str = self.cmd if type(self.cmd) == str else ' '.join(self.cmd)
            sub_cmd.append(cmd_str) 
        elif self.platform == 'LSF':
            # self.sub_cmd = 'bsub -K -q{6} -M{0} -n{1} -R"select[mem>{0}] rusage[mem={0}] span[hosts=1]" -J{2} -o {3} -e {4} {5}'.format(str(self.mem), str(self.core), self.jn, self.out, self.err, self.cmd, self.queue)
            if self.cpu != "":
                cpu_sel = '-R{}'.format(self.cpu)
            else:
                cpu_sel = ''
            
            if self.cpuf != 0:
                cpuf_sel = '-R"select[cpuf=='+str(self.cpuf)+']"'
            else:
                cpuf_sel = ''

            if self.hosts != "":
                hosts_sel = "-Rselect[(" + "||".join([ "hname==" + "'{}'".format(s)  for s in self.hosts.split("||")]) + ")]"  # use double quote here maybe inconsisten with the others
            else:
                hosts_sel = ""
            sub_cmd = ['bsub', '-K', '-q', self.queue, '-M', str(self.mem), cpu_sel, cpuf_sel, hosts_sel, '-n', str(self.core), '-R"select[mem>'+str(self.mem)+'] rusage[mem='+str(self.mem)+'] span[hosts=1]"', '-J', self.jn,  '-o', self.out, '-e', self.err, self.cmd]
        elif self.platform == 'SLURM':
            sub_cmd = ['sbatch', '-W', '-t', self.time, '--mem', str(self.mem), '-n', str(self.ntasks), '-c', str(self.core), '--array', str(self.array), '-J', self.jn, '-o', self.out, '-e', self.err, '--wrap', self.cmd]
        elif self.platform == 'MPM':
            print ("not done yet")
            return 1
        else:
            print ("{} is not supported".format(self.platform))
            return 1

        self.sub_cmd = [ x for x in sub_cmd if x]

        try:
            self.rtn = None # set exit code to None to fix the last successful try bug   
            if self.platform == "BASH":
                self.fout = open(self.out, 'w')
                self.ferr = open(self.err, 'w')
                self.p = Popen(self.sub_cmd, stdout=self.fout, stderr=self.ferr)
            elif self.platform == "LSF" or self.platform == "SLURM":
                self.p = Popen(self.sub_cmd)
                # print("the commandline is {}".format(self.p.args))
            else:
                return 1
        except:
            print ("File not found CMD: {}".format(self.sub_cmd))
            return 1
        return 0
        # except :
    def kill(self):
        if hasattr(self, 'p'):
            self.p.kill()

    def wait(self):
        if hasattr(self, 'p'):
            self.rtn = self.p.wait()
            if self.platform == "BASH":
                self.fout.close()
                self.ferr.close()
        # return exit_code 
    
    def speak(self):
        if hasattr(self, 'sub_cmd'):
            print (self.sub_cmd)
        else:
            print (self.cmd)

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

    # def set_job(self, cmd, **kwargs):
        # self.cmd = cmd
        # if "jn" in kwargs:
            # set_jn(kwargs["jn"])
        # if "mem" in kwargs:
            # set_mem(kwargs["mem"])
        # if "queue" in kwargs:
            # set_queue(kwargs["queue"])
        # if "core" in kwargs:
            # set_core(kwargs["core"])
        # if "err" in kwargs:
            # set_err(kwargs["err"])
        # if "out" in kwargs:
            # set_out(kwargs["out"])
    def set_time(self, ntime):
        self.time = ntime

    def set_retries(self, times):
        self.retries = times
    

    def set_rtn(self, n):
        self.rtn = n
    def reset_retries(self):
        self.retries = 0
    def decre_retries(self):
        self.retries = self.retries - 1
    def set_suc(self):
        self.suc = 1

    def ext_mem(self):
        self.mem *= 2
    
    def ext_time(self, qs):
        found = False
        if self.platform == "LSF": 
            t = 2 * qs[self.queue][1]
            for q in qs:
                [lm, hm] = [int(z) for z in qs[q][0].split(" ")]
                tl = qs[q][1]
                if t < tl and self.mem < hm:
                    self.set_queue(q)
                    found = True 
                    break
        else:
            tm_lst = [2 * int(i) for i in self.time.split(':')]
            for idx, v in enumerate(tm_lst):
                if v > 99:
                    tm_lst[idx] = 99
            self.set_time("{0[0]}:{0[1]}:{0[2]}".format(tm_lst))
            found = True
        return found
    
    

    def check_status(self):
        if hasattr(self, 'p'):
            self.rtn = self.p.poll()
            if self.platform == "BASH" and self.rtn is not None:
                self.fout.close()
                self.ferr.close()
        return self.rtn
    
    def adjq(self, qs):
        found = False
        if self.platform == "LSF":
            [lm, hm] = [int(z) for z in qs[self.queue][0].split(" ")]
            found = False
            if self.mem >= hm:
                for q in qs:
                    [lm, hm] = [int(z) for z in qs[q][0].split(" ")]
                    if self.mem < hm:
                        self.set_queue(q)
                        found = True 
                        break
            else:
                found = True
        elif self.platform == "SLURM": 
            found = True
        return found 

    def chgq(self, qs):
        t = 2 * qs[self.queue][1]
        found = False
        if self.platform == "LSF":
            for q in qs:
                [lm, hm] = [int(z) for z in qs[q][0].split(" ")]
                tl = qs[q][1]
                if t < tl and self.mem < hm:
                    self.set_queue(q)
                    found = True 
                    break
        elif self.platform == "SLURM": 
            found = True
        return found
