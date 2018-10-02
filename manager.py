#manage hpc jobs

class manager:
    def __init__(self, **kwargs)
        self.id = "I am the manager"
        self.retries = 1
        if "retries" in kwargs:
            set_retries(kwargs["retries"])


    def cont(self, jobq):
        for j in jobq:
            j.set_retries(self.retries)
            j.run()
        for j in jobq:
            if j.wait():
                if j.retries: 
                    rtn = diagnose_failure(j)
                    if rtn == 1: # mem error
                        j.ext_mem() #extend mem, may need to change queueif too large
                    elif rtn == 2: # time is not enough have no idea may be we need a config file of queues and its mem and time limits
                        j.

    def dgn_fail(self, j)
         
            

    def set_retries(self, times):
        self.retries = times
