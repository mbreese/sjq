import os
import SocketServer


class SJQHandler(SocketServer.BaseRequestHandler):
    def readline(self):
        s = ""
        while not s or s[-1] != '\n':
            ch = self.request.recv(1)
            s += ch
        return s.rstrip()

    def handle(self):
        exit = False
        while not exit:
            line = self.readline()
            spl = line.split(' ', 1)
            action = spl[0].upper()

            if (action == 'SHUTDOWN'):
                self.shutdown()
            if (action == 'STATUS'):
                self.status(spl[1] if len(spl) > 1 else None)
            if (action == 'SUBMIT'):
                self.submit()
            if (action == 'EXIT'):
                exit=True

    def submit(self):
        args = {
            "mem": None,
            "procs": None,
            "depends": None,
            "stdout": None,
            "stderr": None,
            "env": None,
            "cwd": None,
            "name": None,
            "uid": None,
            "gid": None
        }
        errors = []
        srclen = 0

        try:
            while True:
                line = self.readline()
                if ' ' in line:
                    k,v = line.split(' ',1)
                else:
                    k = line
                    v = ''
                k = k.lower()
                if k in args:
                    if k in ["procs", "uid", "gid"]:
                        args[k] = int(v)
                    elif k in ["stdout", "stderr"]:
                        if not os.path.exists(os.path.dirname(v)):
                            errors.append("%s => %s does not exist" % (k, v))
                        else:
                            args[k] = v
                    elif k in ["cwd"]:
                        if not os.path.exists(v):
                            errors.append("%s => %s does not exist" % (k, v))
                        else:
                            args[k] = v
                    else:
                        args[k] = v
                elif k == "src":
                    srclen = int(v)
                    break

            src = self.request.recv(srclen)

            if errors:
                self.send("ERROR %s" % ('; '.join(errors)))
            else:
                jobid = self.server.sjq.submit_job(src, **args)
                if jobid:
                    self.send("OK %s" % jobid)
                else:
                    self.send("ERROR")
        except:
            self.send("ERROR")

    def status(self, jobid=None):
        if jobid:
            status = self.server.sjq.job_queue.status(jobid)
            if status:
                self.send("OK %s %s" % (jobid, status))
                return
            self.send("ERROR %s not found!" % jobid)
        else:
            self.send("ERROR Not implemented")
        

    def shutdown(self):
        self.send("OK")
        self.server.sjq.shutdown()
 

    def send(self, msg):
        self.request.sendall('%s\r\n' % msg)
        self.request.close()