import os
import sys
import SocketServer

import sjq.support


class SJQHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        exit = False
        while not exit:
            try:
                line = sjq.support.readline(self.request, logger=self.server.sjq.debug)
                if not line:
                    break

                spl = line.split(' ', 1)
                action = spl[0].upper()

                if (action == 'EXIT'):
                    self.send("OK Goodbye!")
                    exit=True
                elif (action == 'SHUTDOWN'):
                    self.shutdown()
                    exit=True
                elif (action == 'STATUS'):
                    self.status(spl[1] if len(spl) > 1 else None)
                elif (action == 'SUBMIT'):
                    self.submit()
                elif (action == 'KILL'):
                    jobid = None
                    try:
                        jobid = int(spl[1])
                    except:
                        jobid = None

                    if jobid:
                        self.kill(jobid)
                    else:
                        self.send("ERROR Bad jobid")

                elif (action == 'PING'):
                    self.ping()
                else:
                    self.send("ERROR Unknown command")
            except Exception, e:
                sys.stderr.write(str(e))
                self.send("ERROR Server exception")
                exit=True

        self.request.close()

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
                line = sjq.support.readline(self.request)
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
            self.server.sjq.debug("<<< <%s bytes>" % len(src))


            cmd = None
            for line in [x.strip() for x in src.split('\n')]:
                if line[:2] == '#!':
                    cmd = line[2:]
                break

            if not cmd:
                errors.append("Don't know how to run script (missing shebang)")

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

    def kill(self, jobid):
        self.server.sjq.kill_job(jobid)
        self.send("OK")
        
    def status(self, jobid=None):
        #JOBID\tJOBNAME\t[RQHSFAK]\tDEPENDS\r\n
        output=[]
        for tup in self.server.sjq.job_queue.status(jobid):
            output.append('\t'.join([str(x) for x in tup]))

        out = '\n'.join(output)
        self.send("OK %s" % len(out))
        self.request.sendall(out)

    def ping(self):
        self.send("OK %s" % self.server.sjq.queue_stats())

    def shutdown(self):
        self.send("OK Shutting down server")
        self.server.sjq.shutdown()
 

    def send(self, msg):
        self.server.sjq.debug(">>> %s" % msg)
        self.request.sendall('%s\r\n' % msg)
