import os
import sys
import socket
import sjq.config

config = sjq.config.load_config()

class SJQClient(object):
    def __init__(self, verbose=False):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(config['sjq.socket'])
        self.sock.settimeout(30.0)

        self.verbose = verbose
        self._closed = False

    def readline(self):
        s = ""
        while not s or s[-1] != '\n':
            ch = self.sock.recv(1)
            if not ch:
                break
            s += ch

        if self.verbose:
            sys.stderr.write("<<< %s\n" % (s.replace('\n', '\\n').replace('\r', '\\r')))

        return s.rstrip()

    def sendrecv(self, msg):
        if self.verbose:
            sys.stderr.write(">>> %s\n" % msg)

        # send data
        self.sock.sendall("%s\r\n" % msg)

        # Receive response
        line = self.readline()
        resp = line
        while line and line[:2] != "OK" and line[:5] != "ERROR":
            line = self.readline()
            resp += line
        return resp

    def close(self):
        if not self._closed:
            self.sendrecv("EXIT")
            self.sock.close()

    def shutdown(self):
        ret = self.sendrecv("SHUTDOWN")
        self._closed = True
        self.sock.close()
        return ret


    def status(self, jobid=None):
        if jobid:
            return self.sendrecv("STATUS %s" % jobid)
        else:
            return self.sendrecv("STATUS")


    def submit(self, src, procs=None, mem=None, stderr=None, stdout=None, env=False, cwd=None, name=None, uid=None, gid=None):
        if env:
            envvals = []
            for k in os.environ:
                envvals.append('%s=%s' % (k, os.environ[k]))
            jobenv = ','.join(envvals)
        else:
            jobenv = None

        if not cwd:
            cwd = os.getcwd()

        if uid is None:
            uid = os.getuid()

        if gid is None:
            gid = os.getgid()

        # Connect to server and send data
        self.sock.sendall("SUBMIT\r\n")
        if procs:
            self.sock.sendall("PROCS %s\r\n" % procs)
        if mem:
            self.sock.sendall("MEM %s\r\n" % mem)
        if stdout:
            self.sock.sendall("STDOUT %s\r\n" % stdout)
        if stderr:
            self.sock.sendall("STDERR %s\r\n" % stderr)
        if jobenv:
            self.sock.sendall("ENV %s\r\n" % jobenv)
        if cwd:
            self.sock.sendall("CWD %s\r\n" % cwd)
        if uid is not None:
            self.sock.sendall("UID %s\r\n" % uid)
        if gid is not None:
            self.sock.sendall("GID %s\r\n" % gid)
        if name:
            self.sock.sendall("NAME %s\r\n" % name)

        self.sock.sendall("SRC %s\r\n" % len(src))
        self.sock.sendall(src)

        # Receive data from the server
        buf = self.sock.recv(1024)
        result = buf
        while len(buf) > 0:
            buf = self.sock.recv(1024)
            result += buf

        return result.strip()