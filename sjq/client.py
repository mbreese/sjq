import os
import sys
import base64
import socket
import sjq.config
import sjq.support

config = sjq.config.load_config()


class SJQClient(object):
    def __init__(self, verbose=False):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(config['sjq.socket'])
        self.sock.settimeout(30.0)

        self.verbose = verbose
        self._closed = False

    def recvbytes(self, size):
        buf = ''
        while len(buf) < size:
            buf += self.sock.recv(size - len(buf))

        return buf

    def sendrecv(self, msg):
        if self.verbose:
            sys.stderr.write(">>> %s\n" % msg)

        # send data
        self.sock.sendall("%s\r\n" % msg)

        # Receive response
        line = sjq.support.readline(self.sock)
        resp = line
        while line and line[:2] != "OK" and line[:5] != "ERROR":
            line = sjq.support.readline(self.sock)
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

    def ping(self, jobid=None):
        return self.sendrecv("PING")

    def status(self, jobid=None):
        if jobid:
            line = self.sendrecv("STATUS %s" % jobid)
        else:
            line = self.sendrecv("STATUS")

        if line[:2] == 'OK':
            size = int(line.split(' ', 1)[1])
            return self.recvbytes(size)

        return line
 
    def release(self, *jobids):
        for jobid in jobids:
            return self.sendrecv("RELEASE %s" % jobid)

    def kill(self, *jobids):
        for jobid in jobids:
            return self.sendrecv("KILL %s" % jobid)

    def submit(self, src, procs=None, mem=None, stderr=None, stdout=None, env=False, cwd=None, name=None, uid=None, gid=None, depends=None, hold=False):
        if env:
            envvals = []
            for k in os.environ:
                envvals.append('%s=%s' % (k, base64.b64encode(os.environ[k])))
            jobenv = ';'.join(envvals)
        else:
            jobenv = None

        if not cwd:
            cwd = os.path.abspath(os.getcwd())

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
        if depends:
            self.sock.sendall("DEPENDS %s\r\n" % depends)
        if hold:
            self.sock.sendall("HOLD\r\n")

        self.sock.sendall("SRC %s\r\n" % len(src))
        self.sock.sendall(src)

        # Receive data from the server
        return sjq.support.readline(self.sock)
