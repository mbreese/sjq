import os
import socket
import sjq.config

config = sjq.config.load_config()

class SQJClient(object):
    def __init__(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(config['sjq.socket'])

    def sendrecv(self, msg):
        # Connect to server and send data
        self.sock.sendall("%s\r\n" % msg)

        # Receive data from the server and shut down
        buf = self.sock.recv(1024)
        result = buf
        while len(buf) > 0:
            buf = self.sock.recv(1024)
            result += buf

        return result.strip()

    def close(self):
        self.sendrecv("EXIT")

    def shutdown(self):
        return self.sendrecv("SHUTDOWN")


    def status(self, jobid):
        return self.sendrecv("STATUS %s" % jobid)

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