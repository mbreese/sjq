import os
import sys
import time
import atexit
import select
import signal
import socket
import datetime
import tempfile
import threading
import subprocess
import SocketServer

import sjq.client
import sjq.config
import handler
import jobqueue


def start(verbose=False, args=None, daemon=False):
    srv = SJQServer(verbose, args)
    socketfile = os.path.abspath(os.path.expanduser(srv.config['sjq.socket']))

    if os.path.exists(socketfile):
        try:
            client = sjq.client.SJQClient()
            resp = client.ping()
            if resp and resp[:2] == 'OK':
                if verbose:
                    sys.stderr.write("SJQ server already running!\n")
                return
        except:
            pass

        os.unlink(socketfile)

    pidfile = None
    if 'sjq.pidfile' in srv.config and srv.config['sjq.pidfile']:
        pidfile = os.path.abspath(os.path.expanduser(srv.config['sjq.pidfile']))

        if os.path.exists(pidfile):
            pid = -1
            with open(pidfile) as f:
                try:
                    pid = int(f.read().strip())
                except:
                    pid = -1

            if pid > 0:
                try:
                    os.kill(pid, signal.SIGKILL)
                except:
                    pass

            os.unlink(pidfile)

    if srv.config['sjq.daemon'] or daemon:
        stderr = None
        if srv.config['sjq.logfile']:
            stderr = os.path.abspath(os.path.expanduser(srv.config['sjq.logfile']))
        daemonize(stderr=stderr, pidfile=pidfile)

    srv.start()


class SJQServer(object):
    def __init__(self, verbose=False, args=None):
        self.config = sjq.config.load_config(args)

        self.job_queue = jobqueue.JobQueue(self.config['sjq.db'])

        self._server = None
        self._is_shutdown = False
        self.verbose = verbose

        self.cond = threading.Condition()
        self.lock = threading.Lock()

        self.procs_avail = self.config['sjq.maxprocs']
        self.mem_avail = self.config['sjq.maxmem']
        self.sched_time = self.config['sjq.schedtime']
        self.running_jobs = {}

    def debug(self, msg):
        self.log(msg, True)

    def log(self, msg, debug=False):
        if not debug or self.verbose:
            sys.stderr.write('%s %s\n' % (datetime.datetime.now(), msg))

    def sched(self):
        missing_job_waiting = None

        while not self._is_shutdown:
            self.lock.acquire()
            self.debug('---------------------------------')
            self.debug("PROC AVAIL: %s" % self.procs_avail)
            self.debug("MEM AVAIL: %s" % (self.mem_avail if self.mem_avail else "*"))

            donelist = []
            for jobid in self.running_jobs:
                proc, job = self.running_jobs[jobid]
                retcode = proc.poll()
                if retcode is not None:
                    self.log("JOB: %s PID: %s DONE (%s)" % (jobid, proc.pid, retcode))
                    if retcode == 0:
                        self.job_queue.update_job_state(jobid, 'S', retcode)
                    else:
                        self.job_queue.update_job_state(jobid, 'F', retcode)
                        self.job_queue.abort_deps(jobid)

                    donelist.append(jobid)
                else:
                    self.debug("JOB: %s PID: %s RUNNING" % (jobid, proc.pid))

            self.lock.release()
            for jobid in donelist:
                self.release_running_job(jobid)

            self.lock.acquire()
            self.debug('Checking held jobs...')
            self.job_queue.check_held_jobs()

            self.debug(self.queue_stats())

            self.debug('Looking for jobs...')
            job = self.job_queue.findjob(self.procs_avail, self.mem_avail)
            if job:
                proc = self.spawn_job(job)
                if proc:
                    self.log("JOB: %s PID: %s STARTED" % (job['jobid'], proc.pid))
                    self.job_queue.update_job_state(job['jobid'], 'R')
                    self.procs_avail -= job['procs']
                    if self.mem_avail is not None:
                        self.mem_avail -= job['mem']
                    self.running_jobs[job['jobid']] = (proc, job)
                    self.lock.release()
                    continue

            self.job_queue.close()  # close the connection for the thread, if opened

            if self.config['sjq.autoshutdown']:
                if len(self.running_jobs) == 0:
                    if not missing_job_waiting:
                        missing_job_waiting = time.time()
                    else:
                        now = time.time()
                        if now - missing_job_waiting > self.config['sjq.waittime']:
                            self.log("No jobs to run, shutting down!")
                            self.shutdown()
                else:
                    missing_job_waiting = None

            self.lock.release()

            self.cond.acquire()

            # we don't really need to cycle every [sched_time] sec, but
            # just in case the notifications are missed, it's nice to verify
            self.cond.wait(self.sched_time)
            self.cond.release()

        for jobid in list(self.running_jobs.keys()):
            self.kill_job(jobid)

    def queue_stats(self):
        states = sorted(self.job_queue.jobstates())
        return ' '.join([':'.join([str(y) for y in x]) for x in states])

    def kill_job(self, jobid):
        if jobid in self.running_jobs:
            proc, job = self.running_jobs[jobid]
            self.log("KILLING JOB: %s PID: %s" % (job['jobid'], proc.pid))
            # The forked process is the head of a program group.
            # Since we could have spawned other procs from here, 
            # kill the entire group
            os.killpg(proc.pid, signal.SIGKILL)
            self.release_running_job(jobid)

        self.job_queue.update_job_state(job['jobid'], 'K')
        self.cond.acquire()
        self.cond.notify()
        self.cond.release()

    def release_job(self, jobid):
        self.job_queue.update_job_state(jobid, 'H')
        self.cond.acquire()
        self.cond.notify()
        self.cond.release()

    def release_running_job(self, jobid):
        self.lock.acquire()
        if jobid in self.running_jobs:
            job = self.running_jobs[jobid][1]

            self.procs_avail += job['procs']
            if self.mem_avail is not None:
                self.mem_avail += job['mem']

            del self.running_jobs[jobid]
        self.lock.release()

    def spawn_job(self, job):
        cmd = None
        for line in [x.strip() for x in job['src'].split('\n')]:
            if line[:2] == '#!':
                cmd = line[2:]
            break

        if not cmd:
            self.log("Don't know how to run job: %s\n" % line)
        else:
            if not 'cwd' in job or not job['cwd']:
                cwd = os.path.expanduser("~")
            else:
                cwd = job['cwd']

            if job['stdout']:
                if job['stdout'][0] == '/':
                    stdout = open(job['stdout'], 'w')
                else:
                    stdout = open(os.path.join(job['cwd'], job['stdout']), 'w')
            else:
                stdout = open(os.path.join(job['cwd'], '%s.o%s' % (job['name'], job['jobid'])), 'w')

            if job['stderr']:
                if job['stderr'][0] == '/':
                    stderr = open(job['stderr'], 'w')
                else:
                    stderr = open(os.path.join(job['cwd'], job['stderr']), 'w')
            else:
                stderr = open(os.path.join(job['cwd'], '%s.e%s' % (job['name'], job['jobid'])), 'w')

            env = None

            env = {}
            if 'env' in job and job['env']:
                for pair in sjq.support.escaped_split(job['env'], ';'):
                    k,v = pair.split('=',1)
                    env[k]=v

            env['JOB_ID'] = str(job['jobid'])

            if not 'uid' in job:
                job['uid'] = None
            if not 'gid' in job:
                job['gid'] = None

            stdin = tempfile.TemporaryFile()
            stdin.write(job['src'])
            stdin.seek(0)

            if os.getuid() == 0:
                def preexec_fn():
                    demote(job['uid'], job['gid'])
                    os.setpgrp()
            else:
                def preexec_fn():
                    os.setpgrp()

            proc = subprocess.Popen([cmd], stdin=stdin, stdout=stdout, stderr=stderr, cwd=cwd, env=env, preexec_fn=preexec_fn)
            # start a thread to wait for the proc to be done
            t = threading.Thread(None, self.wait, None, (proc,))
            t.daemon=True
            t.start()
            return proc

        return None

    def wait(self, proc):
        # wait for the proc to be done, then notify the queue
        proc.wait()
        self.cond.acquire()
        self.cond.notify()
        self.cond.release()

    def start(self):
        if self._is_shutdown:
            self.log("SJQ server already shutdown!")
            return

        if os.path.exists(self.config['sjq.socket']):
            self.log("Socket path: %s exists!" % self.config['sjq.socket'])
            return

        if not self._server:
            self.log("Starting job scheduler")
            t = threading.Thread(target=self.sched, args = ())
            t.daemon = True
            t.start()

            self._server = ThreadedUnixServer(self.config['sjq.socket'], handler.SJQHandler)
            self._server.socket.settimeout(30.0)
            self._server.sjq = self
            self.log("Listening for job requests...")
            try:
                self._server.serve_forever()
            except socket.error: 
                pass
            except select.error:
                pass
            except KeyboardInterrupt:
                pass

            self.__shutdown()
            t.join()

    def shutdown(self):
        # if you don't do this, then we'll deadlock
        self._server.socket.close()

    def __shutdown(self):
        if self._server:
            # self.lock.acquire()

            self.log("Shutting down...")
            try:
                self._server.shutdown()
            except:
                pass

            self.log("Removing socket...")
            try:
                os.unlink(self.config['sjq.socket'])
            except:
                pass

            self._server = None
            self._is_shutdown = True

            self.cond.acquire()
            self.cond.notify()
            self.cond.release()
            # self.lock.release()

    def submit_job(self, src, procs=None, mem=None, **args):
        if procs == None:
            procs = self.config['sjq.defaults.procs']
        
        if mem == None:
            mem = self.config['sjq.defaults.mem']
        else:
            mem = sjq.convert_mem_val(mem)

        if procs > self.config['sjq.maxprocs']:
            return None

        if self.config['sjq.maxmem'] and mem > self.config['sjq.maxmem']:
            return None

        args['procs'] = procs
        args['mem'] = mem
        args['src'] = src

        jobid = self.job_queue.submit(args)
        self.job_queue.close()

        self.log("New job: %s" % jobid)

        self.cond.acquire()
        self.cond.notify()
        self.cond.release()
        return jobid


def demote(uid, gid):
    def wrap():
        if gid is not None:
            os.setgid(gid)
        if uid is not None:
            os.setuid(uid)
    return wrap

class ThreadedUnixServer(SocketServer.ThreadingMixIn, SocketServer.UnixStreamServer):
    pass



def daemonize(stdin='/dev/null', stdout='/dev/null', stderr=None, pidfile=None):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16

        based on code from: http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
        """

        if pidfile and os.path.exists(pidfile):
            sys.stderr.write("pidfile: %s already exists!\n" % pidfile)
            sys.exit(1)

        if stderr is None:
           stderr = '/dev/null'

        try:
                pid = os.fork()
                if pid > 0:
                        # exit first parent
                        sys.exit(0)
        except OSError, e:
                sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
                sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
                pid = os.fork()
                if pid > 0:
                        # exit from second parent
                        sys.exit(0)
        except OSError, e:
                sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
                sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(stdin, 'r')
        so = file(stdout, 'a+')
        se = file(stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        if pidfile:
            def delpid():
                os.unlink(pidfile)
            atexit.register(delpid)
            pid = str(os.getpid())
            f = open(pidfile,'w+')
            f.write("%s\n" % pid)
            f.close()
