import os
import sqlite3
import datetime
import threading

class LocalConnection(threading.local):
    __slots__ = 'path'

    def __init__(self, path):
        self.path = path
        self.conn = None

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def getconn(self):
        if not self.conn:
            self.conn = sqlite3.connect(self.path)
            self.conn.row_factory = sqlite3.Row

        return self.conn

class JobQueue(object):
    def __init__(self, path):
        self.path = path
        
        if not os.path.exists(path):
            conn = sqlite3.connect(self.path)
            conn.executescript('''\
CREATE TABLE job (jobid INTEGER PRIMARY KEY ASC AUTOINCREMENT, src TEXT, state TEXT, name TEXT NOT NULL DEFAULT 'sjqjob', submitted TIMESTAMP, started TIMESTAMP, finished TIMESTAMP, procs INTEGER, mem INTEGER, stdout TEXT, stderr TEXT, env TEXT, cwd TEXT, uid INTEGER, gid INTEGER, retcode INTEGER);
CREATE TABLE job_dep(jobid INTEGER, parentid INTEGER);
''')
            conn.commit()
            conn.close()

        self.localconn = LocalConnection(path)

        self.abort_running()

    def getconn(self):
        return self.localconn.getconn()
        # if not self.local.conn:
        #     self.local.conn = sqlite3.connect(self.path)
        #     self.local.conn.row_factory = sqlite3.Row
        # return self.local.conn

    def status(self, jobid):
        conn = self.getconn()
        cur = conn.cursor()
        cur.execute("SELECT state FROM job WHERE jobid = ?", (jobid,))
        state = None
        for row in cur:
            state = row[0]
        cur.close()
        return state

    def findjob(self, maxprocs=None, maxmem=None):
        clauses = []
        vals = []
        if maxprocs is not None:
            clauses.append('procs')
            vals.append(maxprocs)
        if maxmem is not None:
            clauses.append('mem')
            vals.append(maxmem)

        sql = 'SELECT * FROM job WHERE state ="Q"'
        for clause in clauses:
            sql += ' and %s <= ?' % clause

        sql += ' ORDER BY jobid'

        conn = self.getconn()
        job = None
        cur = conn.cursor()
        cur.execute(sql, vals)
        for row in cur:
            job = {}
            for idx, col in enumerate(cur.description):
                job[col[0]] = row[idx]
            break
        cur.close()
        return job
    
    def abort_deps(self, jobid):
        pass
        # sql="SELECT jobid, parentid from job_dep WHERE job_dep.parentid=?"
        # conn = self.getconn()
        # conn.execute(sql, (jobid,))
        # conn.commit()

    def abort_running(self):
        sql = "UPDATE job SET state='A' WHERE state='R'"
        conn = self.getconn()
        conn.execute(sql)
        conn.commit()

    def update_job_state(self, jobid, state, retcode=None):
        if state == 'R':
            sql = 'UPDATE job SET state=?, started=? WHERE jobid=?'
            vals = (state, datetime.datetime.now(), jobid)
        elif state in ['S', 'F', 'A']:
            sql = 'UPDATE job SET state=?, finished=?, retcode=? WHERE jobid=?'
            vals = (state, datetime.datetime.now(), retcode, jobid)
        else:
            return


        conn = self.getconn()
        conn.execute(sql, vals)
        conn.commit()

    def check_held_jobs(self):
        conn = self.getconn()

        promote_ids = []
        cur = conn.cursor()
        cur.execute(" SELECT a.jobid, jd.parentid, b.state FROM job a LEFT OUTER JOIN job_dep jd ON (a.jobid=jd.jobid) LEFT OUTER JOIN job b ON (jd.parentid=b.jobid) WHERE a.state='H'", )

        last_jobid = None
        last_passed = True
        for row in cur:
            if row[0] != last_jobid:
                if last_jobid and last_passed:
                    promote_ids.append(last_jobid)
                last_passed = True
                
            last_jobid = row[0]
            if row[1] and row[2] != 'S':
                last_passed = False
        cur.close()

        if last_jobid and last_passed:
            promote_ids.append(last_jobid)

        if promote_ids:
            for jobid in promote_ids:
                conn.execute("UPDATE job SET state='Q' WHERE jobid=?", (jobid,))
            conn.commit()


    def submit(self, job):
        keys = ['src', 'submitted', 'state']
        vals = [job['src'], datetime.datetime.now(), 'H']

        valid = ['procs', 'mem', 'name', 'stdout', 'stderr', 'env', 'cwd', 'uid', 'gid']

        for k in valid:
            if k in job:
                keys.append(k)
                vals.append(job[k])

        if not 'name' in keys:
            keys.append('name')
            vals.append('sjqjob')

        sql = 'INSERT INTO job (%s) VALUES (%s)' % (','.join(keys), ','.join(["?"] * len(keys)))

        conn = self.getconn()
        cur = conn.cursor()
        cur.execute(sql, vals)
        jobid = cur.lastrowid
        cur.close()

        if 'depends' in job and job['depends']:
            for depid in job['depends'].split(':'):
                sql = 'INSERT INTO job_dep (jobid, parentid) VALUES (?, ?)'
                vals = (jobid, depid)
                conn.execute(sql, vals)

        conn.commit()
        return jobid

    def close(self):
        self.localconn.close()
