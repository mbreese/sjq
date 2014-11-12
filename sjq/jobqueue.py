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
        
        if not os.path.exists(path) or os.stat(path).st_size == 0:
            conn = sqlite3.connect(self.path)
            conn.executescript('''\
CREATE TABLE job (jobid INTEGER PRIMARY KEY ASC AUTOINCREMENT, src TEXT, state TEXT, name TEXT NOT NULL DEFAULT 'sjqjob', submitted TIMESTAMP, started TIMESTAMP, finished TIMESTAMP, procs INTEGER, mem INTEGER, stdout TEXT, stderr TEXT, env TEXT, cwd TEXT, uid INTEGER, gid INTEGER, retcode INTEGER, abort_jobid INTEGER);
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

    def status(self, jobid=None):
        conn = self.getconn()
        cur = conn.cursor()
        if jobid:
            cur.execute("SELECT j.jobid, j.name, j.state, (SELECT group_concat(parentid,':') FROM job_dep jd WHERE jd.jobid=j.jobid) FROM job j WHERE j.jobid = ?", (jobid,))
        else:
            cur.execute("SELECT j.jobid, j.name, j.state, (SELECT group_concat(parentid,':') FROM job_dep jd WHERE jd.jobid=j.jobid) FROM job j")

        tups = []
        for row in cur:
            tups.append((row[0], row[1], row[2], row[3] if row[3] else ''))
        cur.close()
        return tups

    def jobstates(self):
        conn = self.getconn()
        cur = conn.cursor()
        cur.execute("SELECT state, COUNT(state) FROM job GROUP BY state")
        states = []
        for row in cur:
            states.append((row[0], row[1]))
        cur.close()
        return states

    def findjob(self, maxprocs=None, maxmem=None):
        clauses = []
        vals = []
        if maxprocs is not None:
            clauses.append('procs')
            vals.append(maxprocs)
        if maxmem is not None:
            clauses.append('mem')
            vals.append(maxmem)

        sql = 'SELECT * FROM job WHERE state = "Q"'
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
    
    def abort_deps(self, jobid, orig_id=None):
        if orig_id is None:
            orig_id = jobid

        sql = "UPDATE job SET state='A', abort_jobid=? WHERE jobid IN (SELECT jobid FROM job_dep WHERE parentid=?);"
        args = (orig_id, jobid)

        conn = self.getconn()
        conn.execute(sql, args)
        conn.commit()

        child_ids = []

        sql = 'SELECT jobid FROM job_dep WHERE parentid=?'
        cur = conn.cursor()
        cur.execute(sql, (jobid,))
        for row in cur:
            child_ids.append(row[0])
        cur.close();

        for child_id in child_ids:
            self.abort_deps(child_id, orig_id)

    def abort_running(self):
        sql = "UPDATE job SET state='A' WHERE state='R'"
        conn = self.getconn()
        conn.execute(sql)
        conn.commit()

    def update_job_state(self, jobid, newstate, retcode=None):
        if newstate == 'R':
            sql = "UPDATE job SET state=?, started=? WHERE jobid=? AND state = 'Q'"
            vals = (newstate, datetime.datetime.now(), jobid)
        elif newstate in ['S', 'F']:
            sql = "UPDATE job SET state=?, finished=?, retcode=? WHERE jobid=? AND state='R'"
            vals = (newstate, datetime.datetime.now(), retcode, jobid)
        elif newstate == 'K':
            sql = "UPDATE job SET state=?, finished=? WHERE jobid=? AND (state='R' OR state='Q' OR state='H')"
            vals = (newstate, datetime.datetime.now(), jobid)
        elif newstate == 'H':
            sql = "UPDATE job SET state=? WHERE jobid=? AND state='U'"
            vals = (newstate, jobid)
        else:
            return

        conn = self.getconn()
        conn.execute(sql, vals)
        conn.commit()

    def check_held_jobs(self):
        conn = self.getconn()

        promote_ids = []
        aborted_ids = {}
        cur = conn.cursor()
        cur.execute("SELECT a.jobid, jd.parentid, b.state FROM job a LEFT OUTER JOIN job_dep jd ON (a.jobid=jd.jobid) LEFT OUTER JOIN job b ON (jd.parentid=b.jobid) WHERE a.state='H'")

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
                if row[2] in ['A', 'F', 'K']:
                    aborted_ids[row[0]] = row[1]
        cur.close()

        if last_jobid and last_passed:
            promote_ids.append(last_jobid)

        if promote_ids:
            for jobid in promote_ids:
                if not jobid in aborted_ids:
                    conn.execute("UPDATE job SET state='Q' WHERE jobid=?", (jobid,))
                conn.commit()

        if aborted_ids:
            for jobid in aborted_ids:
                conn.execute("UPDATE job SET state='A', abort_jobid=? WHERE jobid=?", (aborted_ids[jobid], jobid,))
                self.abort_deps(jobid, aborted_ids[jobid])
            conn.commit()
            self.check_held_jobs()

    def submit(self, job):
        keys = ['src', 'submitted', 'state']
        vals = [job['src'], datetime.datetime.now(), ]

        if 'hold' in job and job['hold']:
            vals.append('U')
        else:
            vals.append('H')

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
