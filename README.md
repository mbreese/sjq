Simple Job Queue (SJQ)
====
You specify:
  - how many processes to run (CPUs)
  - how much memory to use
    (and defaults per job) 

The SJQ will open a socket to accept job requests. The protocol for these
requests is a simple text-based protocol. By default, this socket will be
located in $HOME/.mvsjq.sock. Jobs will run under the credentials of the 
running user. When there are no more jobs, SJQ will wait for $TIMEOUT
seconds (default: 60). If there are still no jobs, then it will shut itself
down.

Note: If you want a more complicated setup for more than one client user, you
can  specify an absolute path for the socket, and the SJQ will run the jobs
whatever account has started the daemon. You can also run the daemon as root
and it will run the jobs as the UID/GID of the submitting user. However, if
you *really* want to do this, please consider a different scheduler, such as
Open Grid Engine, PBS, or SLURM. SJQ was designed to make it easier to run
multi-task pipelines on a single server that didn't already have a job
scheduler installed. It should be used sparingly!

Scheduling priority
-------------------
Jobs are executed on a first-come, first-serve basis. If a job can not execute
due to CPU, memory, or job dependencies, then it will be skipped and SJQ will
attempt to run the next available job.


Configuration
-------------
You can set various default values in the $HOME/.sjqrc file. The config
values that are relevant to SJQ are:

sjq.socket=path-to-file           default: $HOME/.sjq.sock
sjq.log=path-to-logfile           default: none
sjq.daemonize=[TF]                default: F
sjq.autoshutdown=[TF]             default: T
sjq.waittime=value-in-seconds     default: 60
sjq.maxjobs=max-jobs-to-keep      default: 10000
sjq.maxprocs=max-procs            default: total CPUs in system
sjq.maxmem=max-mem (ex: 8M, 2G)   default: None (memory use not restricted)

sjq.defaults.procs=num            default: 1
sjq.defaults.mem=mem-per-job      default: 2G

Note: maxjobs is the number of jobs that will be kept in memory, including any
      jobs that have finished. If the length of the job queue reaches maxjobs,
      any completed jobs will be removed from the queue. If there still is not
      enough room, then no more jobs can be submitted to the queue.

Protocol
--------

To check the connection: 

    send: PING\r\n
    recv: PONG\r\n

To close the connection: 

    send: EXIT\r\n
    recv: BYE\r\n

To submit a job:

    send: SUBMIT\r\n
    send: OPTION VALUE\r\n
    send: OPTION VALUE\r\n
          ...
    send: SRC script-len\r\n
    send  <script-bytes>
    recv: OK jobid\r\n
    recv: ERROR error-message(s)\r\n

    Valid options:
    MEM CPU DEPENDS STDOUT STDERR ENV CWD NAME UID* GID*

      MEM   maximum memory required (100M, 2G, etc...) (G=1024^3, M=1024^2)
      CPU   number of CPUs this job requires
            (can not be larger than the number of managed CPUs)

            Note: CPU and MEM restrictions aren't enforced - only used for
                  scheduling purposes

      DEPENDS is a colon delimited list of job-ids that must finish
              (successfully) for this job to start

      STDOUT, STDERR should be filenames - if not specified, they will be saved
                    in the CWD as jobid.{stdout,stderr}

      ENV a colon-delimited list of the environment variables 
          that should be set prior to running the script

      CWD the current working directory for the job

      NAME a human-readable name for the job (not required)

      UID/GID the uid/gid to run this job under - this only works when SJQ is
              started by root and is *not* recommended


To kill a job:

    send: KILL jobid\r\n
    recv: OK\r\n
    recv: ERROR jobid missing\r\n

To stop the server:

    send: SHUTDOWN\r\n
    recv: OK\r\n

To list job status:

    send: STATUS {JOBID}\r\n
    recv: JOBID\tJOBNAME\t[RQHSFAK]\tDEPENDS\r\n
          (one line for each job)
    recv: OK\r\n

If "JOBID" is not given, then all jobs, including jobs that have finished
(successfully or not) will be listed.

[RQHSFAK] - running, queued, holding, success, fail, abort (parent job failed), killed
