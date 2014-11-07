import os
import multiprocessing

import sjq

CONFIG_FILE=os.path.expanduser("~/.sjqrc")

_config = None

_defconfig = {
    'sjq.socket':  os.path.expanduser('~/.sjq.sock'),
    'sjq.db':  os.path.expanduser('~/.sjq.db'),
    'sjq.logfile':  None,
    'sjq.daemon':  False,
    'sjq.pidfile': os.path.expanduser('~/.sjq.pid'),
    'sjq.autoshutdown':  True,
    'sjq.waittime':  60,
    'sjq.maxjobs':  10000,
    'sjq.maxprocs': multiprocessing.cpu_count(),
    'sjq.maxmem':  None,
    'sjq.defaults.procs':  1,
    'sjq.defaults.mem':  sjq.convert_mem_val('2G'),
    'sjq.cur_jobid': 1
}

def get_config():
    global _config
    if not _config:
        _config = _defconfig
    return _config


def load_config(defaults=None):
    global _config

    if not _config:
        _config = _defconfig

    if defaults:
        for k in defaults:
            _config[k] = defaults[k]

    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            for line in f:
                if '=' in line:
                    spl = [x.strip() for x in line.strip().split('=',1)]
                    _config[spl[0]] = autotype(spl[1])
    return _config


def config_prefix(prefix):
    out = {}
    for k in _config:
        if k[:len(prefix)] == prefix:
            out[k[len(prefix):]] = _config[k]
    return out


def autotype(val):
    if not val:
        return ''
    try:
        ret = int(val)
        return ret
    except:
        try:
            ret = float(val)
            return ret
        except:
            if val.upper() in ['T', 'TRUE', 'Y', 'YES']:
                return True
            if val.upper() in ['F', 'FALSE', 'N', 'NO']:
                return False

            if val[0] == '"' and val[-1] == '"':
                val = val[1:-1]
            elif ' ' in val:
                val = val.split()
            return val

