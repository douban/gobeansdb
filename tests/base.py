# coding: utf-8

import os
import yaml
import time
import shlex
import shutil
import socket
import subprocess

from tests.dbclient import MCStore
from tests.utils import mkdir_p


### start/stop cmd in subprocess

def start_cmd(cmd):
    print "start", cmd
    log_file = '/tmp/gobeansdb/log.txt'
    mkdir_p(os.path.dirname(log_file))
    with open(log_file, 'a') as f:
        p = subprocess.Popen(
            cmd if isinstance(cmd, (tuple, list,)) else shlex.split(cmd),
            stderr=f,
        )
    time.sleep(0.2)
    if p.poll() is not None:
        raise Exception("cannot start %s" % (cmd))
    return p

def stop_cmd(popen):
    if popen.poll() is not None:
        return
    popen.terminate()
    popen.wait()


### parse config

SERVER_LOCAL = 'beansdb_local.yaml'
SERVER_GLOBAL = 'beansdb_global.yaml'
CONF_DIR = './conf'


def get_server_addr(server_global=SERVER_GLOBAL, server_local=SERVER_LOCAL):
    global_conf = load_yaml(server_global)
    local_conf = load_yaml(server_local)
    port = global_conf['server']['port']
    if local_conf.get('server') and local_conf.get('server').get('port'):
        port = local_conf['server']['port']
    hostname = local_conf['hstore']['local'].get('hostname') or socket.gethostname()
    return '%s:%s' % (hostname, port)


def get_db_homes(server_local=SERVER_LOCAL):
    local_conf = load_yaml(server_local)
    return local_conf['hstore']['local']['homes']


def load_yaml(filename, conf_dir=CONF_DIR):
    filepath = os.path.join(conf_dir, filename)
    with open(filepath) as f:
        return yaml.load(f)


### BeansdbInstance

class BeansdbInstance(object):
    ''' Start/stop Beansdb instance.
    '''
    def __init__(self):
        self.popen = None
        self.cmd = "./bin/gobeansdb -confdir conf"
        self.addr = get_server_addr()
        self.db_homes = get_db_homes()

    def __del__(self):
        self.stop()

    def start(self):
        assert self.popen is None
        self.popen = start_cmd(self.cmd)
        try_times = 0
        while True:
            try:
                store = MCStore(self.addr)
                store.get("@")
                return
            except IOError:
                try_times += 1
                if try_times > 10:
                    raise Exception('connect error for addr: %s', self.addr)
                time.sleep(0.5)

    def stop(self):
        print 'stop', self.cmd
        if self.popen:
            stop_cmd(self.popen)
            self.popen = None

    def clean(self):
        if self.popen:
            self.stop()
        for db_home in self.db_homes:
            if os.path.exists(db_home):
                shutil.rmtree(db_home)


if __name__ == '__main__':
    db = BeansdbInstance()
    db.start()
    db.clean()