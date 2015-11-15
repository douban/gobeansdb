# coding: utf-8

import os
import time
import shlex
import shutil
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


### BeansdbInstance

class BeansdbInstance(object):
    ''' Start/stop Beansdb instance.
    '''
    def __init__(self):
        self.popen = None
        self.cmd = "./bin/gobeansdb"
        self.addr = '127.0.0.1:7900'
        self.db_home = './testdb'

    def __del__(self):
        self.stop()

    def start(self):
        assert self.popen is None
        self.popen = start_cmd(self.cmd)
        while True:
            try:
                store = MCStore(self.addr)
                store.get("@")
                return
            except IOError:
                time.sleep(0.5)

    def stop(self):
        print 'stop', self.cmd
        if self.popen:
            stop_cmd(self.popen)
            self.popen = None

    def clean(self):
        if self.popen:
            self.stop()
        if os.path.exists(self.db_home):
            shutil.rmtree(self.db_home)


if __name__ == '__main__':
    db = BeansdbInstance()
    db.start()
    db.clean()