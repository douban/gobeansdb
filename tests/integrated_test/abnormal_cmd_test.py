# coding: utf-8
import telnetlib
import unittest
import libmc

def connect(server, **kwargs):
    comp_threshold = kwargs.pop('comp_threshold', 0)
    prefix = kwargs.pop('prefix', None)

    c = libmc.Client([server],
                     do_split=0,
                     comp_threshold=comp_threshold,
                     prefix=prefix)
    c.config(libmc.MC_CONNECT_TIMEOUT, 300)  # 0.3s
    c.config(libmc.MC_POLL_TIMEOUT, 3000)  # 3s
    c.config(libmc.MC_RETRY_TIMEOUT, 5)  # 5s
    return c


class AbnormalCmdTest(unittest.TestCase):
    def setUp(self):
        self.server = '127.0.0.1:7900'
        self.db = connect(self.server)
        self.invalid_key = '/this/is/a/bad/key/%s' % chr(15)

    def run_cmd_by_telnet(self, cmd, expected, timeout=2):
        server, port = self.server.split(':')
        t = telnetlib.Telnet(server, port)
        t.write('%s\r\n' % cmd)
        out = t.read_until('\n', timeout=timeout)
        t.write('quit\n')
        t.close()
        r = out.strip('\r\n')
        self.assertEqual(r, expected)

    def test_get(self):
        # get not exist key
        cmd = 'get /test/get'
        self.run_cmd_by_telnet(cmd, 'END')

        # invalid key
        cmd = 'get %s' % self.invalid_key
        self.run_cmd_by_telnet(cmd, 'END')

    def test_set(self):
        # invalid key
        cmd = 'set %s 0 0 3\r\naaa' % self.invalid_key
        self.run_cmd_by_telnet(cmd, 'NOT_STORED')

        # invalid data
        cmd = 'set /test/set 0 0 3\r\naaaa'
        self.run_cmd_by_telnet(cmd, 'CLIENT_ERROR bad data chunk')

    def test_incr(self):
        key = '/test/incr'
        self.assertEqual(self.db.delete(key), True)
        cmd = 'incr %s 10' % key
        self.run_cmd_by_telnet(cmd, '10')
        self.assertEqual(self.db.get(key), 10)

        # incr 一个 value 为字符串的 key
        key = '/test/incr2'
        self.assertEqual(self.db.set(key, 'aaa'), True)
        cmd = 'incr %s 10' % key
        self.run_cmd_by_telnet(cmd, '0')
        self.assertEqual(self.db.get(key), 'aaa')


    def test_delete(self):
        key = '/delete/not/exist/key'
        cmd = 'delete %s' % key
        self.run_cmd_by_telnet(cmd, 'NOT_FOUND')

        cmd = 'delete %s' % self.invalid_key
        self.run_cmd_by_telnet(cmd, 'NOT_FOUND')

    def test_get_meta_by_key(self):
        key = '/get_meta_by_key/not/exist/key'
        cmd = 'get ?%s' % key
        self.run_cmd_by_telnet(cmd, 'END')

        cmd = 'get ?%s' % self.invalid_key
        self.run_cmd_by_telnet(cmd, 'END')


    def test_get_meta_by_path(self):
        key = '/get_meta_by_path/not/exist/key'
        cmd = 'get @%s' % key
        self.run_cmd_by_telnet(cmd, 'END')

        cmd = 'get @%s' % self.invalid_key
        self.run_cmd_by_telnet(cmd, 'END')

    def test_gc(self):
        cmd = 'gc @ 0 0'
        self.run_cmd_by_telnet(cmd, 'CLIENT_ERROR bad command line format')

        cmd = 'gc @0 -1 0'
        self.run_cmd_by_telnet(cmd, 'CLIENT_ERROR bad command line format')

        cmd = 'gc @0 0 -1'
        self.run_cmd_by_telnet(cmd, 'CLIENT_ERROR bad command line format')

        cmd = 'gc @0 2 0'
        self.run_cmd_by_telnet(cmd, 'CLIENT_ERROR start_fid bigger than end_fid')

        cmd = 'gc @0 0 0'
        self.run_cmd_by_telnet(cmd, 'OK')
