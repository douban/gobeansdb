# coding: utf-8
import telnetlib
import unittest
from tests.dbclient import MCStore
from tests.base import BeansdbInstance


class AbnormalCmdTest(unittest.TestCase):
    def setUp(self):
        self.addr = '127.0.0.1:7900'
        self.db = BeansdbInstance()
        self.db.start()
        self.store = MCStore(self.addr)
        self.invalid_key = '/this/is/a/bad/key/%s' % chr(15)

    def tearDown(self):
        self.db.clean()

    def run_cmd_by_telnet(self, cmd, expected, timeout=2):
        addr, port = self.addr.split(':')
        t = telnetlib.Telnet(addr, port)
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

        cmd = 'set /test/set 0 0 3\r\naaaa'
        self.run_cmd_by_telnet(cmd, 'CLIENT_ERROR bad data chunk')

    def test_incr(self):
        key = '/test/incr'
        self.assertEqual(self.store.delete(key), True)
        cmd = 'incr %s 10' % key
        self.run_cmd_by_telnet(cmd, '10')
        self.assertEqual(self.store.get(key), 10)

        # incr 一个 value 为字符串的 key
        key = '/test/incr2'
        self.assertEqual(self.store.set(key, 'aaa'), True)
        cmd = 'incr %s 10' % key
        self.run_cmd_by_telnet(cmd, '0')
        self.assertEqual(self.store.get(key), 'aaa')

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

    @unittest.skip("we will check gc completely in another pr")
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
