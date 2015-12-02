# coding: utf-8

import string
import zlib
import unittest
import time
from tests.base import BaseTest
from tests.dbclient import MCStore
from tests.utils import random_string


VERSION, HASH, FLAG, SIZE, TIMESTAMP, CHUNKID, OFFSET = range(7)

class KeyVersionTest(BaseTest):
    def setUp(self):
        BaseTest.setUp(self)

        self.last_pos = 0
        self.last_size = 0

    def update_pos(self, size):
        self.last_pos += self.last_size
        self.last_size = size

    def get_meta(self, store, key):
        meta = store.get("??" + key)
        if meta:
            meta = meta.split()
            assert(len(meta) == 7)
            return tuple([int(meta[i]) for i in [VERSION, CHUNKID, OFFSET]])

    def test_set_version(self):
        store = MCStore(self.db.addr)
        key = 'key1'
        store.set(key, 'aaa')
        self.update_pos(256)

        self.assertEqual(store.get(key), 'aaa')
        self.assertEqual(self.get_meta(store, key), (1, 0, self.last_pos))

        store.set_raw(key, 'bbb', rev=3)
        self.update_pos(256)
        self.assertEqual(self.get_meta(store, key), (3, 0, self.last_pos))

        store.set_raw(key, 'bbb', rev=4)
        self.assertEqual(self.get_meta(store, key), (4, 0, self.last_pos))

        store.set_raw(key, 'ccc', rev=2)
        self.assertEqual(store.get(key), 'bbb')
        self.assertEqual(self.get_meta(store, key), (4, 0, self.last_pos))

        self.checkCounterZero()

    def test_delete_version(self):
        store = MCStore(self.db.addr)
        key = 'key1'

        store.set(key, 'aaa')
        self.update_pos(256)
        self.assertEqual(self.get_meta(store, key), (1, 0, self.last_pos))

        store.delete(key)
        self.update_pos(256)
        self.assertEqual(store.get(key), None)

        self.assertEqual(self.get_meta(store, key), (-2, 0, self.last_pos))
        self.checkCounterZero()

        store.set(key, 'bbb')
        self.update_pos(256)
        self.assertEqual(store.get(key), 'bbb')
        self.assertEqual(self.get_meta(store, key), (3, 0, self.last_pos))
        self.checkCounterZero()

    def _test_compress(self, overflow):
        store = MCStore(self.db.addr)
        value = string.letters
        compressed_value = zlib.compress(value, 0)
        key = 'k' * (256 - len(compressed_value) - 24 + (1 if overflow else 0))

        value_easy_compress = 'v' * len(compressed_value)
        self.assertTrue(store.set(key, value_easy_compress))
        self.assertEqual(store.get(key), value_easy_compress)
        self.update_pos(256)
        self.assertEqual(self.get_meta(store, key), (1, 0, self.last_pos))

        self.assertTrue(store.set_raw(key, compressed_value, flag=0x00000010))
        self.assertEqual(store.get(key), value)
        self.update_pos(512 if overflow else 256)
        self.assertEqual(self.get_meta(store, key), (2, 0, self.last_pos))

        self.assertTrue(store.set(key, 'aaa'))
        self.update_pos(256)
        self.assertEqual(self.get_meta(store, key), (3, 0, self.last_pos))
        self.checkCounterZero()

    def test_compress_257(self):
        self._test_compress(overflow=True)

    def test_compress_256(self):
        self._test_compress(overflow=False)

    def test_special_key(self):
        store = MCStore(self.db.addr)
        kvs = [('a' * 200, 1), ('a', range(1000))]
        for k, v in kvs:
            self.assertTrue(store.set(k, v))
            self.assertEqual(store.get(k), v)

        # restart
        self.db.stop()
        self.db.start()
        store = MCStore(self.db.addr)
        for (k, v) in kvs:
            v2 = store.get(k)
            self.assertEqual(v2, v, "key %s, value %s, not %s" % (k, v, v2))
        self.checkCounterZero()

    def test_big_value(self):
        store = MCStore(self.db.addr)
        key = 'largekey'
        size = 10 * 1024 * 1024
        rsize = (((size + len(key) + 24) >> 8) + 1) << 8
        string_large = random_string(size / 10) * 10

        self.assertTrue(store.set(key, string_large))
        self.assertEqual(store.get(key), string_large)
        self.update_pos(rsize)

        self.assertEqual(self.get_meta(store, key), (1, 0, self.last_pos))

        self.assertTrue(store.set(key, 'aaa'))
        self.update_pos(256)
        self.assertEqual(self.get_meta(store, key), (2, 0, self.last_pos))

        self.checkCounterZero()

    def test_collision(self):
        # keyhash = "c80f795945b78f6b"
        store = MCStore(self.db.addr)
        # key1 and key2 have the same keyhash "c80f795945b78f6b"
        # key_other and key_other2 is in bucket c too
        # key_other is used to test with key1, key2 for get/set/gc...
        # key_other2 is used to make it possible to gc [0, 1]
        key1 = "processed_log_backup_text_20140912102821_1020_13301733"
        key2 = "/subject/10460967/props"
        key_other = "/ark/p/385242854/"
        key_other2 = "/doumail/849134123/source"
        keys = [key1, key2, key_other]
        for k in keys:
            self.assertTrue(store.set(k, k))
            self.assertEqual(store.get(k), k)

        for i, k in enumerate(keys):
            self.assertEqual(store.get(k), k)
            ver = 2 if i == 1 else 1
            self.assertEqual(self.get_meta(store, k), (ver, 0, i *256))

        self.db.stop()
        self.db.start()
        store = MCStore(self.db.addr)
        for k in keys:
            self.assertEqual(store.get(k), k)

        for k in keys:
            self.assertTrue(store.set(k, k + k))

        for i, k in enumerate(keys):
            self.assertEqual(store.get(k), k + k)
            ver = 3 if i == 1 else 2
            self.assertEqual(self.get_meta(store, k), (ver, 1, i * 256))

        self.db.stop()
        self.db.start()
        store = MCStore(self.db.addr)
        self.assertTrue(store.set(key_other2, 1))

        self.db.stop()
        self.db.start()
        store = MCStore(self.db.addr)

        self.gc(12)
        time.sleep(1)

        for i, k in enumerate(keys):
            self.assertEqual(store.get(k), k + k)
            ver = 3 if i == 1 else 2
            self.assertEqual(self.get_meta(store, k), (ver, 0, i * 256))


if __name__ == '__main__':
    unittest.main()

