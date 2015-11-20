# coding: utf-8

import string
import zlib
import unittest
from tests.base import BeansdbInstance, BaseTest
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


if __name__ == '__main__':
    unittest.main()