# coding: utf-8
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


class MCStore(object):

    IGNORED_LIBMC_RET = frozenset([
        libmc.MC_RETURN_OK,
        libmc.MC_RETURN_INVALID_KEY_ERR
    ])

    def __init__(self, addr, **kwargs):
        self.addr = addr
        self.mc = connect(addr, **kwargs)

    def __repr__(self):
        return '<MCStore(addr=%s)>' % repr(self.addr)

    def __str__(self):
        return self.addr

    def set(self, key, data, rev=0):
        return bool(self.mc.set(key, data, rev))

    def set_raw(self, key, data, rev=0, flag=0):
        if rev < 0:
            raise Exception(rev)
        return self.mc.set_raw(key, data, rev, flag)

    def set_multi(self, values, return_failure=False):
        return self.mc.set_multi(values, return_failure=return_failure)

    def _check_last_error(self):
        last_err = self.mc.get_last_error()
        if last_err not in self.IGNORED_LIBMC_RET:
            raise IOError(last_err, self.mc.get_last_strerror())

    def get(self, key):
        try:
            r = self.mc.get(key)
            if r is None:
                self._check_last_error()
            return r
        except ValueError:
            self.mc.delete(key)

    def get_raw(self, key):
        r, flag = self.mc.get_raw(key)
        if r is None:
            self._check_last_error()
        return r, flag

    def get_multi(self, keys):
        r = self.mc.get_multi(keys)
        self._check_last_error()
        return r

    def delete(self, key):
        return bool(self.mc.delete(key))

    def delete_multi(self, keys, return_failure=False):
        return self.mc.delete_multi(keys, return_failure=return_failure)

    def exists(self, key):
        meta_info = self.mc.get('?' + key)
        if meta_info:
            version = meta_info.split(' ')[0]
            return int(version) > 0
        return False

    def incr(self, key, value):
        return self.mc.incr(key, int(value))