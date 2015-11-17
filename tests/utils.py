import os
import errno
import string
import random


def mkdir_p(path):
    "like `mkdir -p`"
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def random_string(n):
    s = string.ascii_letters
    result = ""
    for _ in xrange(n):
        result += random.choice(s)
    return result