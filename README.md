# GoBeansDB [![Build Status](http://qa-ci.intra.douban.com/job/peteris-coresys_gobeansdb-master-unittest/badge/icon)](http://qa-ci.intra.douban.com/job/peteris-coresys_gobeansdb-master-unittest/)

Yet anonther distributed key-value storage system from Douban Inc.

# Install

```
$ cd ${GOHOME}
$ git clone http://github.intra.douban.com/coresys/gobeansdb.git src/github.intra.douban.com/coresys/gobeansdb
$ cd src/github.intra.douban.com/coresys/gobeansdb
$ make
```

# test

```
$ make test  # unit test
$ make pytest  # Integrated test
```

# run

```
$ ${GOHOME}/bin/gobeansdb -h
```
