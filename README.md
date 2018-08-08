# GoBeansDB [![Build Status](https://travis-ci.org/douban/gobeansdb.svg?branch=master)](https://travis-ci.org/douban/gobeansdb) 

Yet anonther distributed key-value storage system from Douban Inc.

# Install

```shell
$ cd ${GOPATH}
$ git clone http://github.com/douban/gobeansdb.git src/github.com/douban/gobeansdb
$ cd src/github.com/douban/gobeansdb
$ make
```

# test

```shell
$ make test  # unit test
$ make pytest  # Integrated test
```

# run

```shell
$ ${GOPATH}/bin/gobeansdb -h
```
