all:install

GOPATH:=$(CURDIR)/../../../../
export GOPATH

test:
	go version
	vgo test github.com/douban/gobeansdb/memcache
	vgo test github.com/douban/gobeansdb/loghub
	vgo test github.com/douban/gobeansdb/cmem
	vgo test github.com/douban/gobeansdb/quicklz
	ulimit -n 1024; vgo test github.com/douban/gobeansdb/store

pytest:install
	./tests/run_test.sh

install:
	vgo install github.com/douban/gobeansdb
