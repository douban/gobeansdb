all:install

GOPATH:=$(CURDIR)/../../../../
export GOPATH
export GO15VENDOREXPERIMENT=1

godep:
	which godep >/dev/null 2>&1 || go get github.com/tools/godep

savedep: godep
	if [ -d "./Godeps" ]; then rm -r Godeps; fi
	godep save ./...

test:
	go version
	go test github.com/douban/gobeansdb/memcache
	go test github.com/douban/gobeansdb/loghub
	go test github.com/douban/gobeansdb/cmem
	go test github.com/douban/gobeansdb/quicklz
	ulimit -n 1024; go test github.com/douban/gobeansdb/store

pytest:install
	./tests/run_test.sh

install:
	go install github.com/douban/gobeansdb/gobeansdb
