all:install

GOPATH:=$(CURDIR)/../../../../
export GOPATH
export GO15VENDOREXPERIMENT=1

godep:
	which godep >/dev/null 2>&1 || go get github.com/tools/godep

savedep: godep
	godep save ./...

test:
	go version
	go test github.intra.douban.com/coresys/gobeansdb/memcache
	go test github.intra.douban.com/coresys/gobeansdb/loghub
	go test github.intra.douban.com/coresys/gobeansdb/cmem
	go test github.intra.douban.com/coresys/gobeansdb/quicklz
	ulimit -n 1024; go test github.intra.douban.com/coresys/gobeansdb/store

pytest:install
	./tests/run_test.sh

install:
	go install github.intra.douban.com/coresys/gobeansdb/gobeansdb
