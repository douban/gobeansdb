all:install

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
	GO111MODULE=on go mod vendor
	go install ./
