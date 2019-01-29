all:install

# FIXME: When this issue is done(https://github.com/golang/go/issues/23965#issuecomment-409232583)
# Determine the compiler and version
COMPILER_HELP := $(shell $(CC) --help | head -n 1)
ifneq (,$(findstring clang,$(COMPILER_HELP)))
    COMPILER = clang
else ifneq (,$(findstring gcc,$(COMPILER_HELP)))
    COMPILER = gcc
else
    COMPILER = unknown
endif


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
	CC=$(COMPILER) go install ./
