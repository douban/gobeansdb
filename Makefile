all:install

GOPATH:=$(CURDIR)
export GOPATH

dep:
	go get github.com/spaolacci/murmur3
	go get gopkg.in/yaml.v2

test:dep
	go version
	go test memcache
	go test loghub
	go test gobeansdb
	go test cmem
	go test quicklz
	go test store

install:
	go install gobeansdb
