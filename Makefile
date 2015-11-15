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
	go test config
	go test cmem
	go test quicklz
	go test store

# Only for local test now.
# Need start a gobeansdb server on port 7900
pytest:
	virtualenv venv
	venv/bin/pip install -r tests/pip-req.txt
	venv/bin/nosetests -v

install:
	go install gobeansdb
