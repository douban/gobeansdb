package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	ZKClient       *zkClient
	LocalRoutePath string
)

type zkClient struct {
	Version int
	Root    string
	Servers []string
	Client  *zk.Conn
	Events  <-chan zk.Event
}

func NewZK(root string, servers []string) (c *zkClient, err error) {
	c = &zkClient{Root: root, Servers: servers}
	c.Client, c.Events, err = zk.Connect(servers, 10*time.Second)
	return c, err
}

func (c *zkClient) Get(subPath string) ([]byte, *zk.Stat, error) {
	path := fmt.Sprintf("%s/%s", c.Root, subPath)
	log.Printf("get zk %s", path)
	return c.Client.Get(path)
}

func (c *zkClient) GetRouteRaw(version int) (data []byte, ver int, err error) {
	if version < 0 {
		data, _, err = c.Get("route")
		if err != nil {
			log.Printf("%v", err)
			return
		}
		log.Printf("got route version %s", string(data))
		ver, err = strconv.Atoi(string(data))
		if err != nil {
			return
		}
	} else {
		ver = version
	}
	sp := fmt.Sprintf("route/route_%010d", ver)
	data, _, err = c.Get(sp)
	if err != nil {
		log.Printf("%v", err)
	}
	return
}

func UpdateLocalRoute(content []byte) {
	log.Printf("update local route %s", LocalRoutePath)
	fd, err := os.OpenFile(LocalRoutePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("fail to write", LocalRoutePath)
	}
	fd.Write(content)
	fd.Close()
}
