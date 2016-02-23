package config

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	ZKClient       *zkClient
	LocalRoutePath string
)

type zkClient struct {
	Stat    *zk.Stat
	Root    string
	Servers []string
	Client  *zk.Conn
	Events  <-chan zk.Event
}

func NewZK(root string, servers []string) (c *zkClient, err error) {
	c = &zkClient{Root: root, Servers: servers}
	fmt.Println(servers)
	c.Client, c.Events, err = zk.Connect(servers, 10*time.Second)
	return c, err
}

func (c *zkClient) Get(subPath string) ([]byte, *zk.Stat, error) {
	return c.Client.Get(fmt.Sprintf("%s/%s", c.Root, subPath))
}

func (c *zkClient) GetRouteRaw() ([]byte, *zk.Stat, error) {
	return c.Get("route")
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
