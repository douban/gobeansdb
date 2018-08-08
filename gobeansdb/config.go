package main

import (
	"fmt"
	"log"
	"os"

	"github.com/douban/gobeansdb/config"
	"github.com/douban/gobeansdb/store"
	"github.com/douban/gobeansdb/utils"
)

type DBConfig struct {
	config.ServerConfig `yaml:"server,omitempty"`
	config.MCConfig     `yaml:"mc,omitempty"`
	store.HStoreConfig  `yaml:"hstore,omitempty"`
}

func (c *DBConfig) ConfigPackages() {
	config.ServerConf = c.ServerConfig
	config.MCConf = c.MCConfig
	store.Conf = &c.HStoreConfig
}

func (c *DBConfig) Load(confdir string) {
	c.InitDefault()

	if confdir != "" {
		// global
		path := fmt.Sprintf("%s/%s", confdir, "global.yaml")
		err := config.LoadYamlConfig(c, path)
		if err != nil {
			log.Fatalf("bad config %s: %s", path, err.Error())
		}
		c.checkEmptyConfig(path)

		//local
		path = fmt.Sprintf("%s/%s", confdir, "local.yaml")
		if _, e := os.Stat(path); e == nil {
			err = config.LoadYamlConfig(c, path)
			if err != nil {
				log.Fatalf("bad config %s: %s", path, err.Error())
			}
			c.checkEmptyConfig(path)
		}

		routePath := fmt.Sprintf("%s/%s", confdir, "route.yaml")
		var route *config.RouteTable
		// route
		if len(c.ZKServers) > 0 {
			route, err = config.LoadRouteTableZK(routePath, c.ZKPath, c.ZKServers)
			if err != nil {
				log.Printf("fail to load route table from zk: %s", err.Error())
			}
		}
		if len(c.ZKServers) == 0 || err != nil {
			route, err = config.LoadRouteTableLocal(routePath)
		}
		if err != nil {
			log.Fatalf("fail to load route table: %s", err.Error())
		}
		c.DBRouteConfig = route.GetDBRouteConfig(c.Addr())
		config.Route = *route
	}
	utils.InitSizesPointer(c)
	err := c.HStoreConfig.InitTree()
	if err != nil {
		log.Fatalf("bad config: %s", err.Error())
	}
	c.ConfigPackages()
}

func (c *DBConfig) InitDefault() {
	c.ServerConfig = config.DefaultServerConfig
	c.MCConfig = config.DefaultMCConfig
	c.HStoreConfig.InitDefault()
	utils.InitSizesPointer(c)
}

func (c *DBConfig) checkEmptyConfig(path string) {
	if c.MaxKeyLen == 0 || c.SplitCapStr == "" || c.TreeHeight == 0 {
		log.Fatal("bad config: empty struct in ", path)
	}
}
