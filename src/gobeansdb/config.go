package main

import (
	"cmem"
	"fmt"
	"io/ioutil"
	"log"
	mc "memcache"
	"route"
	"store"

	yaml "gopkg.in/yaml.v2"
)

var (
	config = GoBeansdbConfig{
		Addr:    "127.0.0.1:7900",
		Listen:  "0.0.0.0",
		Port:    7900,
		WebPort: 7908,
		Threads: 4,
		ZK:      "NO",
	}
)

type GoBeansdbConfig struct {
	Addr    string `yaml:"-"` // HStoreConfig.Hostname:Port
	ZK      string `yaml:",omitempty"`
	Listen  string `yaml:",omitempty"`
	Port    int    `yaml:",omitempty"`
	WebPort int    `yaml:",omitempty"`
	Threads int    `yaml:",omitempty"`
	LogDir  string `yaml:",omitempty"`

	store.HStoreConfig `yaml:"hstore,omitempty"`
	cmem.Config        `yaml:"cmem,omitempty"`
}

func loadServerConfig(path string) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal("read config failed", path, err.Error())
	}

	if err := yaml.Unmarshal(content, &config); err != nil {
		log.Fatal("unmarshal yaml format config failed")
	}
	checkEmptyConfig(path)
}

func checkEmptyConfig(path string) {
	if config.MaxKeySize == 0 || config.SplitCountStr == "" || config.ThresholdListKey == 0 {
		log.Fatal("bad config: empty struct in ", path)
	}
}

func loadConfigs(confdir string) {
	store.InitDefaultConfig()
	config.HStoreConfig = store.GetConfig()
	config.Config = cmem.MemConfig
	if confdir != "" {
		loadServerConfig(fmt.Sprintf("%s/%s", confdir, "server_global.yaml"))
		loadServerConfig(fmt.Sprintf("%s/%s", confdir, "server_local.yaml"))
		// route
		rt, err := route.LoadRouteTable(fmt.Sprintf("%s/%s", confdir, "route.yaml"), config.ZK)
		if err != nil {
			log.Fatalf("fail to load route table")
		}
		config.Addr = fmt.Sprintf("%s:%d", config.HStoreConfig.Hostname, config.Port)

		config.HStoreConfig.RouteConfig = rt.GetServerConfig(config.Addr)
		log.Printf("route table: %#v", rt)
	}
	// config store
	err := config.HStoreConfig.Init()
	if err != nil {
		log.Fatalf("bad config: %s", err.Error())
	}
	store.SetConfig(config.HStoreConfig)
	cmem.MemConfig = config.Config
	// config mc
	mc.MaxKeyLength = config.HStoreConfig.MaxKeySize
	mc.MaxBodyLength = int(config.HStoreConfig.MaxValueSize)
}

func dumpConfigs() {
	b, err := yaml.Marshal(config)
	if err != nil {
		log.Panicln(err)
	} else {
		fmt.Println(string(b))
	}
}
