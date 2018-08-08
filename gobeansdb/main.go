package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/douban/gobeansdb/config"
	"github.com/douban/gobeansdb/loghub"
	mc "github.com/douban/gobeansdb/memcache"
	"github.com/douban/gobeansdb/store"
)

var (
	server  *mc.Server
	storage *Storage
	conf    DBConfig
	logger  = loghub.ErrorLogger
)

func main() {
	var version = flag.Bool("version", false, "print version of gobeansdb")
	var confdir = flag.String("confdir", "", "path of server config dir")
	var dumpconf = flag.Bool("dumpconf", false, "print configuration")
	var buildhint = flag.String("buildhint", "", "a data file OR a bucket dir")

	flag.Parse()

	if *version {
		fmt.Println("gobeansdb version", config.Version)
		return
	}
	log.Printf("version %s", config.Version)

	if *confdir != "" {
		log.Printf("use confdir %s", *confdir)
	}
	conf.Load(*confdir)
	runtime.GOMAXPROCS(conf.Threads)
	if *dumpconf {
		config.DumpConfig(conf)
		return
	} else if *buildhint != "" {
		if *confdir != "" {
			initWeb()
		} else {
			conf.HintConfig.SplitCap = (3 << 20) // cost maxrss about 900M
		}
		if err := store.DataToHint(*buildhint); err != nil {
			log.Printf("%s", err.Error())
		}
		return
	}

	loghub.InitLogger(conf.ErrorLog, conf.AccessLog, conf.AnalysisLog)
	logger.Infof("gobeansdb version %s starting at %d, config: %#v",
		config.Version, conf.Port, conf)

	if config.ZKClient == nil {
		logger.Warnf("route version: local")
	} else {
		logger.Infof("route version: %d", config.ZKClient.Version)
	}
	logger.Infof("route table: %#v", config.Route)

	initWeb()

	var err error

	hstore, err := store.NewHStore()
	if err != nil {
		logger.Fatalf("fail to init NewHStore %s", err.Error())
	}
	storage = &Storage{hstore: hstore}

	server = mc.NewServer(storage)
	addr := fmt.Sprintf("%s:%d", conf.Listen, conf.Port)
	if err := server.Listen(addr); err != nil {
		logger.Fatalf("listen failed %s", err.Error())
	}
	logger.Infof("mc server listen at %s", addr)
	log.Println("ready")

	server.HandleSignals(conf.ErrorLog, conf.AccessLog, conf.AnalysisLog)
	go storage.hstore.HintDumper(1 * time.Minute) // it may start merge go routine
	go storage.hstore.Flusher()
	config.AllowReload = true
	err = server.Serve()
	tmp := storage
	storage = nil
	tmp.hstore.Close()

	logger.Infof("shut down gracefully")
}
