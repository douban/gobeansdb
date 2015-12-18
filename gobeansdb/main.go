package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.intra.douban.com/coresys/gobeansdb/config"
	"github.intra.douban.com/coresys/gobeansdb/loghub"
	mc "github.intra.douban.com/coresys/gobeansdb/memcache"
	"github.intra.douban.com/coresys/gobeansdb/store"
)

var (
	server  *mc.Server
	storage *Storage
	conf    = &config.DB
	logger  = loghub.ErrorLog
)

func initLog() {
	if conf.ErrLog != "" {
		logpath := conf.ErrLog
		log.Printf("loggging to %s", logpath)
		loghub.InitErrorLog(conf.ErrLog, loghub.INFO, 200)
	}
}

func handleSignals() {
	sch := make(chan os.Signal, 10)
	signal.Notify(sch, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT,
		syscall.SIGHUP, syscall.SIGSTOP, syscall.SIGQUIT)
	go func(ch <-chan os.Signal) {
		for {
			sig := <-ch
			logger.Infof("signal recieved " + sig.String())
			server.Shutdown()
		}
	}(sch)
}

func main() {
	var version = flag.Bool("version", false, "print version of gobeansdb")
	var confdir = flag.String("confdir", "", "path of server config dir")
	var dumpconf = flag.Bool("dumpconf", false, "print configuration")
	var buildhint = flag.String("buildhint", "", "a data file OR a bucket dir")

	flag.Parse()

	if *version {
		fmt.Println("gobeansdb version", config.Version)
		return
	} else {
		log.Printf("version %s", config.Version)
	}

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
		store.DataToHint(*buildhint)
		return
	}

	initLog()
	logger.Infof("gobeansdb version %s starting at %d, config: %#v",
		config.Version, conf.Port, conf)
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
		logger.Fatalf("listen failed", err.Error())
	}
	logger.Infof("mc server listen at %s", addr)
	log.Println("ready")
	handleSignals()
	go storage.hstore.HintDumper(1 * time.Minute) // it may start merge go routine
	go storage.hstore.Flusher()
	err = server.Serve()
	tmp := storage
	storage = nil
	tmp.hstore.Close()

	logger.Infof("shut down gracefully")
}
