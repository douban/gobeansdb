package main

import (
	"config"
	"flag"
	"fmt"
	"loghub"
	mc "memcache"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"store"
	"syscall"
)

var (
	server  *mc.Server
	storage *Storage
	conf    = &config.DB
	logger  = loghub.Default
)

func initLog() {
	if conf.LogDir != "" {
		logpath := filepath.Join(conf.LogDir, "gobeansdb.log")
		logger.Infof("loggging to %s", logpath)
		loghub.SetDefault(logpath, loghub.INFO, 200)
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
	var confdir = flag.String("confdir", "", "path of server config dir")
	var dumpconf = flag.Bool("dumpconf", false, "")

	flag.Parse()

	conf.Load(*confdir)
	if *dumpconf {
		config.DumpConfig(conf)
		return
	}

	initLog()

	logger.Infof("gorivendb version %s starting at %d, config: %#v", mc.VERSION, conf.Port, conf)
	logger.Infof("route table: %#v", config.Route)
	runtime.GOMAXPROCS(conf.Threads)
	initWeb()

	var err error
	storage = new(Storage)
	storage.hstore, err = store.NewHStore()
	if err != nil {
		logger.Fatalf("fail to init NewHStore %s", err.Error())
	}

	server = mc.NewServer(storage)
	addr := fmt.Sprintf("%s:%d", conf.Listen, conf.Port)
	if err := server.Listen(addr); err != nil {
		logger.Fatalf("listen failed", err.Error())
	}
	logger.Infof("mc server listen at %s", addr)
	handleSignals()
	go storage.hstore.Flusher()
	err = server.Serve()
	tmp := storage
	storage = nil
	tmp.hstore.Close()

	logger.Infof("shut down gracefully")
}
