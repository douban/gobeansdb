package main

import (
	"flag"
	"fmt"
	"log"
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
)

func initLog() {
	// TODO
	logpath := filepath.Join(config.LogDir, "gobeansdb.log")
	_ = logpath
}

func handleSignals() {
	sch := make(chan os.Signal, 10)
	signal.Notify(sch, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT,
		syscall.SIGHUP, syscall.SIGSTOP, syscall.SIGQUIT)
	go func(ch <-chan os.Signal) {
		for {
			sig := <-ch
			log.Print("signal recieved " + sig.String())
			server.Shutdown()

		}
	}(sch)
}

func main() {
	var confdir = flag.String("confdir", "", "path of server config dir")
	var dumpconf = flag.Bool("dumpconf", false, "")

	flag.Parse()

	loadConfigs(*confdir)
	if *dumpconf {
		dumpConfigs()
		return
	}

	log.Printf("gorivendb version %s starting at %d, config: %#v", mc.VERSION, config.Port, config)
	runtime.GOMAXPROCS(config.Threads)
	initWeb()

	var err error
	storage = new(Storage)
	storage.hstore, err = store.NewHStore()
	if err != nil {
		log.Fatal(err.Error())
	}

	server = mc.NewServer(storage)
	addr := fmt.Sprintf("%s:%d", config.Listen, config.Port)
	if err := server.Listen(addr); err != nil {
		log.Fatal("listen failed", err.Error())
	}
	handleSignals()
	go storage.hstore.Flusher()
	err = server.Serve()
	storage.hstore.Close()

	log.Println("shut down gracefully")
}
