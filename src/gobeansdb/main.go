package main

import (
	"config"
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
	conf    = &config.DB
)

func initLog() {
	// TODO
	logpath := filepath.Join(conf.LogDir, "gobeansdb.log")
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
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var confdir = flag.String("confdir", "", "path of server config dir")
	var dumpconf = flag.Bool("dumpconf", false, "")

	flag.Parse()

	conf.Load(*confdir)
	if *dumpconf {
		config.DumpConfig(conf)
		return
	}

	log.Printf("gorivendb version %s starting at %d, config: %#v", mc.VERSION, conf.Port, conf)
	runtime.GOMAXPROCS(conf.Threads)
	initWeb()

	var err error
	storage = new(Storage)
	storage.hstore, err = store.NewHStore()
	if err != nil {
		log.Fatal(err.Error())
	}

	server = mc.NewServer(storage)
	addr := fmt.Sprintf("%s:%d", conf.Listen, conf.Port)
	if err := server.Listen(addr); err != nil {
		log.Fatal("listen failed", err.Error())
	}
	log.Printf("mc server listen at %s", addr)
	handleSignals()
	go storage.hstore.Flusher()
	err = server.Serve()
	storage.hstore.Close()

	log.Println("shut down gracefully")
}
