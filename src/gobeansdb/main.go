package main

import (
	"flag"
	"fmt"
	"log"
	mc "memcache"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"store"
	"syscall"
)

var (
	server *mc.Server
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

func initWeb() {
	webaddr := fmt.Sprintf("%s:%d", config.Listen, config.WebPort)
	go func() {
		log.Printf("http listen at %s", webaddr)
		err := http.ListenAndServe(webaddr, nil) //start web before load
		if err != nil {
			log.Fatal(err.Error())
		}

	}()
}

func main() {
	var confdir = flag.String("confdir", "", "path of server config file")
	flag.Parse()

	loadConfigs(*confdir)

	log.Printf("gorivendb version %s starting at %d, config: %#v", mc.VERSION, config.Port, config)
	runtime.GOMAXPROCS(config.Threads)
	initWeb()

	var err error
	storage := new(Storage)
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
