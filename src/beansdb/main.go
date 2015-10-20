package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
)

func initLog() {
	// TODO
	logpath := filepath.Join(config.LogDir, "gobeansdb.log")
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
		err = http.ListenAndServe(webaddr, nil) //start web before load
		if err != nil {
			log.Fatal(err.Error())
		}

	}()
}

func main() {
	var confdir = flag.String("confdir", "", "path of server config file")
	flag.Parse()

	loadConfigs(*confdir)

	log.Printf("gorivendb version %s starting at %s %#v", VERSION, config)
	runtime.GOMAXPROCS(config.Threads)
	initWeb()
	s, err = store.NewHStroe()
	if err != nil {
		log.Fatal(err.Error())
	}
	server := mc.NewServer(s)
	addr := fmt.Sprintf("%s:%d", config.Listen, config.Port)
	if err := server.Listen(addr); err != nil {
		log.Fatal("listen failed", err.Error())
	}

	err = server.Serve()
	handleSignals()
	log.Println("shut down gracefully")
}
