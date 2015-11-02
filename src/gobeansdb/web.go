package main

import (
	"cmem"
	"encoding/json"
	"fmt"
	"log"
	mc "memcache"
	"net/http"
	_ "net/http/pprof"

	yaml "gopkg.in/yaml.v2"
)

// TODO:
//   migrate: list dir, start file server,  start  pull client, block gc
//   reload: local config, route config
//   stats:
//	  - mem:  c, go, data, hint
//    - buckets: #key, #req, space

func init() {
	http.HandleFunc("/", handleIndex)

	http.HandleFunc("/config", handleConfig)
	//stats
	http.HandleFunc("/requests", handleRequests)
	http.HandleFunc("/buffers", handleBuffers)

	http.HandleFunc("/buckets", handleBuckets)

	http.HandleFunc("/reload", handleReload)

}

func initWeb() {
	webaddr := fmt.Sprintf("%s:%d", config.Listen, config.WebPort)
	http.Handle("/log", http.FileServer(http.Dir(config.LogDir))) // TODO: tail

	go func() {
		log.Printf("http listen at %s", webaddr)
		err := http.ListenAndServe(webaddr, nil) //start web before load
		if err != nil {
			log.Fatal(err.Error())
		}

	}()
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w,
		`
    <a href='/debug/pprof'> /debug/pprof </a> <p/>
    <a href='/config'> /config </a> <p/>
    <a href='/requests'> /requests </a> <p/>
    <a href='/buffers'> /buffers </a> <p/>
    <a href='/log'> /log </a> <p/>
    <a href='/buckets'> /buckets </a> <p/>

    `)
}

func handleWebPanic(w http.ResponseWriter) {
	r := recover()
	if r != nil {
		fmt.Fprintf(w, "\npanic:%#v, stack:%s", r, getStack(1000))
	}
}

func handleJson(w http.ResponseWriter, v ...interface{}) {
	defer handleWebPanic(w)
	b, err := json.Marshal(v)
	if err != nil {
		w.Write([]byte(err.Error()))
	} else {
		w.Write(b)
	}

}

func handleYaml(w http.ResponseWriter, v ...interface{}) {
	defer handleWebPanic(w)
	b, err := yaml.Marshal(v)
	if err != nil {
		w.Write([]byte(err.Error()))
	} else {
		w.Write(b)
	}
}

func handleConfig(w http.ResponseWriter, r *http.Request) {
	handleJson(w, config)
}

func handleRequests(w http.ResponseWriter, r *http.Request) {
	handleJson(w, mc.RL)
}

func handleBuffers(w http.ResponseWriter, r *http.Request) {
	handleJson(w, cmem.AllocedSize, cmem.AllocedCount)
}

func handleReload(w http.ResponseWriter, r *http.Request) {
	// TODO:
	// reload local config,  then drop/load bucket
	// reload route config
}

func handleBuckets(w http.ResponseWriter, r *http.Request) {
	// TODO: show infos by buckets
}
