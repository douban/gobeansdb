package main

import (
	"cmem"
	"encoding/json"
	"fmt"
	"log"
	mc "memcache"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"utils"

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
	http.HandleFunc("/memstats", handleMemStates)
	http.HandleFunc("/rusage", handleRusage)

	http.HandleFunc("/buckets", handleBuckets)

	http.HandleFunc("/reload", handleReload)

	// dir
	http.HandleFunc("/collision/", handleCollision)
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
    <a href='/memstats'> /memstats </a> <p/>
    <a href='/rusage'> /rusage </a> <p/>
    <a href='/log'> /log </a> <p/>
    <a href='/buckets'> /buckets </a> <p/>
    <a href='/collision'> /collision </a> <p/>

    `)
}

func handleWebPanic(w http.ResponseWriter) {
	r := recover()
	if r != nil {
		fmt.Fprintf(w, "\npanic:%#v, stack:%s", r, utils.GetStack(1000))
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

func handleRusage(w http.ResponseWriter, r *http.Request) {
	var rusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	handleJson(w, rusage)
}

func handleMemStates(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	handleJson(w, ms)
}

func handleBuffers(w http.ResponseWriter, r *http.Request) {
	handleJson(w, &cmem.DBRL)
}

func handleCollision(w http.ResponseWriter, r *http.Request) {
	e := []byte("need bucket id, e.g. /collision/c")
	s := filepath.Base(r.URL.Path)
	bucketID, err := strconv.ParseInt(s, 16, 16)
	if err != nil {
		w.Write(e)
		return
	}
	if bucketID > int64(config.NumBucket) || bucketID < 0 {
		w.Write(e)
		return
	}
	w.Write(storage.hstore.GetCollisionsByBucket(int(bucketID)))
}

func handleReload(w http.ResponseWriter, r *http.Request) {
	// TODO:
	// reload local config,  then drop/load bucket
	// reload route config
}

func handleBuckets(w http.ResponseWriter, r *http.Request) {
	// TODO: show infos by buckets
}
