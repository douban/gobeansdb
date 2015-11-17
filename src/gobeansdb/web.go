package main

import (
	"cmem"
	"encoding/json"
	"fmt"
	"loghub"
	mc "memcache"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"runtime"
	"store"
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
	http.HandleFunc("/logbuf", handleLogBuffer)
	http.HandleFunc("/logbufall", handleLogBufferALL)
	http.HandleFunc("/loglast", handleLogLast)

	// dir
	http.HandleFunc("/collision/", handleCollision)
	http.HandleFunc("/hash/", handleKeyhash)

}

func initWeb() {
	webaddr := fmt.Sprintf("%s:%d", conf.Listen, conf.WebPort)
	//http.Handle("/log", http.FileServer(http.Dir(conf.LogDir))) // TODO: tail

	go func() {
		logger.Infof("http listen at %s", webaddr)
		err := http.ListenAndServe(webaddr, nil) //start web before load
		if err != nil {
			logger.Fatalf(err.Error())
		}

	}()
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w,
		`
    <a href='/debug/pprof'> /debug/pprof </a> <p/>

     <hr/>

    <a href='/config'> /config </a> <p/>
    <a href='/requests'> /requests </a> <p/>
    <a href='/buffers'> /buffers </a> <p/>
    <a href='/memstats'> /memstats </a> <p/>
    <a href='/rusage'> /rusage </a> <p/>
    <a href='/logbuf'> /logbuf </a> <p/>
    <a href='/logbufall'> /logbufall </a> <p/>
    <a href='/loglast'> /loglast </a> <p/>
    <a href='/buckets'> /buckets </a> <p/>

    <hr/>

    <a href='/collision'> /collision/{16-byte-len hex keyhash} </a> <p/>
    <a href='/hash'> /hash/{hex bucket id} </a> <p/>

    `)
}

func handleWebPanic(w http.ResponseWriter) {
	r := recover()
	if r != nil {
		stack := utils.GetStack(1000)
		logger.Errorf("web req panic:%#v, stack:%s", r, stack)
		fmt.Fprintf(w, "\npanic:%#v, stack:%s", r, stack)
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
	handleJson(w, conf)
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
	defer handleWebPanic(w)
	handleJson(w, &cmem.DBRL)
}

func handleCollision(w http.ResponseWriter, r *http.Request) {
	if storage == nil {
		return
	}
	defer handleWebPanic(w)
	e := []byte("need bucket id, e.g. /collision/c")
	s := filepath.Base(r.URL.Path)
	bucketID, err := strconv.ParseInt(s, 16, 16)
	if err != nil {
		w.Write(e)
		return
	}
	if bucketID > int64(conf.NumBucket) || bucketID < 0 {
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

func handleKeyhash(w http.ResponseWriter, r *http.Request) {
	if storage == nil {
		return
	}
	defer handleWebPanic(w)
	path := filepath.Base(r.URL.Path)
	if len(path) != 16 {
		return
	}
	ki := &store.KeyInfo{StringKey: path, Key: []byte(path), KeyIsPath: true}
	rec, err := storage.hstore.GetRecordByKeyHash(ki)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	} else if rec == nil {
		return
	}
	arr := rec.Payload.CArray
	defer arr.Free()
	rec.Payload.Body = nil
	w.Write([]byte(fmt.Sprintf("%s \n", rec.Key)))
	w.Write([]byte(fmt.Sprintf("%#v", rec.Payload.Meta)))
}

func handleLogBuffer(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	loghub.Default.DumpBuffer(false, w)
}

func handleLogBufferALL(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	loghub.Default.DumpBuffer(true, w)
}

func handleLogLast(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	w.Write(loghub.Default.GetLast())
}
