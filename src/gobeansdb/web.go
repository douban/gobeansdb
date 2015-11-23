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

	http.HandleFunc("/reload", handleReload)
	http.HandleFunc("/logbuf", handleLogBuffer)
	http.HandleFunc("/logbufall", handleLogBufferALL)
	http.HandleFunc("/loglast", handleLogLast)

	// dir
	http.HandleFunc("/gc/", handleGC)
	http.HandleFunc("/bucket/", handleBucket)
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

    <hr/>

    <a href='/bucket'> /bucket/{hex bucket id} </a> <p/>
    <a href='/collision'> /collision/{16-byte-len hex keyhash} </a> <p/>
    <a href='/hash'> /hash/{hex bucket id} </a> <p/>

    `)
}

// tools
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

func getBucket(r *http.Request) (bucketID int64, err error) {
	s := filepath.Base(r.URL.Path)
	return strconv.ParseInt(s, 16, 16)
}

func getFormValueInt(r *http.Request, name string, ndefault int) (n int, err error) {
	n = ndefault
	s := r.FormValue(name)
	if s != "" {
		n, err = strconv.Atoi(s)
	}
	return
}

// handlers

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
	bucketID, err := getBucket(r)
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

func handleBucket(w http.ResponseWriter, r *http.Request) {
	if storage == nil {
		return
	}
	defer handleWebPanic(w)
	var err error
	var bucketID int64
	bucketID, err = getBucket(r)
	if err != nil {
		return
	}
	handleJson(w, storage.hstore.GetBucketInfo(int(bucketID)))
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

func handleGC(w http.ResponseWriter, r *http.Request) {
	if storage == nil {
		return
	}
	defer handleWebPanic(w)
	var err error
	var bucketID int64
	var pretend bool
	defer func() {
		if err != nil {
			w.Write([]byte(err.Error()))
		} else if !pretend {
			resp := fmt.Sprintf("ok, goto /bucket/%d", bucketID)
			w.Write([]byte(resp))
		}
	}()

	bucketID, err = getBucket(r)
	if err != nil {
		return
	}
	r.ParseForm()
	start, err := getFormValueInt(r, "start", -1)
	if err != nil {
		return
	}
	end, err := getFormValueInt(r, "end", -1)
	if err != nil {
		return
	}
	noGCDays, err := getFormValueInt(r, "nogcdays", -1)
	if err != nil {
		return
	}

	s := r.FormValue("run")
	pretend = (s != "true")
	start, end, err = storage.hstore.GC(int(bucketID), start, end, noGCDays, pretend)
	if err == nil && pretend {
		w.Write([]byte(fmt.Sprintf("pretend bucket %d, start %d, end %d", bucketID, start, end)))
	}
}
