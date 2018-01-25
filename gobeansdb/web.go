package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"

	yaml "gopkg.in/yaml.v2"

	"github.intra.douban.com/coresys/gobeansdb/cmem"
	"github.intra.douban.com/coresys/gobeansdb/config"
	"github.intra.douban.com/coresys/gobeansdb/loghub"
	mc "github.intra.douban.com/coresys/gobeansdb/memcache"
	"github.intra.douban.com/coresys/gobeansdb/store"
	"github.intra.douban.com/coresys/gobeansdb/utils"
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
	http.HandleFunc("/du", handleDU)

	// dir
	http.HandleFunc("/gc/", handleGC)
	http.HandleFunc("/bucket/", handleBucket)
	http.HandleFunc("/collision/", handleCollision)

	http.HandleFunc("/hash/", handleKeyhash)
	http.HandleFunc("/route/", handleRoute)
	http.HandleFunc("/route/version", handleRouteVersion)
	http.HandleFunc("/route/reload", handleReloadRoute)

	http.HandleFunc("/statgetset", handleStatGetSet)
	http.HandleFunc("/freememory", handleFreeMemory)

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

func checkStarting(w http.ResponseWriter) (starting bool) {
	starting = storage == nil
	if starting {
		w.Write([]byte("starting"))
	}
	return
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
    <a href='/du'> /du </a> <p/>
    <a href='/statgetset'> /statgetset </a> <p/>

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
		stack := utils.GetStack(2000)
		logger.Errorf("web req panic:%#v, stack:%s", r, stack)
		fmt.Fprintf(w, "\npanic:%#v, stack:%s", r, stack)
	}
}

func handleJson(w http.ResponseWriter, v interface{}) {
	defer handleWebPanic(w)
	b, err := json.Marshal(v)
	if err != nil {
		w.Write([]byte(err.Error()))
	} else {
		w.Write(b)
	}
}

func handleYaml(w http.ResponseWriter, v interface{}) {
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
	rusage := utils.Getrusage()
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
	if checkStarting(w) {
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

func showBucket(w http.ResponseWriter, path string) {
	for _, bkt := range conf.BucketsHex {
		line := fmt.Sprintf("<a href='/%s/%s'> %s </a> <p/>", path, bkt, bkt)
		w.Write([]byte(line))
	}
	return
}

func handleBucket(w http.ResponseWriter, r *http.Request) {
	if checkStarting(w) {
		return
	}
	defer handleWebPanic(w)
	var err error
	var bucketID int64
	s := filepath.Base(r.URL.Path)
	if s == "all" {
		all := make([]*store.BucketInfo, 0)
		for i, s := range conf.BucketsStat {
			if s > 0 {
				all = append(all, storage.hstore.GetBucketInfo(i))
			}
		}
		handleJson(w, all)
		return
	}
	bucketID, err = getBucket(r)
	if err != nil {
		w.Write([]byte("<a href='/bucket/all'> all </a> <p/>"))
		showBucket(w, "bucket")
		return
	}
	handleJson(w, storage.hstore.GetBucketInfo(int(bucketID)))
}

func handleStatGetSet(w http.ResponseWriter, r *http.Request) {
	if checkStarting(w) {
		return
	}
	defer handleWebPanic(w)
	res := make(map[int][]int64)
	counts := storage.hstore.GetNumCmdByBuckets()
	for i, count := range counts {
		res[i] = count
	}
	handleYaml(w, res)
	return
}

func handleFreeMemory(w http.ResponseWriter, r *http.Request) {
	if checkStarting(w) {
		return
	}
	defer handleWebPanic(w)
	// attempt to free memory immediately
	go debug.FreeOSMemory()
	return
}

func handleKeyhash(w http.ResponseWriter, r *http.Request) {
	if checkStarting(w) {
		return
	}
	defer handleWebPanic(w)
	path := filepath.Base(r.URL.Path)
	if len(path) != 16 {
		return
	}
	ki := &store.KeyInfo{StringKey: path, Key: []byte(path), KeyIsPath: true}
	rec, _, err := storage.hstore.GetRecordByKeyHash(ki)
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
	loghub.ErrorLogger.Hub.DumpBuffer(false, w)
}

func handleLogBufferALL(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	loghub.ErrorLogger.Hub.DumpBuffer(true, w)
}

func handleLogLast(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	w.Write(loghub.ErrorLogger.Hub.GetLastLog())
}

func handleGC(w http.ResponseWriter, r *http.Request) {
	if checkStarting(w) {
		return
	}
	defer handleWebPanic(w)
	var result string
	var err error
	var bucketID int64
	var pretend bool
	defer func() {
		if err != nil {
			e := fmt.Sprintf("<p> err : %s </p>", err.Error())
			w.Write([]byte(e))
			showBucket(w, "gc")
		} else {
			if !pretend {
				result2 := fmt.Sprintf(" <a href='/bucket/%d'> /bucket/%d </a> <p/>", bucketID, bucketID)
				result = result2 + result
			}
			w.Write([]byte(result))
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

	s = r.FormValue("merge")
	merge := (s == "true")

	start, end, err = storage.hstore.GC(int(bucketID), start, end, noGCDays, merge, pretend)
	if err == nil {
		result = fmt.Sprintf("<p/> bucket %d, start %d, end %d, merge %v, pretend %v <p/>",
			bucketID, start, end, merge, pretend)
	}
}

func handleDU(w http.ResponseWriter, r *http.Request) {
	if checkStarting(w) {
		return
	}
	defer handleWebPanic(w)
	handleJson(w, storage.hstore.GetDU())
}

func handleRoute(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	handleYaml(w, config.Route)
}

func handleRouteVersion(w http.ResponseWriter, r *http.Request) {
	defer handleWebPanic(w)
	if len(conf.ZKServers) == 0 {
		w.Write([]byte("-1"))
		return
	} else {
		w.Write([]byte(strconv.Itoa(config.ZKClient.Version)))
	}
}

func handleReloadRoute(w http.ResponseWriter, r *http.Request) {
	var err error
	if !config.AllowReload {
		w.Write([]byte("err: reloading"))
		return
	}
	config.AllowReload = false
	defer func() {
		config.AllowReload = true
		if err != nil {
			logger.Errorf("handleRoute err: %s", err.Error())
			w.Write([]byte(fmt.Sprintf(err.Error())))
			return
		}
	}()

	if checkStarting(w) {
		return
	}
	if len(conf.ZKServers) == 0 {
		w.Write([]byte("err: not using zookeeper"))
		return
	}
	defer handleWebPanic(w)

	r.ParseForm()
	ver, err := getFormValueInt(r, "ver", -1)
	if err != nil {
		return
	}

	newRouteContent, ver, err := config.ZKClient.GetRouteRaw(ver)
	if ver == config.ZKClient.Version {
		w.Write([]byte(fmt.Sprintf("warn: same version %d", ver)))
		return
	}

	info := fmt.Sprintf("update with route version %d\n", ver)
	logger.Infof(info)
	newRoute := new(config.RouteTable)
	err = newRoute.LoadFromYaml(newRouteContent)
	if err != nil {
		return
	}
	dbRouteConfig := newRoute.GetDBRouteConfig(config.ServerConf.Addr())
	loaded, unloaded, err := storage.hstore.ChangeRoute(dbRouteConfig)
	if err != nil {
		w.Write([]byte(fmt.Sprintf("err: %v", err)))
		logger.Infof("fail to reload: %v", err)
		return
	}
	info = fmt.Sprintf("ok: loaded:%v, unloaded:%v", loaded, unloaded)
	w.Write([]byte(info))
	store.Conf.DBRouteConfig = dbRouteConfig
	config.Route = *newRoute
	config.ZKClient.Version = ver
}
