package store

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var (
	// common
	tBase     = flag.String("base", "/tmp/test_gobeansdb", "base dir of test")
	tNotClear = flag.Bool("notclear", false, "dump meta and key when Testcompatibility")

	// HTree config
	tDepth    = flag.Int("depth", 0, "")
	tHeigth   = flag.Int("height", 4, "heigh of HTree (a single root is 1)")
	tLeafSize = flag.Int("leafsize", 30, "#item of a leaf")
	tPos      = flag.String("pos", "0", "hexString, e.g. ff")

	// profile
	tKeysPerGC = flag.Int("gckeys", 0, "gc per # keys added")
	tPort      = flag.Int("port", 0, "http port for pprof")
	tDoProf    = flag.Bool("prof", false, "do cpu prof for each bench")
	tParallel  = flag.Int("parallel", 0, "do parallel set")

	// input data
	tDataDir  = flag.String("datadir", "", "directory in which to load data files for test")
	tDataFile = flag.String("data", "", "path of datafile to Testcompatibility")

	// verbosity
	tDumpRecord = flag.Bool("dumprecord", false, "dump meta and key when Testcompatibility")
)

var (
	dir           string
	recordPerFile = 6 // even numb
)

func init() {
	os.MkdirAll(*tBase, 0777)
	doProf = *tDoProf
}

func setupTest(casename string, numhome int) {
	config = HStoreConfig{}
	// dir = time.Now().Format("20060102T030405")
	dir = fmt.Sprintf("%s/%s", *tBase, casename)
	logger.Infof("test in %s", dir)
	os.RemoveAll(dir)
	os.Mkdir(dir, 0777)
	for i := 0; i < numhome; i++ {
		home := fmt.Sprintf("%s/home_%d", dir, i)
		os.Mkdir(home, 0777)
		config.Homes = append(config.Homes, home)
	}
}
func clearTest() {
	if *tNotClear {
		return
	}
	os.RemoveAll(dir)
}

type kvgen struct{}

func (g *kvgen) gen(i int) (keyhash uint64, key string, payload *Payload) {
	keyhash = uint64(i)
	key = fmt.Sprintf("key_%d", i)
	value := fmt.Sprintf("value_%d", i)
	payload = &Payload{Meta: Meta{TS: uint32(i)}, Value: []byte(value)}
	return
}

func TestHStoreEmpty(t *testing.T) {
	gen := kvgen{}
	setupTest("HStoreEmpty", 1)
	defer clearTest()

	initDefaultConfig()
	config.NumBucket = 16
	config.Buckets = make([]int, 16)
	config.Buckets[0] = 1
	config.TreeHeight = 3
	config.init()

	bucketDir := filepath.Join(config.Homes[0], "0") // TODO: auto create?
	os.Mkdir(bucketDir, 0777)

	store, err := NewHStore()
	if err != nil {
		t.Fatal(err)
	}

	// set

	N := 10
	for i := 0; i < N; i++ {
		keyhash, key, payload := gen.gen(i)
		if err := store.Set([]byte(key), keyhash, payload); err != nil {
			t.Fatal(err)
		}
	}

	// get
	for i := 0; i < N; i++ {
		keyhash, key, payload := gen.gen(i)
		payload2, pos, err := store.Get([]byte(key), keyhash, false)
		if err != nil {
			t.Fatal(err)
		}
		if payload2 == nil || (string(payload.Value) != string(payload2.Value)) || (pos != Position{0, uint32(PADDING * i)}) {
			t.Fatalf("%d: %#v %#v", i, payload2, pos)
		}
	}
}
