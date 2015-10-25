package store

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var (
	// common
	tBase     = flag.String("base", "/tmp/test_gobeansdb", "base dir of test")
	tNotClear = flag.Bool("notclear", false, "dump meta and key when Testcompatibility")

	// HTree config in TestLoadHints
	tNumbucket = flag.Int("buckets", 16, "#bucket : 1, 16, 256")
	tHeigth    = flag.Int("height", 4, "heigh of HTree (a single root is 1)")
	tLeafSize  = flag.Int("leafsize", 30, "#item of a leaf")
	tPos       = flag.String("pos", "0", "hexString, e.g. ff")

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
	initDefaultConfig()
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

type KVGen struct {
	numbucket int
	depth     uint
	bucketId  int
}

func newKVGen(numbucket int) *KVGen {
	gen := &KVGen{numbucket: numbucket}

	d := uint(0)
	b := numbucket
	for {
		b /= 16
		if b > 0 {
			d++
		} else {
			break
		}
	}
	gen.depth = d

	getKeyHash = func(key []byte) uint64 {
		s := string(key)
		parts := strings.Split(s, "_")
		bkt, _ := strconv.ParseUint(parts[1], 16, 32)
		hash, _ := strconv.ParseUint(parts[2], 16, 32)
		h := (bkt << (4 * (16 - gen.depth))) + hash
		return h
	}
	return gen
}

func (g *KVGen) gen(ki *KeyInfo, i int) (payload *Payload) {
	ki.StringKey = fmt.Sprintf("key_%x_%x", g.numbucket-1, i)
	ki.Key = []byte(ki.StringKey)
	value := fmt.Sprintf("value_%x", i)
	payload = &Payload{
		Meta: Meta{
			TS:  uint32(i),
			Ver: 1},
		Value: []byte(value),
	}

	return
}
func (g *KVGen) close() {
	getKeyHash = getKeyHashDefalut
}

func TestHStoreMem(t *testing.T) {
	testHStore(t, 0, 1)
}

func TestHStoreFlush(t *testing.T) {
	testHStore(t, 1, 1)
}

func TestHStoreRestart0(t *testing.T) {
	testHStore(t, 2, 1)
}

func TestHStoreRestart1(t *testing.T) {
	testHStore(t, 2, 16)
}

func testHStore(t *testing.T, op, numbucket int) {
	initDefaultConfig()
	gen := newKVGen(numbucket)

	setupTest(fmt.Sprintf("testHStore_%d_%d", op, numbucket), 1)
	defer clearTest()
	config.NumBucket = numbucket
	config.Buckets = make([]int, numbucket)
	config.Buckets[numbucket-1] = 1
	config.TreeHeight = 3
	config.Init()
	defer gen.close()

	bucketDir := filepath.Join(config.Homes[0], "0") // will be removed
	os.Mkdir(bucketDir, 0777)

	store, err := NewHStore()
	if err != nil {
		t.Fatal(err)
	}

	// set

	N := 10
	var ki KeyInfo
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i)

		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	switch op {
	case 1:
		store.flushdatas(true)
	case 2:
		store.Close()
		logger.Infof("closed")
		store, err = NewHStore()
	}

	// get
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i)
		payload2, pos, err := store.Get(&ki, false)
		if err != nil {
			t.Fatal(err)
		}
		if payload2 == nil || (string(payload.Value) != string(payload2.Value)) || (pos != Position{0, uint32(PADDING * i)}) {
			t.Fatalf("%d: %#v %#v", i, payload2, pos)
		}
	}
}
