package store

import (
	"cmem"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"utils"
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
	conf.InitDefault()
	// dir = time.Now().Format("20060102T030405")
	dir = fmt.Sprintf("%s/%s", *tBase, casename)
	logger.Infof("test in %s", dir)
	os.RemoveAll(dir)
	os.Mkdir(dir, 0777)
	conf.Homes = nil
	for i := 0; i < numhome; i++ {
		home := fmt.Sprintf("%s/home_%d", dir, i)
		os.Mkdir(home, 0777)
		conf.Homes = append(conf.Homes, home)
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

	return gen
}

func (g *KVGen) gen(ki *KeyInfo, i, ver int) (payload *Payload) {
	ki.StringKey = fmt.Sprintf("key_%x_%x", g.numbucket-1, i)
	ki.Key = []byte(ki.StringKey)
	value := fmt.Sprintf("value_%x_%d", i, ver)
	payload = &Payload{
		Meta: Meta{
			TS:  uint32(i),
			Ver: 0},
	}
	payload.Body = []byte(value)

	return
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
	conf.InitDefault()

	setupTest(fmt.Sprintf("testHStore_%d_%d", op, numbucket), 1)
	defer clearTest()
	conf.NumBucket = numbucket
	conf.Buckets = make([]int, numbucket)
	conf.Buckets[numbucket-1] = 1
	conf.TreeHeight = 3
	conf.Init()

	bucketDir := filepath.Join(conf.Homes[0], "0") // will be removed
	os.Mkdir(bucketDir, 0777)

	gen := newKVGen(numbucket)
	getKeyHash = makeKeyHasherParse(gen.depth)
	defer func() {
		getKeyHash = getKeyHashDefalut
	}()

	store, err := NewHStore()
	if err != nil {
		t.Fatal(err)
	}
	logger.Infof("%#v", conf)
	// set

	N := 10
	var ki KeyInfo
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 0)

		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	logger.Infof("set done")
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
		payload := gen.gen(&ki, i, 0)
		payload2, pos, err := store.Get(&ki, false)
		if err != nil {
			t.Fatal(err)
		}
		if payload2 == nil || (string(payload.Body) != string(payload2.Body)) || (pos != Position{0, uint32(PADDING * i)}) {
			t.Fatalf("%d: %#v %#v", i, payload2, pos)
		}
	}
}

func makeKeyHasherFixBucet(bucket, depth uint) HashFuncType {
	return func(key []byte) uint64 {
		shift := depth * 4
		return (getKeyHashDefalut(key) >> shift) | (uint64(bucket) << (64 - shift))
	}
}

func makeKeyHasherParse(depth uint) HashFuncType {
	return func(key []byte) uint64 {
		s := string(key)
		parts := strings.Split(s, "_")
		bkt, _ := strconv.ParseUint(parts[1], 16, 32)
		hash, _ := strconv.ParseUint(parts[2], 16, 32)
		h := (bkt << (4 * (16 - depth))) + hash
		return h
	}
}

func checkDataSize(t *testing.T, ds *dataStore, sizes0 []uint32) {
	sizes := make([]uint32, 256)
	copy(sizes, sizes0)
	for i, sz := range sizes {
		ck := ds.chunks[i]
		if ck.size != sz {
			t.Fatalf("chunk %d wrong mem size %d != %d", i, ck.size, sz)
		}
		st, err := os.Stat(ck.path)
		if sz > 0 {
			if err != nil {
				t.Fatalf("chunk %d file not exist, should have size %d, err = %v", i, sz, err)
			}
			if st.Size() != int64(sz) {
				t.Fatalf("chunk %d wrong disk size %d != %d", i, st.Size(), sz)
			}
		} else {
			if ck.size != 0 {
				t.Fatalf("chunk %d wrong mem size %d != 0", i, ck.size, 0)
			}
			st, err := os.Stat(ck.path)
			if err == nil {
				t.Fatalf("chunk %d file should not exist, size %d", i, st.Size())
			}
		}
	}
}

func testGCUpdateSame(t *testing.T, store *HStore, bucketID, numRecPerFile int) {
	gen := newKVGen(16)

	var ki KeyInfo
	N := numRecPerFile / 2
	logger.Infof("test gc all updated in the same file")
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 0)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 1)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	store.flushdatas(true)

	payload := gen.gen(&ki, -1, 0) // rotate
	if err := store.Set(&ki, payload); err != nil {
		t.Fatal(err)
	}
	store.flushdatas(true)
	bkt := store.buckets[bucketID]
	store.gcMgr.gc(bkt, 0, 0)

	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 1)
		payload2, pos, err := store.Get(&ki, false)
		if err != nil {
			t.Fatal(err)
		}
		if payload2 != nil {
			cmem.DBRL.GetData.SubSize(payload2.AccountingSize)
		}
		if payload2 == nil || (string(payload.Body) != string(payload2.Body)) || (pos != Position{0, uint32(PADDING * (i))}) {
			if payload2 != nil {
				t.Errorf("%d: exp %s, got %s", i, string(payload.Body), string(payload2.Body))
			}
			t.Fatalf("%d: %#v %#v", i, payload2.Meta, pos)
		}
	}
	n := N * 256
	checkDataSize(t, bkt.datas, []uint32{uint32(n), 256})
	dir := utils.NewDir()
	dir.Set("000.data", int64(n))
	dir.Set("000.000.idx.s", -1)
	dir.Set("001.data", 256)
	dir.Set("001.000.idx.s", -1)
	dir.Set("001.000.idx.hash", -1)
	dir.Set("nextgc.txt", 1)
	checkFiles(t, bkt.Home, dir)

	treeID := HintID{1, 0}
	if bkt.TreeID != treeID || bkt.hints.maxDumpedHintID != treeID {
		t.Fatalf("bad treeID %v %v", bkt.TreeID, bkt.hints.maxDumpedHintID)
	}
}

func testGCDeleteSame(t *testing.T, store *HStore, bucketID, numRecPerFile int) {
	gen := newKVGen(16)

	var ki KeyInfo
	N := numRecPerFile / 2
	logger.Infof("test gc all updated in the same file")
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 0)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		gen.gen(&ki, i, 1)
		if err := store.Set(&ki, GetPayloadForDelete()); err != nil {
			t.Fatal(err)
		}
	}
	store.flushdatas(true)
	payload := gen.gen(&ki, -1, 0) // rotate
	if err := store.Set(&ki, payload); err != nil {
		t.Fatal(err)
	}
	store.flushdatas(true)
	bkt := store.buckets[bucketID]
	store.gcMgr.gc(bkt, 0, 0)
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 1)
		payload2, pos, err := store.Get(&ki, false)
		if err != nil {
			t.Fatal(err)
		}
		if payload2 != nil {
			cmem.DBRL.GetData.SubSize(payload2.AccountingSize)
		}
		if !(payload2 != nil && len(payload2.Body) == 0 && payload2.Ver == -2 &&
			payload2.TS != 0 && pos == Position{0, uint32(PADDING * (i))}) {
			if payload2 != nil {
				t.Errorf("%d: exp %s, got %#v", i, string(payload.Body), payload2.Body)
			}
			t.Fatalf("%d: %#v %#v", i, payload2.Meta, pos)
		}
	}
	n := N * 256
	checkDataSize(t, bkt.datas, []uint32{uint32(n), 256})
	dir := utils.NewDir()
	dir.Set("000.data", int64(n))
	dir.Set("000.000.idx.s", -1)
	dir.Set("001.data", 256)
	dir.Set("001.000.idx.s", -1)
	dir.Set("001.000.idx.hash", -1)
	dir.Set("nextgc.txt", 1)
	checkFiles(t, bkt.Home, dir)

	treeID := HintID{1, 0}
	if bkt.TreeID != treeID || bkt.hints.maxDumpedHintID != treeID {
		t.Fatalf("bad treeID %v %v", bkt.TreeID, bkt.hints.maxDumpedHintID)
	}
}

func readHStore(t *testing.T, store *HStore, n, v int) {
	gen := newKVGen(16)
	for i := 0; i < n; i++ {
		var ki KeyInfo
		payload := gen.gen(&ki, i, v)
		payload2, pos, err := store.Get(&ki, false)
		if err != nil {
			t.Fatal(err)
		}
		if payload2 == nil || payload2.Ver != 3 || (string(payload.Body) != string(payload2.Body)) {
			if payload2 != nil {
				t.Errorf("%d: exp %s, got %s", i, string(payload.Body), string(payload2.Body))
			}
			t.Fatalf("%d: %#v %#v", i, payload2.Meta, pos)
		}
		if payload2 != nil {
			cmem.DBRL.GetData.SubSize(payload2.AccountingSize)
		}
	}
}

func testGCMulti(t *testing.T, store *HStore, bucketID, numRecPerFile int) {
	gen := newKVGen(16)

	var ki KeyInfo
	N := numRecPerFile
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 0)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 1)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 2)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}

	store.flushdatas(true)

	payload := gen.gen(&ki, -1, 0) // rotate
	if err := store.Set(&ki, payload); err != nil {
		t.Fatal(err)
	}

	store.flushdatas(true)
	bkt := store.buckets[bucketID]

	stop := false
	readfunc := func() {
		for i := 0; i < N; i++ {
			payload := gen.gen(&ki, i, 2)
			payload2, pos, err := store.Get(&ki, false)
			if err != nil {
				t.Fatal(err)
			}
			if payload2 == nil || payload2.Ver != 3 || (string(payload.Body) != string(payload2.Body)) || (stop && pos != Position{0, uint32(PADDING * (i))}) {
				if payload2 != nil {
					t.Errorf("%d: exp %s, got %s", i, string(payload.Body), string(payload2.Body))
				}
				t.Fatalf("%d: %#v %#v", i, payload2.Meta, pos)
			}
			if payload2 != nil {
				cmem.DBRL.GetData.SubSize(payload2.AccountingSize)
			}
		}
	}

	go func() {
		for !stop {
			readHStore(t, store, N, 2)
		}
	}()
	store.gcMgr.gc(bkt, 0, 2)
	stop = true
	readfunc()

	n := 256 * numRecPerFile
	checkDataSize(t, bkt.datas, []uint32{uint32(n), 0, 0, 256})
	dir := utils.NewDir()
	dir.Set("000.data", int64(n))
	dir.Set("000.000.idx.s", -1)
	dir.Set("003.data", 256)
	dir.Set("003.000.idx.s", -1)
	dir.Set("003.000.idx.hash", -1)
	dir.Set("nextgc.txt", 1)
	checkFiles(t, bkt.Home, dir)

	treeID := HintID{3, 0}
	if bkt.TreeID != treeID || bkt.hints.maxDumpedHintID != treeID {
		t.Fatalf("bad treeID %v %v", bkt.TreeID, bkt.hints.maxDumpedHintID)
	}
}

func TestGCMulti(t *testing.T) {
	testGC(t, testGCMulti, "multi", 10000)
}

func TestGCMultiFlush(t *testing.T) {
	GCWriteBufferSize = 256
	defer func() {
		GCWriteBufferSize = GCWriteBufferSizeDefault
	}()
	testGC(t, testGCMulti, "multi", 1000)
}

func TestGCUpdateSame(t *testing.T) {
	testGC(t, testGCUpdateSame, "updateSame", 100)
}

func TestGCDeleteSame(t *testing.T) {
	testGC(t, testGCDeleteSame, "deleteSame", 100)
}

type testGCFunc func(t *testing.T, hstore *HStore, bucket, numRecPerFile int)

// numRecPerFile should be even
func testGC(t *testing.T, casefunc testGCFunc, name string, numRecPerFile int) {

	setupTest(fmt.Sprintf("testGC_%s", name), 1)
	defer clearTest()

	numbucket := 16
	bkt := numbucket - 1
	conf.NumBucket = numbucket
	conf.Buckets = make([]int, numbucket)
	conf.Buckets[bkt] = 1
	conf.TreeHeight = 3
	getKeyHash = makeKeyHasherFixBucet(uint(bkt), 1)
	defer func() {
		getKeyHash = getKeyHashDefalut
	}()

	conf.DataFileMaxStr = strconv.Itoa(int(256 * uint32(numRecPerFile)))

	conf.Init()

	bucketDir := filepath.Join(conf.Homes[0], "f") // will be removed
	os.Mkdir(bucketDir, 0777)

	store, err := NewHStore()
	if err != nil {
		t.Fatal(err)
	}
	logger.Infof("%#v", conf)
	casefunc(t, store, bkt, numRecPerFile)

	if !cmem.DBRL.IsZero() {
		t.Fatalf("%#v", cmem.DBRL)
	}

	store.Close()
	checkAllDataWithHints(bucketDir)
}

func checkDataWithHints(dir string, chunk int) error {
	dpath := fmt.Sprintf("%s/%03d.data")
	_, err := os.Stat(dpath)
	hpat := fmt.Sprintf("%s/%03d.*")
	hpaths, _ := filepath.Glob(hpat)
	if err != nil {
		if len(hpaths) > 0 {
			return fmt.Errorf("%v should not exist", hpaths)
		}
	} else {
		if len(hpaths) == 0 {
			return fmt.Errorf("%v has no hints", dpath)
		} else {
			dm := make(map[string]*HintItemMeta)
			ds, _ := newDataStreamReader(dpath, 1<<20)
			defer ds.Close()
			for {
				rec, offset, _, err := ds.Next()
				if err != nil {
					return err
				}
				if rec == nil {
					break
				}
				rec.Payload.Decompress()
				rec.Payload.CalcValueHash()
				dm[string(rec.Key)] = &HintItemMeta{getKeyHash(rec.Key), offset, rec.Payload.Ver, rec.Payload.ValueHash}
			}
			hm := make(map[string]*HintItemMeta)
			for _, hp := range hpaths {
				hs := newHintFileReader(hp, 0, 4<<10)
				err := hs.open()
				if err != nil {
					return err
				}
				for {
					it, err := hs.next()
					if err != nil {
						return err
					}
					if it == nil {
						break
					}
					hm[it.Key] = &it.HintItemMeta
				}
			}
			for k, v := range dm {
				it, ok := hm[k]
				if !ok {
					return fmt.Errorf("data key %s not in hint", k)
				}
				if *it != *v {
					return fmt.Errorf("key %s diff: %#v != %#v", k, it, v)
				}
				delete(hm, k)
			}
			n := len(hm)
			if n > 0 {
				for k, _ := range dm {
					return fmt.Errorf("%d hint key not in data, one of them: %s", n, k)
				}
			}
		}
	}
	return nil
}

func checkAllDataWithHints(dir string) error {
	for i := 0; i < 256; i++ {
		err := checkDataWithHints(dir, i)
		if err != nil {
			return err
		}
	}
	return nil
}
