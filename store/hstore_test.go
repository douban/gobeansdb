package store

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/douban/gobeansdb/cmem"
	"github.com/douban/gobeansdb/config"
	"github.com/douban/gobeansdb/utils"
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

func setupTest(casename string) {
	Conf.InitDefault()
	// dir = time.Now().Format("20060102T030405")
	dir = fmt.Sprintf("%s/%s", *tBase, casename)
	logger.Infof("test in %s", dir)
	os.RemoveAll(dir)
	os.Mkdir(dir, 0777)
	Conf.Home = dir
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
			TS:  uint32(i + 1),
			Ver: 0},
	}
	payload.Body = []byte(value)

	return
}

func GetFunctionName(i interface{}) string {
	name := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	parts := strings.Split(name, ".")
	return parts[len(parts)-1]
}

type KeyHasherMaker func(depth uint, bucket int) HashFuncType

func makeKeyHasherFixBucet(depth uint, bucket int) HashFuncType {
	return func(key []byte) uint64 {
		shift := depth * 4
		return (getKeyHashDefalut(key) >> shift) | (uint64(bucket) << (64 - shift))
	}
}

func makeKeyHasherParseKey(depth uint, bucket int) HashFuncType {
	return func(key []byte) uint64 {
		s := string(key)
		parts := strings.Split(s, "_")
		bkt, _ := strconv.ParseUint(parts[1], 16, 32)
		hash, _ := strconv.ParseUint(parts[2], 16, 32)
		h := (bkt << (4 * (16 - depth))) + hash
		return h
	}
}

func makeKeyHasherTrival(depth uint, bucket int) HashFuncType {
	return func(key []byte) uint64 {
		shift := depth * 4
		return (uint64(bucket) << (64 - shift))
	}
}

func TestHStoreMem0(t *testing.T) {
	testHStore(t, 0, 1, makeKeyHasherParseKey)
}

func TestHStoreFlush0(t *testing.T) {
	testHStore(t, 1, 1, makeKeyHasherParseKey)
}

func TestHStoreRestart0(t *testing.T) {
	testHStore(t, 2, 1, makeKeyHasherParseKey)
}

func TestHStoreMem1(t *testing.T) {
	testHStore(t, 2, 16, makeKeyHasherParseKey)
}

func TestHStoreFlush1(t *testing.T) {
	testHStore(t, 2, 16, makeKeyHasherParseKey)
}

func TestHStoreRestart1(t *testing.T) {
	testHStore(t, 2, 16, makeKeyHasherParseKey)
}

func testHStore(t *testing.T, op, numbucket int, hashMaker KeyHasherMaker) {
	Conf.InitDefault()
	funcname := GetFunctionName(hashMaker)
	home := fmt.Sprintf("testHStore_%d_%d_%v", op, numbucket, funcname)
	setupTest(home)
	defer clearTest()

	bkt := numbucket - 1
	Conf.NumBucket = numbucket
	Conf.BucketsStat = make([]int, numbucket)
	Conf.BucketsStat[bkt] = 1
	Conf.TreeHeight = 3
	Conf.TreeDump = 3
	Conf.Init()

	bucketDir := GetBucketPath(bkt)
	logger.Infof("xxx %s", bucketDir)
	os.Mkdir(bucketDir, 0777)

	gen := newKVGen(numbucket)
	getKeyHash = hashMaker(gen.depth, bkt)
	defer func() {
		getKeyHash = getKeyHashDefalut
	}()

	store, err := NewHStore()
	if err != nil {
		t.Fatal(err)
	}
	logger.Infof("%#v", Conf)
	// set

	N := 10
	var ki KeyInfo
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 0)
		logger.Infof("%v %v %#v %s", ki.StringKey, ki.KeyHash, payload.Meta, payload.Body)

		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
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
				t.Fatalf("chunk %d wrong mem size %d != 0", i, ck.size)
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
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 1)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	store.flushdatas(true)

	payload := gen.gen(&ki, -1, 0) // rotate
	cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
	if err := store.Set(&ki, payload); err != nil {
		t.Fatal(err)
	}
	store.flushdatas(true)
	bkt := store.buckets[bucketID]
	store.gcMgr.gc(bkt, 0, 0, true)

	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 1)
		payload2, pos, err := store.Get(&ki, false)
		if err != nil {
			t.Fatal(err)
		}
		if payload2 != nil {
			cmem.DBRL.GetData.SubSizeAndCount(payload2.CArray.Cap)
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
	dir.Set("nextgc.txt", 1)
	dir.Set("collision.yaml", -1)
	checkFiles(t, bkt.Home, dir)

	treeID := HintID{1, 0}
	if bkt.hints.maxDumpedHintID != treeID {
		t.Fatalf("bad treeID %v %v", bkt.TreeID, bkt.hints.maxDumpedHintID)
	}
}

func testGCNothing(t *testing.T, store *HStore, bucketID, numRecPerFile int) {
	gen := newKVGen(16)

	var ki KeyInfo
	N := numRecPerFile
	logger.Infof("test gc all updated in the same file")
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 0)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	store.flushdatas(true)
	payload := gen.gen(&ki, -1, 0) // rotate
	cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
	if err := store.Set(&ki, payload); err != nil {
		t.Fatal(err)
	}
	store.flushdatas(true)

	bkt := store.buckets[bucketID]
	store.gcMgr.gc(bkt, 0, 0, true)
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 0)
		payload2, pos, err := store.Get(&ki, false)
		if err != nil {
			t.Fatal(err)
		}
		if payload2 != nil {
			cmem.DBRL.GetData.SubSizeAndCount(payload2.CArray.Cap)
		}
		if !(payload2 != nil && len(payload2.Body) != 0 && payload2.Ver == 1 &&
			payload2.TS == uint32(i+1) && pos == Position{0, uint32(PADDING * (i))}) {
			if payload2 != nil {
				t.Errorf("%d: exp %s, got %#v", i, string(payload.Body), string(payload2.Body))
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
	dir.Set("nextgc.txt", 1)
	dir.Set("collision.yaml", -1)
	checkFiles(t, bkt.Home, dir)

	treeID := HintID{1, 0}
	if bkt.hints.maxDumpedHintID != treeID {
		t.Fatalf("bad treeID %v %v", bkt.TreeID, bkt.hints.maxDumpedHintID)
	}
}

func testGCAfterRebuildHTree(t *testing.T, store *HStore, bucketID, numRecPerFile int) {
	config.MCConf.BodyMax = 512
	defer func() {
		config.MCConf.BodyMax = 50 << 20
	}()
	gen := newKVGen(16)

	var ki KeyInfo

	N := numRecPerFile / 2
	Conf.DataFileMax = 256 * int64(N)
	logger.Infof("test gc numRecPerFile = %d", numRecPerFile)

	// 000.data
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 0)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	store.Close()
	logger.Infof("closed")

	store, err := NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}

	// 001.data
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 1)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	// 002.data
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 2)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}

	store.flushdatas(true)

	// 003.data delete key
	tsShift := 8
	for i := 0; i < N; i++ {
		gen.gen(&ki, i, 4)
		p := GetPayloadForDelete()
		p.TS = uint32(i + tsShift)
		if err := store.Set(&ki, p); err != nil {
			t.Fatal(err)
		}
	}
	// 004.data
	for i := 0; i < N/2; i++ {
		payload := gen.gen(&ki, i, 3)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}

	store.Close()
	store, err = NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}
	store.flushdatas(true)
	bkt := store.buckets[bucketID]

	n := N * 256
	dir := utils.NewDir()
	dir.Set("000.data", int64(n))
	dir.Set("000.000.idx.s", -1)
	dir.Set("001.data", int64(n))
	dir.Set("001.000.idx.s", -1)
	dir.Set("002.data", int64(n))
	dir.Set("002.000.idx.s", -1)
	dir.Set("003.data", int64(n))
	dir.Set("003.000.idx.s", -1)
	dir.Set("004.data", int64(n/2))
	dir.Set("004.000.idx.s", -1)
	dir.Set("004.000.idx.hash", -1)
	dir.Set("collision.yaml", -1)
	checkFiles(t, bkt.Home, dir)

	gen.gen(&ki, N-4, 2)

	payload2, _, err := store.Get(&ki, false)
	if err != nil {
		t.Fatal(err)
	}
	if payload2 == nil {
		t.Fatalf("nil")
	}
	if payload2.Ver != -4 {
		t.Fatalf("bad ver %#v", payload2)
	}
	store.Close()

	// remove the hash tree
	utils.Remove(bkt.Home + "/004.000.idx.hash")
	utils.Remove(bkt.Home + "/004.000.idx.hash")
	store, err = NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}

	// GC
	bkt = store.buckets[bucketID]
	store.gcMgr.gc(bkt, 1, 3, true)
	dir = utils.NewDir()
	dir.Set("000.data", int64(n))
	dir.Set("000.000.idx.s", -1)
	//dir.Set("000.001.idx.s", -1)
	dir.Set("001.data", int64(n/2))
	dir.Set("001.000.idx.s", -1)
	dir.Set("004.data", int64(n/2))
	dir.Set("004.000.idx.s", -1)
	dir.Set("nextgc.txt", -1)
	dir.Set("collision.yaml", -1)
	checkFiles(t, bkt.Home, dir)

	store.Close()
	// check the fils
	dir = utils.NewDir()
	dir.Set("000.data", int64(n))
	dir.Set("000.000.idx.s", -1)
	//dir.Set("000.001.idx.s", -1)
	dir.Set("001.data", int64(n/2))
	dir.Set("001.000.idx.s", -1)
	dir.Set("004.000.idx.hash", -1)
	dir.Set("004.data", int64(n/2))
	dir.Set("004.000.idx.s", -1)
	dir.Set("nextgc.txt", -1)
	dir.Set("collision.yaml", -1)
	checkFiles(t, bkt.Home, dir)

	// remove the hash tree
	utils.Remove(bkt.Home + "/004.000.idx.hash")
	dir = utils.NewDir()
	dir.Set("000.data", int64(n))
	dir.Set("000.000.idx.s", -1)
	dir.Set("001.data", int64(n/2))
	dir.Set("001.000.idx.s", -1)
	dir.Set("004.data", int64(n/2))
	dir.Set("004.000.idx.s", -1)
	dir.Set("nextgc.txt", -1)
	dir.Set("collision.yaml", -1)
	checkFiles(t, bkt.Home, dir)

	store, err = NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}

	// GC begin with 0
	bkt = store.buckets[bucketID]
	store.gcMgr.gc(bkt, 0, 1, true)
	if store.gcMgr.stat.Begin > 0 {
		t.Fatalf("Begin 0")
	}
	store.Close()
	dir = utils.NewDir()
	// dir.Set("000.data", int64(n/2))
	// dir.Set("000.000.idx.s", -1)
	dir.Set("004.data", int64(n/2))
	dir.Set("004.000.idx.s", -1)
	dir.Set("004.000.idx.hash", -1)
	dir.Set("nextgc.txt", -1)
	dir.Set("collision.yaml", -1)
	checkFiles(t, bkt.Home, dir)
}

func testGCDeleteSame(t *testing.T, store *HStore, bucketID, numRecPerFile int) {
	gen := newKVGen(16)

	var ki KeyInfo
	N := numRecPerFile / 2
	logger.Infof("test gc all updated in the same file")
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 0)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	tsShift := 8
	for i := 0; i < N; i++ {
		gen.gen(&ki, i, 1)
		p := GetPayloadForDelete()
		p.TS = uint32(i + tsShift)
		if err := store.Set(&ki, p); err != nil {
			t.Fatal(err)
		}
	}
	store.flushdatas(true)
	payload := gen.gen(&ki, -1, 0) // rotate
	cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
	if err := store.Set(&ki, payload); err != nil {
		t.Fatal(err)
	}
	store.flushdatas(true)
	n := N * 256
	bkt := store.buckets[bucketID]
	dir := utils.NewDir()
	dir.Set("000.data", int64(n*2))
	//	dir.Set("000.000.idx.s", -1)
	dir.Set("001.data", 256)
	//dir.Set("001.000.idx.s", -1)
	//	dir.Set("nextgc.txt", 1)
	//	dir.Set("collision.yaml", -1)
	checkFiles(t, bkt.Home, dir)

	store.gcMgr.gc(bkt, 0, 0, true)
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 1)
		payload2, pos, err := store.Get(&ki, false)
		if err != nil {
			t.Fatal(err)
		}

		if payload2 != nil {
			cmem.DBRL.GetData.SubSizeAndCount(payload2.CArray.Cap)
		}
		if !(payload2 != nil && len(payload2.Body) == 0 && payload2.Ver == -2 &&
			payload2.TS == uint32(i+tsShift) && pos == Position{0, uint32(PADDING * (i))}) {
			if payload2 != nil {
				t.Errorf("%d: exp %s, got %#v", i, string(payload.Body), payload2.Body)
			}
			t.Fatalf("%d: %#v %#v", i, payload2.Meta, pos)
		}
	}
	checkDataSize(t, bkt.datas, []uint32{uint32(n), 256})
	dir = utils.NewDir()
	dir.Set("000.data", int64(n))
	dir.Set("000.000.idx.s", -1)
	dir.Set("001.data", 256)
	dir.Set("001.000.idx.s", -1)
	dir.Set("nextgc.txt", 1)
	dir.Set("collision.yaml", -1)
	checkFiles(t, bkt.Home, dir)

	treeID := HintID{1, 0}
	if bkt.hints.maxDumpedHintID != treeID {
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
		if payload2 == nil || payload2.Ver != 2 || (string(payload.Body) != string(payload2.Body)) {
			if payload2 != nil {
				t.Errorf("%d: exp %s, got %s %#v", i, string(payload.Body), string(payload2.Body), payload2.Meta)
			}
			t.Fatalf("%d: pos %#v", i, pos)
		}
		if payload2 != nil {
			cmem.DBRL.GetData.SubSizeAndCount(payload2.CArray.Cap)
		}
	}
}

func testGCMulti(t *testing.T, store *HStore, bucketID, numRecPerFile int) {
	config.MCConf.BodyMax = 512
	defer func() {
		config.MCConf.BodyMax = 50 << 20
	}()
	gen := newKVGen(16)

	Conf.DataFileMax = 256 * int64(numRecPerFile)

	var ki KeyInfo
	N := numRecPerFile

	// 000.data
	for i := 0; i < N/2; i++ {
		payload := gen.gen(&ki, i*(-1)-N, 0)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	store.Close()
	logger.Infof("closed")

	store, err := NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}

	// 001.data
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i*(-1)-N*2, 0)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	// 002.data
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 1)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}

	// 003.data
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 2)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	store.flushdatas(true)

	// 004.data
	payload := gen.gen(&ki, -1, 0) // rotate
	cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
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
			pos2 := Position{1, uint32(PADDING * (i + N/2))}
			if i >= N/2 {
				pos2 = Position{2, uint32(PADDING * (i - N/2))}
			}
			if payload2 == nil || payload2.Ver != 2 || (string(payload.Body) != string(payload2.Body)) || (stop && pos != pos2) {
				if payload2 != nil {
					t.Errorf("%d: exp %s, got %s %#v", i, string(payload.Body), string(payload2.Body), payload2.Meta)
				}
				t.Fatalf("%d: exp %#v got %#v", i, pos2, pos)
			}
			if payload2 != nil {
				cmem.DBRL.GetData.SubSizeAndCount(payload2.CArray.Cap)
			}
		}
	}

	go func() {
		for !stop {
			readHStore(t, store, N, 2)
		}
	}()

	// 000: [-n  ,  -3n/2)
	// 001: [-2n        ,         -3n)
	// 002: [0          ,           n)
	// 003: [0          ,           n]
	// 004: -1
	// gc =>
	// 000: [-n   , 3n/2) [-2n, -5n/2)
	// 001: [-5n/2,  -3n) [0  ,   n/2)
	// 002: [n/2  ,    n)
	// 004: -1

	store.gcMgr.gc(bkt, 1, 3, true)
	stop = true
	readfunc()

	n := 256 * numRecPerFile
	checkDataSize(t, bkt.datas, []uint32{uint32(n), uint32(n), uint32(n / 2), 0, 256})
	dir := utils.NewDir()
	dir.Set("000.data", int64(n))
	dir.Set("000.000.idx.s", -1)
	dir.Set("000.001.idx.s", -1)
	dir.Set("001.data", int64(n))
	dir.Set("001.000.idx.s", -1)
	dir.Set("002.data", int64(n/2))
	dir.Set("002.000.idx.s", -1)
	dir.Set("004.data", 256)
	dir.Set("004.000.idx.s", -1)
	// Last
	dir.Set("nextgc.txt", 1)
	dir.Set("collision.yaml", -1)
	checkFiles(t, bkt.Home, dir)

	treeID := HintID{4, 0}
	if bkt.hints.maxDumpedHintID != treeID {
		t.Fatalf("wrong treeID %v %v", bkt.TreeID, bkt.hints.maxDumpedHintID)
	}
	if bkt.hints.state != HintStateIdle {
		t.Fatalf("wrong bkt.hints.stat %v", bkt.hints.state)
	}

	store.Close()
	logger.Infof("closed")
	store, err = NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}
	bkt = store.buckets[bucketID]
	dir.Set("collision.yaml", -1)
	dir.Set("004.000.idx.hash", -1)
	checkFiles(t, bkt.Home, dir)
	readfunc()

	store.Close()
	logger.Infof("closed")
	utils.Remove(bkt.Home + "/004.000.idx.hash")
	store, err = NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}
	bkt = store.buckets[bucketID]
	checkFiles(t, bkt.Home, dir)
	readfunc()
}

func testGCToLast(t *testing.T, store *HStore, bucketID, numRecPerFile int) {
	config.MCConf.BodyMax = 512
	defer func() {
		config.MCConf.BodyMax = 50 << 20
	}()
	gen := newKVGen(16)

	var ki KeyInfo

	N := numRecPerFile / 2
	logger.Infof("test gc numRecPerFile = %d", numRecPerFile)

	payload := gen.gen(&ki, -1, 0) // rotate
	cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
	if err := store.Set(&ki, payload); err != nil {
		t.Fatal(err)
	}
	store.Close()
	logger.Infof("closed")

	store, err := NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}
	bkt := store.buckets[bucketID]
	tsShift := 1
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 0)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		payload := gen.gen(&ki, i, 1)
		cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
		if err := store.Set(&ki, payload); err != nil {
			t.Fatal(err)
		}
	}
	store.flushdatas(true)

	payload = gen.gen(&ki, -2, 0) // rotate
	cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
	if err := store.Set(&ki, payload); err != nil {
		t.Fatal(err)
	}
	store.flushdatas(true)
	store.gcMgr.gc(bkt, 1, 1, true)
	readfunc := func() {
		for i := 0; i < N; i++ {
			payload := gen.gen(&ki, i, 1)
			payload2, pos, err := store.Get(&ki, false)
			if err != nil {
				t.Fatal(err)
			}
			if payload2 != nil {
				cmem.DBRL.GetData.SubSizeAndCount(payload2.CArray.Cap)
			}
			if !(payload2 != nil && payload2.Ver == 2 &&
				payload2.TS == uint32(i+tsShift) && pos == Position{0, uint32(PADDING * (i + 1))}) {
				if payload2 != nil {
					t.Errorf("%d: exp %s, got %#v", i, string(payload.Body), string(payload2.Body))
				}
				t.Fatalf("%d: %#v %#v", i, payload2.Meta, pos)
			}
		}
	}
	readfunc()
	n := (N + 1) * 256
	checkDataSize(t, bkt.datas, []uint32{uint32(n), 0, 256})
	dir := utils.NewDir()
	dir.Set("000.data", int64(n))
	dir.Set("000.000.idx.s", -1)
	dir.Set("000.001.idx.s", -1)
	dir.Set("002.data", 256)
	dir.Set("002.000.idx.s", -1)
	dir.Set("nextgc.txt", 1)
	dir.Set("collision.yaml", -1)
	dir.Set("002.000.idx.hash", -1)

	treeID := HintID{2, 0}
	if bkt.hints.maxDumpedHintID != treeID {
		t.Fatalf("bad treeID %v %v", bkt.TreeID, bkt.hints.maxDumpedHintID)
	}

	store.Close()
	logger.Infof("closed")
	utils.Remove(bkt.Home + "/002.000.idx.hash")
	store, err = NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}
	bkt = store.buckets[bucketID]
	checkFiles(t, bkt.Home, dir)
	readfunc()
}

func TestGCMultiBigBuffer(t *testing.T) {
	testGC(t, testGCMulti, "multiBig", 10000)
}

func TestGCAfterRebuildHtree(t *testing.T) {
	testGC(t, testGCAfterRebuildHTree, "gc build htree", 1000)
}

func TestGCMultiSmallBuffer(t *testing.T) {
	GCWriteBufferSize = 256
	defer func() {
		GCWriteBufferSize = GCWriteBufferSizeDefault
	}()
	testGC(t, testGCMulti, "multiSmalll", 1000)
}

func TestGCNothing(t *testing.T) {
	testGC(t, testGCNothing, "nothing", 100)
}

func TestGCToLast(t *testing.T) {
	testGC(t, testGCToLast, "tolast", 100)
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

	setupTest(fmt.Sprintf("testGC_%s", name))
	defer clearTest()

	numbucket := 16
	bkt := numbucket - 1
	Conf.NumBucket = numbucket
	Conf.BucketsStat = make([]int, numbucket)
	Conf.BucketsStat[bkt] = 1
	Conf.TreeHeight = 3
	getKeyHash = makeKeyHasherFixBucet(1, bkt)
	defer func() {
		getKeyHash = getKeyHashDefalut
	}()

	Conf.DataFileMaxStr = strconv.Itoa(int(256 * uint32(numRecPerFile)))

	Conf.Init()

	bucketDir := filepath.Join(Conf.Home, "f") // will be removed
	os.Mkdir(bucketDir, 0777)

	store, err := NewHStore()
	if err != nil {
		t.Fatal(err)
	}
	logger.Infof("%#v", Conf)
	casefunc(t, store, bkt, numRecPerFile)

	if !cmem.DBRL.IsZero() {
		t.Fatalf("%#v", cmem.DBRL)
	}

	store.Close()
	checkAllDataWithHints(bucketDir)
}

func checkDataWithHints(dir string, chunk int) error {
	dpath := "%s/%03d.data"
	_, err := os.Stat(dpath)
	hpat := "%s/%03d.*"
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
				dm[string(rec.Key)] = &HintItemMeta{getKeyHash(rec.Key), Position{0, offset}, rec.Payload.Ver, rec.Payload.ValueHash}
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

func testGCReserveDelete(t *testing.T, store *HStore, bucketID, numRecPerFile int) {
	gen := newKVGen(16)
	var ki KeyInfo
	logger.Infof("test gc all updated in the same file")

	payload := gen.gen(&ki, 0, 0)
	cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
	if err := store.Set(&ki, payload); err != nil {
		t.Fatal(err)
	}

	store.Close()
	store, err := NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}

	p := GetPayloadForDelete()
	if err := store.Set(&ki, p); err != nil {
		t.Fatal(err)
	}

	store.Close()
	store, err = NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}

	payload = gen.gen(&ki, 1, 0) // rotate
	cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
	if err = store.Set(&ki, payload); err != nil {
		t.Fatal(err)
	}
	store.flushdatas(true)

	bkt := store.buckets[bucketID]
	store.gcMgr.gc(bkt, 1, 1, true)

	gen.gen(&ki, 0, 0)
	payload2, _, err := store.Get(&ki, false)
	if err != nil {
		t.Fatal(err)
	}
	if payload2 == nil {
		t.Fatalf("nil")
	}
	if payload2.Ver != -2 {
		t.Fatalf("bad ver %#v", payload2)
	}
	cmem.DBRL.GetData.SubSizeAndCount(payload2.CArray.Cap)

	store.Close()
	utils.Remove(bkt.Home + "/002.000.idx.hash")
	store, err = NewHStore()
	if err != nil {
		t.Fatalf("%v", err)
	}

	payload2, _, err = store.Get(&ki, false)
	if err != nil {
		t.Fatal(err)
	}
	if payload2 != nil {
		t.Fatalf("nil %#v", payload2)
	}

	checkDataSize(t, bkt.datas, []uint32{256, 256, 256})
	dir := utils.NewDir()
	dir.Set("000.data", 256)
	dir.Set("000.000.idx.s", -1)
	dir.Set("001.data", 256)
	dir.Set("001.000.idx.s", -1)
	dir.Set("002.data", 256)
	dir.Set("002.000.idx.s", -1)
	dir.Set("nextgc.txt", 1)
	dir.Set("collision.yaml", -1)
	dir.Set("002.000.idx.hash", -1)

	checkFiles(t, bkt.Home, dir)
}

func TestGCReserveDelete(t *testing.T) {
	testGC(t, testGCReserveDelete, "reserve_deletes", 100)
}

func TestHStoreCollision(t *testing.T) {
	testHStore(t, 0, 16, makeKeyHasherTrival)
	testHStore(t, 1, 16, makeKeyHasherTrival)
	testHStore(t, 2, 16, makeKeyHasherTrival)
}

func TestChunk256(t *testing.T) {
	Conf.InitDefault()
	setupTest("TestChunk256")
	defer clearTest()

	numbucket := 16
	bucketID := numbucket - 1
	Conf.NumBucket = numbucket
	Conf.BucketsStat = make([]int, numbucket)
	Conf.BucketsStat[bucketID] = 1
	Conf.TreeHeight = 3
	Conf.Init()

	bucketDir := filepath.Join(Conf.Home, "0") // will be removed
	os.Mkdir(bucketDir, 0777)

	gen := newKVGen(numbucket)
	getKeyHash = makeKeyHasherParseKey(gen.depth, bucketID)
	defer func() {
		getKeyHash = getKeyHashDefalut
	}()

	store, err := NewHStore()
	if err != nil {
		t.Fatal(err)
	}
	logger.Infof("%#v", Conf)
	// set
	N := 300
	var ki KeyInfo
	for k := 0; k < 2; k++ {
		for i := 0; i < N-k; i++ {
			payload := gen.gen(&ki, i, 0)
			if k == 0 {

				logger.Infof("%v %v %#v %s", ki.StringKey, ki.KeyHash, payload.Meta, payload.Body)

				cmem.DBRL.SetData.AddSizeAndCount(payload.CArray.Cap)
				if err := store.Set(&ki, payload); err != nil {
					t.Fatal(err)
				}
				store.Close()

				store, err = NewHStore()
				if err != nil {
					t.Fatalf("%v", err)
				}
			}

			payload2, pos, err := store.Get(&ki, false)
			if err != nil {
				t.Fatal(err)
			}
			pos2 := Position{i, uint32(0)}
			if k == 1 {
				pos2 = Position{0, uint32(i) * 256}
			}

			if payload2 == nil || (string(payload.Body) != string(payload2.Body)) || (pos != pos2) {
				if payload2 != nil {
					t.Errorf("%d %d: exp %s, got %s", k, i, string(payload.Body), string(payload2.Body))
				}
				t.Fatalf("%d %d: %#v %#v", k, i, payload2.Meta, pos)
			}
		}
		bkt := store.buckets[bucketID]
		store.gcMgr.gc(bkt, 0, N-1, true)
	}
	store.Close()
}
