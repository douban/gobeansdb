package store

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/douban/gobeansdb/utils"
)

func TestHash(t *testing.T) {
	h := utils.Fnv1a([]byte("test"))
	if h != uint32(2949673445) {
		t.Error("hash error", h)
	}
}

func TestParse(t *testing.T) {
	var u uint64 = 0x89abcdef01234567
	path0 := []int{8, 9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0, 1, 2, 3, 4, 5, 6, 7}
	var pathbuf [16]int
	path := ParsePathUint64(u, pathbuf[:16])
	if !isSamePath(path, path0, 8) {
		t.Errorf("parse %016x error: %v != %v", u, path, path0)
	}
	s := "89abcdef"
	for i := 0; i < 8; i++ {
		path, _ = ParsePathString(s[:i], pathbuf[:16])
		if !isSamePath(path, path0[:i], i) {
			t.Errorf("parse %d, error: %v != %v", i, path, path0)
		}
	}
}

func TestHTree(t *testing.T) {

	pos := 0xfe
	for h := 2; h <= 6; h++ {
		Conf.InitDefault()
		Conf.NumBucket = 256
		Conf.TreeHeight = h
		Conf.Init()
		testHTree(t, 1, pos)
	}

	pos = 0xf
	for h := 2; h <= 7; h++ {
		Conf.InitDefault()
		Conf.NumBucket = 16
		Conf.TreeHeight = h
		Conf.Init()
		testHTree(t, 2, pos)
	}
}

func testHTree(t *testing.T, seq, treepos int) {
	defer func() {
		thresholdListKey = ThresholdListKeyDefault
	}()

	t.Logf("testing height %d %x %d", Conf.TreeDepth, treepos, Conf.TreeHeight)
	depth := Conf.TreeDepth
	height := Conf.TreeHeight
	tree := newHTree(depth, treepos, height)
	N := int(thresholdListKey)
	dstNode := &tree.levels[height-1][0]

	keyhash := uint64(treepos << uint32(64-4*depth))
	ki := NewKeyInfoFromBytes([]byte("key"), keyhash, false)
	var meta Meta
	var pos Position

	reset := func() {
		ki.KeyHash = keyhash
		pos = Position{0, 0}
		meta = Meta{Ver: 1}
	}

	// set
	reset()
	for i := 0; i < N; i++ {
		ki.Prepare()
		tree.set(ki, &meta, pos)
		count := int(dstNode.count)
		if count != i+1 {
			t.Fatalf("wrong count %d != %d", count, i+1)
		}
		ki.KeyHash++
		pos.Offset += PADDING
	}

	reset()
	// get
	for i := 0; i < N; i++ {
		ki.Prepare()
		meta2, pos2, found := tree.get(ki)
		if !found || pos2.Offset != uint32(i)*PADDING {
			t.Fatalf("%d: fail to get %#v, found = %v, meta2 = %#v, pos = %#v", i, ki, found, meta2, pos2)
		}
		ki.KeyHash++
	}

	// delete
	reset()
	meta.Ver = -2
	for i := 0; i < N; i++ {
		ki.Prepare()
		tree.set(ki, &meta, pos)
		count := int(dstNode.count)
		if count != N-i-1 {
			t.Fatalf("wrong count %d != %d", count, N-i-1)
		}
		ki.KeyHash++
	}

	// set again
	reset()
	for i := 0; i < N; i++ {
		ki.Prepare()
		tree.set(ki, &meta, pos)
		count := int(dstNode.count)
		if count != i+1 {
			t.Fatalf("wrong count %d != %d", count, i+1)
		}
		ki.KeyHash++
		pos.Offset += PADDING
	}
	for i := 0; i < height-1; i++ {
		if tree.levels[i][0].count != 0 {
			t.Fatalf("wrong count %d != 0", i)
		}
	}
	tree.updateNodes(0, 0)
	for i := 0; i < height-1; i++ {
		if int(tree.levels[i][0].count) != N {
			t.Fatalf("wrong count %d != N", i)
		}
	}

	ki.KeyHash = uint64((treepos << uint32(64-4*depth)) + (0xf << uint32(64-4*depth-4)))
	ki.Prepare()
	tree.set(ki, &meta, pos)

	// list root
	ki.KeyIsPath = true
	ki.StringKey = fmt.Sprintf("%x", treepos)
	ki.Key = []byte(ki.StringKey)
	ki.Prepare()

	thresholdListKey += 2
	items, nodes := tree.listDir(ki)
	if !(len(nodes) == 0 && len(items) == N+1) {
		t.Fatalf("items:%v, nodes:%v", items, nodes)
	}
	thresholdListKey -= 2
	items, nodes = tree.listDir(ki)
	if !(len(nodes) == 16 && len(items) == 0 && int(nodes[15].count) == 1 && int(nodes[0].count) == N) {
		t.Fatalf("items:%v, nodes:%v, N = %d, \n level0:%v \n level1: %v ", items, nodes, N, tree.levels[0], tree.levels[1])
	}

	ki.StringKey = fmt.Sprintf("%016x", keyhash)
	ki.Key = []byte(ki.StringKey)
	ki.Prepare()
	items, nodes = tree.listDir(ki)
	if !(len(nodes) == 0 && len(items) == 1) {
		t.Fatalf("%s items:%v, nodes:%v", ki.StringKey, items, nodes)
	}
	return
}

type HTreeBench struct {
	treepos     int
	itemPerLeaf int // doubandb is 438*1024*1024/(1<<24) = 27

	// runtime
	tree    *HTree
	base    uint64
	step    uint64
	req     HTreeReq
	numLeaf int

	ki   *KeyInfo
	meta Meta
	pos  Position
}

func (hb *HTreeBench) init() {
	Conf.InitDefault()
	Conf.TreeHeight = *tHeigth
	Conf.NumBucket = 1
	Conf.Init()
	hb.tree = newHTree(Conf.TreeDepth, hb.treepos, Conf.TreeHeight)
	hb.base = uint64(0)
	hb.step = uint64(1<<(uint32(8-Conf.TreeHeight+1)*4)) << 32 // (0x00000100 << 32) given depthbench = 6
	hb.numLeaf = 1 << (4 * (uint32(Conf.TreeHeight)))
	hb.ki = NewKeyInfoFromBytes([]byte("key"), 0, false)
	hb.meta = Meta{Ver: 1, ValueHash: 255}
	hb.pos = Position{0, 0}
}

func (hb *HTreeBench) setKeysFast() {
	base := hb.base
	for i := 0; i < hb.numLeaf; i++ {
		hb.ki.KeyHash = base
		for j := 0; j < hb.itemPerLeaf; j++ {
			hb.ki.Prepare()
			hb.tree.set(hb.ki, &hb.meta, hb.pos)
			hb.ki.KeyHash += 1
		}
		base += hb.step
	}
}

func (hb *HTreeBench) setKeysSlow() {
	base := hb.base
	for i := 0; i < hb.itemPerLeaf; i++ {
		hb.ki.KeyHash = base
		for j := 0; j < hb.numLeaf; j++ {
			hb.ki.Prepare()
			hb.tree.set(hb.ki, &hb.meta, hb.pos)
			hb.ki.KeyHash += hb.step
		}
		base += 1
	}
}

func (hb *HTreeBench) getKeys() {
	base := hb.base
	for i := 0; i < hb.itemPerLeaf; i++ {
		hb.ki.KeyHash = base
		for j := 0; j < hb.numLeaf; j++ {
			hb.ki.Prepare()
			hb.tree.get(hb.ki)
			hb.ki.KeyHash += hb.step
		}
		base += 1
	}
}

func BenchmarkHTreeSetFastGet(b *testing.B) {
	hb := &HTreeBench{
		itemPerLeaf: 30,
	}
	hb.init()
	hb.setKeysFast()
	pf := StartCpuProfile("BenchmarkHTreeSetFastGet")
	hb.getKeys()
	StopCpuProfile(pf)
	WriteHeapProfile("BenchmarkHTreeSetFastGet")
}

func BenchmarkHTreeSetFast(b *testing.B) {
	hb := &HTreeBench{
		itemPerLeaf: 30,
	}
	hb.init()
	pf := StartCpuProfile("BenchmarkHTreeSetFast")
	hb.setKeysFast()
	StopCpuProfile(pf)
	WriteHeapProfile("BenchmarkHTreeSetFast")
}

func BenchmarkHTreeSetSlow(b *testing.B) {
	hb := &HTreeBench{
		itemPerLeaf: 30,
	}
	hb.init()
	pf := StartCpuProfile("BenchmarkHTreeSetSlow")
	hb.setKeysSlow()
	StopCpuProfile(pf)
	WriteHeapProfile("BenchmarkHTreeSetSlow")
}

func tLoadAHint(tree *HTree, r *hintFileReader) (numKey, numAll int, e error) {
	meta := Meta{Ver: 1, ValueHash: 255}
	var pos Position
	for {
		item, err := r.next()
		if err != nil {
			logger.Infof("%s", err)
			e = err
			return
		}
		if item == nil {
			return
		}
		numAll++
		if item.Ver > 0 {
			ki := NewKeyInfoFromBytes([]byte(item.Key), item.Keyhash, false)
			meta.ValueHash = item.Vhash
			meta.Ver = item.Ver
			pos.Offset = item.Pos.Offset
			ki.Prepare()
			tree.set(ki, &meta, pos)
			numKey++
			if *tKeysPerGC > 0 && numKey%(*tKeysPerGC) == 0 {
				FreeMem()
			}
		}
	}
}

func tSetHTreeFromChan(tree *HTree, khashs chan uint64) {
	var ki KeyInfo
	meta := Meta{Ver: 1, ValueHash: 255}
	var pos Position
	var kh uint64
	for {
		kh = 1
		kh = <-khashs
		if kh == 0 {
			return
		}
		ki.KeyHash = kh
		ki.Prepare()
		tree.set(&ki, &meta, pos)
	}
}

func tLoadAHintP(tree *HTree, r *hintFileReader) (numKey, numAll int, e error) {
	numKey = 0
	N := 1000000
	khashs := make(chan uint64, N)
	end := false
	for !end {
		for i := 0; i < N; i++ {
			item, err := r.next()
			if err != nil {
				logger.Infof("%s", err)
				e = err
				return
			}
			if item == nil {
				end = true
				break
			}
			if item.Ver > 0 && item.Keyhash != 0 {
				khashs <- item.Keyhash
				numKey++
			}
			numAll++
		}
		nG := 16
		for i := 0; i < nG; i++ {
			go tSetHTreeFromChan(tree, khashs)
		}
		for len(khashs) > 0 {
			time.Sleep(100 * time.Microsecond)
		}
		for i := 0; i < nG; i++ {
			khashs <- 0
		}
	}
	return
}

func TestRebuildHtreeFromHints(b *testing.T) {
	if *tDataDir == "" {
		return
	}
	runtime.GOMAXPROCS(8)

	Conf.InitDefault()
	Conf.NumBucket = *tNumbucket
	Conf.TreeHeight = *tHeigth
	Conf.Init()

	pos, err := strconv.ParseInt(*tPos, 16, 32)
	if err != nil {
		b.Fatalf("%s", err.Error())
	}

	files, _ := filepath.Glob(filepath.Join(*tDataDir, "*"+".hint.s"))
	logger.Infof("to load %d files", len(files))
	var pf *os.File
	if *tPort == 0 {
		pf = StartCpuProfile("BenchmarkLoadHints")
	} else {
		go func() {
			logger.Infof("%v", http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *tPort), nil))
		}()
	}

	tree := newHTree(Conf.TreeDepth, int(pos), Conf.TreeHeight)
	totalNumKey := 0
	sort.Strings(files)
	for i, file := range files {
		logger.Infof("loading: %s", file)
		r := newHintFileReader(file, 0, 1024*1024)
		r.open()
		numKey := 0
		numAll := 0
		var e error
		if *tParallel == 0 {
			numKey, numAll, e = tLoadAHint(tree, r)
		} else {
			numKey, numAll, e = tLoadAHintP(tree, r)
		}
		if e != nil {
			return
		}
		totalNumKey += numKey
		logger.Infof("%03d: #allkey %d, #key %d, #key_total %d", i, numAll, numKey, totalNumKey)
		logger.Infof("%03d: max rss before gc: %d", i, utils.GetMaxRSS())
		r.close()
		r = nil
	}

	if *tPort == 0 {
		StopCpuProfile(pf)
	}
	ListAll(tree, *tPos+"00000000")
	WriteHeapProfile("BenchmarkHints")
	if *tPort != 0 {

		for i := 0; ; i++ {
			time.Sleep(5 * time.Minute)
			FreeMem()
			logger.Infof("after %03d minites, max rss = %d", i*5, utils.GetMaxRSS())
		}
	}
	runtime.GOMAXPROCS(1)
}

// list bucket[:d], bucket[:d+1], bucket[:d+1]..
func ListAll(tree *HTree, bucket string) {
	logger.Infof("list all %s", bucket)
	ki := NewKeyInfoFromBytes([]byte(bucket), 0, true)
	for i := tree.depth; i < 16; i++ {
		ki.StringKey = bucket[:i]
		ki.Prepare()
		s, err := tree.ListDir(ki)
		if err != nil {
			logger.Fatalf("list @%s\n%s", ki.StringKey, err)
			return
		}
		if s == nil {
			return
		}
		logger.Infof("@%s\n%sEND\n", ki.StringKey, string(s))
	}
}
