package store

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.intra.douban.com/coresys/gobeansdb/loghub"
	"github.intra.douban.com/coresys/gobeansdb/utils"
)

func init() {
	SecsBeforeDump = 1 // dump quick for test
}

func readHintAndCheck(t *testing.T, path string, items []*HintItem) {
	r := newHintFileReader(path, 0, 10240)
	if err := r.open(); err != nil {
		t.Fatal(err)
	}
	n := len(items)
	for i := 0; i < n+2; i++ {
		it, err := r.next()
		if err != nil {
			t.Fatal(err)
		}
		if i < n {
			if it == nil || *it != *items[i] {
				t.Fatalf("%d %#v != %#v", i, it, items[i])
			}
		} else {
			if it != nil {
				t.Fatalf("%#v ", it)
			}
		}
	}
	r.close()
}

func TestHintRW(t *testing.T) {
	setupTest("TestHintRW", 1)
	defer clearTest()
	path := fmt.Sprintf("%s/%s", dir, "000.hint.s")
	utils.Remove(path)

	conf.IndexIntervalSize = 1024

	w, err := newHintFileWriter(path, 0, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer utils.Remove(path)
	n := 100
	items := genSortedHintItems(n)
	for _, it := range items {
		w.writeItem(it)
	}
	w.close()
	readHintAndCheck(t, path, items)
	index := &hintFileIndex{w.index.toIndex(), path, hintFileMeta{}}
	t.Logf("#index = %d, %#v", len(index.index), index.index)
	index2, err := loadHintIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(index.index) != len(index2.index) {
		t.Fatalf("%v \n!=\n %v", index.index, index2.index)
	}
	for i := 0; i < len(index.index); i++ {
		if index.index[i] != index2.index[i] {
			t.Fatalf("%v != %v", index.index, index2.index)
		}
	}
	checkIndex(t, items, index)
}

func checkIndex(t *testing.T, items []*HintItem, index *hintFileIndex) {
	for _, it := range items {
		it2, err := index.get(it.Keyhash, it.Key)
		if err != nil {
			t.Fatal(err)
		}
		if it2 == nil || *it != *it2 {
			t.Fatalf("%v != %v", it, it2)
		}
	}
}

func genSortedHintItems(n int) []*HintItem {
	items := make([]*HintItem, n)
	for i := 0; i < n; i++ {
		base := i * 3
		it := &HintItem{
			HintItemMeta{
				Keyhash: uint64(i),
				Pos:     uint32(base) * 256,
				Ver:     int32(base + 1),
				Vhash:   uint16(base + 2),
			},
			genKey(i),
		}
		items[i] = it
	}
	return items
}

func testMerge(t *testing.T, nsrc int) {
	setupTest(fmt.Sprintf("%s_%d", "TestHintMerge", nsrc), 1)
	defer clearTest()

	n := 10
	items := genSortedHintItems(n)
	src := make([]*hintFileWriter, nsrc)
	srcp := make([]*hintFileReader, nsrc)
	for i := 0; i < nsrc; i++ {
		path := fmt.Sprintf("%s/src.%d.hint.s", dir, i)
		srcp[i] = newHintFileReader(path, 0, 1024)
		utils.Remove(path)
		w, err := newHintFileWriter(path, 0, 1024)
		if err != nil {
			t.Fatal(err)
		}
		defer utils.Remove(path)
		src[i] = w
	}
	for i := 0; i < n; i++ {
		w := src[rand.Intn(nsrc)]
		it := items[i]

		tmp := new(HintItem)
		*tmp = *it
		tmp.Pos = it.Pos - 1
		w.writeItem(tmp)
		// TODO: same khash diff key

		w.writeItem(it)
	}
	for i := 0; i < nsrc; i++ {
		src[i].close()
	}
	dst := fmt.Sprintf("%s/dst.hint.s", dir)
	utils.Remove(dst)
	state := HintStateDump
	ct := newCollisionTable()
	merge(srcp, dst, ct, &state, false)
	defer utils.Remove(dst)
	readHintAndCheck(t, dst, items)
}

func TestMerge2(t *testing.T) {
	testMerge(t, 2)
}

func TestMerge3(t *testing.T) {
	testMerge(t, 3)
}

func genKey(i int) string {
	return fmt.Sprintf("key_%05d", i)
}

func setAndCheckHintBuffer(t *testing.T, buf *hintBuffer, it *HintItem) {
	if !buf.set(it, 0) {
		t.Fatalf("%#v set return false", it)
	}
	r, _ := buf.get(it.Keyhash, it.Key)
	if r == nil || *r != *it {
		t.Fatalf("%#v != %#v", r, it)
	}
}

func TestHintBuffer(t *testing.T) {
	n := 10
	conf.SplitCap = int64(n)
	conf.IndexIntervalSize = 128

	defer func() {

	}()

	buf := newHintBuffer()
	items := genSortedHintItems(n + 1)
	for i := 0; i < n; i++ {
		setAndCheckHintBuffer(t, buf, items[i])
	}
	if buf.set(items[n], 0) {
		t.Fatalf("set return true")
	}
	items[n-1].Ver = -1
	setAndCheckHintBuffer(t, buf, items[n-1])
}

func checkChunk(t *testing.T, ck *hintChunk, it *HintItem) {
	r, err := ck.get(it.Keyhash, it.Key, false)
	if err != nil || r == nil || *r != *it {
		t.Fatalf("err = %s, %#v != %#v", err, r, it)
	}
}

func setAndCheckChunk(t *testing.T, ck *hintChunk, it *HintItem, rotate bool) {
	if rotate != ck.set(it, 0) {
		t.Fatalf("%#v not %v", it, rotate)
	}
	checkChunk(t, ck, it)
}

func TestHintChunk(t *testing.T) {
	n := 10
	conf.SplitCap = int64(n)
	conf.IndexIntervalSize = 128

	ck := newHintChunk(0)
	items := genSortedHintItems(n + 2)
	i := 0
	for ; i < n; i++ {
		setAndCheckChunk(t, ck, items[i], false)
	}
	setAndCheckChunk(t, ck, items[i], true)
	setAndCheckChunk(t, ck, items[i+1], false)

	if len(ck.splits) != 2 {
		t.Fatalf("%d", len(ck.splits))
	}
	items[n-1].Ver = -1
	setAndCheckChunk(t, ck, items[n-1], false)
}

func checkMgr(t *testing.T, hm *hintMgr, it *HintItem, chunkID int) {
	r, cid, err := hm.getItem(it.Keyhash, it.Key, false)
	if err != nil {
		t.Fatalf("%#v, %s", it, err.Error())
	}
	if r == nil || *r != *it || cid != chunkID {
		t.Fatalf("%#v != %#v or %d != %d, %s", r, it, cid, chunkID, loghub.GetStack(1000))
	}
}

func setAndCheckMgr(t *testing.T, hm *hintMgr, it *HintItem, chunkID int) {
	hm.setItem(it, chunkID, 0)
	checkMgr(t, hm, it, chunkID)
}

func checkFiles(t *testing.T, dir string, files *utils.Dir) {
	diskfiles, df1, df2, err := files.CheckPath(dir)
	if err != nil || df1 != nil || df2 != nil {
		t.Fatal(files.ToSlice(), diskfiles.ToSlice(), df1, df2, err)
	}
}

func fillChunk(t *testing.T, dir string, hm *hintMgr, items []*HintItem, chunkID int, files *utils.Dir) {
	logger.Infof("fill %d", chunkID)
	n := len(items)
	setAndCheckMgr(t, hm, items[0], chunkID)
	time.Sleep(time.Second * 4)
	hm.dumpAndMerge(false)
	hm.Merge(false)
	logger.Infof("check %d", chunkID)
	checkFiles(t, dir, files)

	checkMgr(t, hm, items[0], chunkID)
	for i := 1; i < n; i++ {
		checkMgr(t, hm, items[i], chunkID-1)
	}
	for i := 1; i < n; i++ {
		setAndCheckMgr(t, hm, items[i], chunkID)
	}
}

func TestHintMgr(t *testing.T) {
	setupTest("testHintMgr", 0)
	defer clearTest()
	runtime.GOMAXPROCS(4)

	persp := 10
	conf.SplitCap = int64(persp)
	conf.IndexIntervalSize = 128
	conf.MergeInterval = 250 // disable async merge
	nsp := 2
	n := persp * nsp
	items := genSortedHintItems(n)
	hm := newHintMgr(0, dir)

	chunkID := 3
	// write 3
	for i := 0; i < n; i++ {
		setAndCheckMgr(t, hm, items[i], chunkID)
	}
	files := utils.NewDir()
	files.Set("collision.yaml", -1)
	files.SetMultiNoSize("003.000.idx.s", "003.001.idx.s", "003.001.idx.m")
	fillChunk(t, dir, hm, items, 4, files)
	files.SetMultiNoSize("004.000.idx.s", "004.001.idx.s", "004.001.idx.m")
	files.Delete("003.001.idx.m")
	fillChunk(t, dir, hm, items, 5, files)
	files.SetMultiNoSize("005.000.idx.s", "005.001.idx.s", "005.001.idx.m")
	files.Delete("004.001.idx.m")
	fillChunk(t, dir, hm, items, 6, files)

	setAndCheckMgr(t, hm, items[0], 7)
	hm.dumpAndMerge(false)
	// get mtime
	// change item content, 注意 pos
	logger.Infof("set 6 again")
	it := *(items[0])
	it.Pos = items[0].Pos + 100
	hm.setItem(&it, 6, 0)

	time.Sleep(time.Second * 5)
	checkMgr(t, hm, items[0], 7)
	time.Sleep(time.Second * 3)
	checkChunk(t, hm.chunks[6], &it)
}
