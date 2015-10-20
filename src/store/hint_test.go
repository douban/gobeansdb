package store

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func SetHintConfig(conf HintConfig) {
	*hintConfig = conf
}

func readHintAndCheck(t *testing.T, path string, items []*hintItem) {
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
	os.Remove(path)
	SetHintConfig(HintConfig{IndexIntervalSize: 1024})
	w, err := newHintFileWriter(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	n := 100
	items := genSortedHintItems(n)
	for _, it := range items {
		w.writeItem(it)
	}
	w.close()
	readHintAndCheck(t, path, items)
	index := &hintFileIndex{w.index.toIndex(), path}
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

func checkIndex(t *testing.T, items []*hintItem, index *hintFileIndex) {
	for _, it := range items {
		it2, err := index.get(it.keyhash, it.key)
		if err != nil {
			t.Fatal(err)
		}
		if it2 == nil || *it != *it2 {
			t.Fatalf("%v != %v", it, it2)
		}
	}
}

func genSortedHintItems(n int) []*hintItem {
	items := make([]*hintItem, n)
	for i := 0; i < n; i++ {
		base := i * 3
		it := &hintItem{
			keyhash: uint64(i),
			key:     genKey(i),
			pos:     uint32(base) * 256,
			ver:     int32(base + 1),
			vhash:   uint16(base + 2),
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
		os.Remove(path)
		w, err := newHintFileWriter(path, 1024)
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(path)
		src[i] = w
	}
	for i := 0; i < n; i++ {
		w := src[rand.Intn(nsrc)]
		it := items[i]

		tmp := new(hintItem)
		*tmp = *it
		tmp.pos = it.pos - 1
		w.writeItem(tmp)
		// TODO: same khash diff key

		w.writeItem(it)
	}
	for i := 0; i < nsrc; i++ {
		src[i].close()
	}
	dst := fmt.Sprintf("%s/dst.hint.s", dir)
	os.Remove(dst)
	merge(srcp, dst)
	defer os.Remove(dst)
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

func setAndCheckHintBuffer(t *testing.T, buf *hintBuffer, it *hintItem) {
	if !buf.set(it) {
		t.Fatalf("%#v set return false", it)
	}
	r := buf.get(it.key)
	if r == nil || *r != *it {
		t.Fatalf("%#v != %#v", r, it)
	}
}

func TestHintBuffer(t *testing.T) {
	n := 10
	SetHintConfig(HintConfig{SplitCount: n, IndexIntervalSize: 128, SecondsBeforeDump: 3})
	buf := newHintBuffer()
	items := genSortedHintItems(n + 1)
	for i := 0; i < n; i++ {
		setAndCheckHintBuffer(t, buf, items[i])
	}
	if buf.set(items[n]) {
		t.Fatalf("set return true")
	}
	items[n-1].ver = -1
	setAndCheckHintBuffer(t, buf, items[n-1])
}

func checkChunk(t *testing.T, ck *hintChunk, it *hintItem) {
	r, err := ck.get(it.keyhash, it.key)
	if err != nil || r == nil || *r != *it {
		t.Fatalf("err = %s, %#v != %#v", err, r, it)
	}
}

func setAndCheckChunk(t *testing.T, ck *hintChunk, it *hintItem, rotate bool) {
	if rotate != ck.set(it) {
		t.Fatalf("%#v not %v", it, rotate)
	}
	checkChunk(t, ck, it)
}

func TestHintChunk(t *testing.T) {
	n := 10
	SetHintConfig(HintConfig{SplitCount: n, IndexIntervalSize: 128, SecondsBeforeDump: 1})
	ck := newHintChunk()
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
	items[n-1].ver = -1
	setAndCheckChunk(t, ck, items[n-1], false)
}

func checkMgr(t *testing.T, hm *hintMgr, it *hintItem, chunkID int) {
	r, cid, err := hm.getItem(it.keyhash, it.key)
	if err != nil {
		t.Fatalf("%#v, %s", it, err.Error())
	}
	if r == nil || *r != *it || cid != chunkID {
		t.Fatalf("%#v != %#v or %d != %d", r, it, cid, chunkID)
	}
}

func setAndCheckMgr(t *testing.T, hm *hintMgr, it *hintItem, chunkID int) {
	hm.setItem(it, chunkID)
	checkMgr(t, hm, it, chunkID)
}

func checkFiles(t *testing.T, dir string, paths []string) {
	f, err := os.Open(dir)
	defer f.Close()
	names, err2 := f.Readdirnames(-1)
	if err != nil || err2 != nil {
		t.Fatal(names, dir, err, err2)
	}
	if len(paths) != len(names) {
		t.Fatalf("%s != %s", names, paths)
	}
	for i, name := range names {
		if name != paths[i] {
			t.Fatalf("%s != %s", names, paths)
		}
	}
}

func fillChunk(t *testing.T, dir string, hm *hintMgr, items []*hintItem, chunkID int, files []string) {
	logger.Infof("fill %d", chunkID)
	n := len(items)
	setAndCheckMgr(t, hm, items[0], chunkID)
	time.Sleep(time.Second * 5)
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

	persp := 10
	SetHintConfig(HintConfig{SplitCount: persp, IndexIntervalSize: 128, SecondsBeforeDump: 2})
	nsp := 3
	n := persp * nsp
	items := genSortedHintItems(n)
	hm := newHintMgr(dir)

	go hm.merger(time.Second * 2)
	chunkID := 3
	// write 3
	for i := 0; i < n; i++ {
		setAndCheckMgr(t, hm, items[i], chunkID)
	}

	files := []string{"003.hint.s.0"}
	fillChunk(t, dir, hm, items, 4, files)

	files = []string{"003.hint.s.0", "004.hint.m", "004.hint.s.0"}
	fillChunk(t, dir, hm, items, 5, files)
	files = []string{"003.hint.s.0", "004.hint.s.0", "005.hint.m", "005.hint.s.0"}
	fillChunk(t, dir, hm, items, 6, files)

	setAndCheckMgr(t, hm, items[0], 7)
	time.Sleep(time.Second * 3)
	// get mtime
	// change item content, 注意 pos
	logger.Infof("set 6 again")
	it := *(items[0])
	it.pos = items[0].pos + 100
	hm.setItem(&it, 6)

	time.Sleep(time.Second * 5)
	checkMgr(t, hm, items[0], 7)
	checkChunk(t, hm.chunks[6], &it)
}
