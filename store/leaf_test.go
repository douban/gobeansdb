package store

import (
	"bytes"
	"testing"
)

func TestBytes(t *testing.T) {
	b := make([]byte, 30)
	m := HTreeItem{Keyhash: 0, Pos: Position{1, 0}, Ver: 2, Vhash: 3}
	itemToBytes(b, &m)
	m2 := HTreeItem{Keyhash: 0, Pos: Position{4, 0}, Ver: 5, Vhash: 6}
	bytesToItem(b, &m2)
	if m != m2 {
		t.Fatalf("bytesToItem fail %v != %v", m, m2)
	}

	k := uint64(0xaaabbbbb) << 32
	path := []int{0xa, 0xa, 0xa}

	khashToBytes(b, k)

	lenKHash := KHASH_LENS[len(path)]
	mask := ((uint64(1) << (uint32(lenKHash) * 8)) - 1)
	nodeKHash := k & (^mask)
	k2 := bytesToKhash(b)
	k2 &= mask
	k2 += (nodeKHash)

	t.Logf("lenKHash = %d, mask %016x, node hash %016x", lenKHash, mask, nodeKHash)
	if k != k2 {
		t.Fatalf("bytesToKhash fail %016x != %016x", k, k2)
	}
}

func TestLeafEnlarge(t *testing.T) {
	var sh SliceHeader
	sh.enlarge(10)
	leaf := sh.ToBytes()
	if len(leaf) != 10 {
		t.Fatalf("%v %v", leaf, sh)
	}
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	copy(leaf, data)
	sh.enlarge(20)
	leaf = sh.ToBytes()
	if len(leaf) != 20 || 0 != bytes.Compare(leaf[:10], data) {
		t.Fatalf("%v", leaf)
	}
}

func TestLeaf(t *testing.T) {
	Conf.InitDefault()

	Conf.NumBucket = 256
	Conf.Init()

	// lenKHash := KHASH_LENS[len(ni.path)]
	// t.Logf("%d %d", lenKHash, Conf.TreeKeyHashLen)
	lenKHash := Conf.TreeKeyHashLen
	lenItem := lenKHash + TREE_ITEM_HEAD_SIZE

	var sh SliceHeader
	var leaf []byte
	var base uint64 = 0xfe * (1 << 56)
	N := 16
	exist := true

	ki := NewKeyInfoFromBytes([]byte("key"), base, false)

	req := HTreeReq{ki: ki}

	reset := func() {
		req.ki.KeyHash = base
		req.Meta = Meta{Ver: 1, ValueHash: 255}
		req.Position = Position{0, 0}
	}

	// set
	reset()
	for i := 0; i < N; i++ {
		ki.Prepare()
		req.encode()
		_, exist = sh.Set(&req)
		leaf = sh.ToBytes()
		if exist || len(leaf) != (i+1)*lenItem {
			t.Fatalf("i = %d, leaf = %v, sh = %v, exist = %v", i, leaf, sh, exist)
		}
		req.Offset += PADDING
		ki.KeyHash++
	}

	// update
	var shift uint32 = 100

	reset()
	req.Offset += shift * PADDING
	for i := 0; i < N; i++ {
		ki.Prepare()
		req.encode()
		_, exist = sh.Set(&req)
		leaf = sh.ToBytes()
		if !exist || len(leaf) != N*lenItem {
			t.Fatalf("i = %d, leaf = %v, exist = %v", i, leaf, exist)
		}
		req.Offset += PADDING
		ki.KeyHash++
	}

	// get
	reset()
	for i := 0; i < N; i++ {
		ki.Prepare()
		found := sh.Get(&req)
		if !found || req.item.Pos.Offset != (uint32(i)+shift)*PADDING {
			t.Fatalf("i=%d, shift=%d, found=%v, req.item=%#v", i, shift, found, req.item)
		}
		ki.KeyHash++
	}

	// remove
	reset()
	req.Offset += shift * PADDING
	for i := 0; i < N; i++ {
		ki.Prepare()
		oldm, removed := sh.Remove(ki, Position{0, req.Offset})
		if !removed || oldm.Pos.Offset != req.Offset || sh.Len != lenItem*(N-i-1) {
			t.Fatalf("i=%d, offset=%x, removed=%v, oldm =%#v, %v",
				i, req.Offset, removed, oldm, sh.Len/lenItem)
		}
		req.Offset += PADDING
		ki.KeyHash++
	}

	// iter
	i := 0
	f := func(h uint64, m *HTreeItem) {
		if h != base+uint64(i) || m.Pos.Offset != (uint32(i)+shift)*PADDING {
			t.Fatalf("%d: %016x %v", i, h, m)
		}
		i += 1
	}

	var ni NodeInfo
	ni.path = []int{0xf, 0xe}
	sh.Iter(f, &ni)
}
