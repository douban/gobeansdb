package store

/*
#include<stdlib.h>
#include<string.h>
int find(void *ss, void *s, int item_size, int cmp_size, int n) {
	char *p = (char*)ss;
	int i;
	for (i = 0; i < n; i++, p += item_size) {
		if (0 == memcmp(p, s, cmp_size))
			return i;
	}
	return -1;
}
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"unsafe"
)

const LEN_USE_C_FIND = 100

// BlockArrayLeaf

type bytesLeaf []byte

type ItemFunc func(uint64, *HTreeItem)

func getNodeKhash(path []int) uint32 {
	var khash uint32
	for i, off := range path {
		khash += uint32(((off & 0xf) << uint32((4 * (7 - i)))))
	}
	return khash
}

func bytesToItem(b []byte, item *HTreeItem) {
	item.pos = binary.LittleEndian.Uint32(b)
	item.ver = int32(binary.LittleEndian.Uint32(b[4:]))
	item.vhash = binary.LittleEndian.Uint16(b[8:])
}

func itemToBytes(b []byte, item *HTreeItem) {
	binary.LittleEndian.PutUint32(b, item.pos)
	binary.LittleEndian.PutUint32(b[4:], uint32(item.ver))
	binary.LittleEndian.PutUint16(b[8:], item.vhash)
}

func khashToBytes(b []byte, khash uint64) {
	binary.LittleEndian.PutUint64(b, khash)
}

func bytesToKhash(b []byte) (khash uint64) {
	return binary.LittleEndian.Uint64(b)
}

func (leaf bytesLeaf) find(req *HTreeReq, ni *NodeInfo) int {
	lenKHash := config.TreeKeyHashLen
	lenItem := lenKHash + 10
	size := len(leaf)
	var khashBytes [8]byte
	khashToBytes(khashBytes[0:], req.ki.KeyHash)
	kb := khashBytes[:lenKHash]
	n := len(leaf) / lenItem
	if n < LEN_USE_C_FIND {
		for i := 0; i < size; i += lenItem {
			if bytes.Compare(leaf[i:i+lenKHash], kb) == 0 {
				return i
			}
		}
	} else {
		ss := (*reflect.SliceHeader)((unsafe.Pointer(&leaf))).Data
		s := (*reflect.SliceHeader)((unsafe.Pointer(&kb))).Data
		i := int(C.find((unsafe.Pointer(ss)), unsafe.Pointer(s), C.int(lenItem), C.int(lenKHash), C.int(n)))
		return i * lenItem
	}
	return -1
}

// not filled with 0!
func (leaf bytesLeaf) enlarge(size int) []byte {
	var b []byte
	src := (*reflect.SliceHeader)((unsafe.Pointer(&leaf)))
	dst := (*reflect.SliceHeader)((unsafe.Pointer(&b)))
	if leaf != nil {
		dst.Data = uintptr(C.realloc(unsafe.Pointer(src.Data), C.size_t(size)))
	} else {
		dst.Data = uintptr(C.malloc(C.size_t(size)))
	}
	dst.Cap = size
	dst.Len = size
	return b
}

func (leaf bytesLeaf) enlarge2(size int) []byte {
	tmp := make([]byte, size)
	copy(tmp, []byte(leaf))
	return tmp
}

func (leaf bytesLeaf) Set(req *HTreeReq, ni *NodeInfo) (oldm HTreeItem, exist bool, newLeaf bytesLeaf) {
	lenKHash := config.TreeKeyHashLen
	// lenKHash = KHASH_LENS[len(ni.path)]
	idx := leaf.find(req, ni)
	exist = (idx >= 0)
	var dst []byte
	var tmp []byte
	if exist {
		bytesToItem(leaf[idx+lenKHash:], &oldm)
		dst = leaf[idx:]
		newLeaf = leaf
	} else {
		newSize := len(leaf) + lenKHash + 10
		tmp = leaf.enlarge(newSize)
		dst = tmp[len(leaf):]
		newLeaf = tmp
	}
	khashToBytes(dst, req.ki.KeyHash)
	itemToBytes(dst[lenKHash:], &req.item)
	return
}

func (leaf bytesLeaf) Get(req *HTreeReq, ni *NodeInfo) (exist bool) {
	idx := leaf.find(req, ni)
	exist = (idx >= 0)
	if exist {
		//TODO
		bytesToItem(leaf[idx+config.TreeKeyHashLen:], &req.item)
	}
	return
}

func (leaf bytesLeaf) Iter(f ItemFunc, ni *NodeInfo) {
	lenKHash := config.TreeKeyHashLen
	lenItem := lenKHash + 10
	mask := config.TreeKeyHashMask

	nodeKHash := uint64(getNodeKhash(ni.path)) << 32 & (^config.TreeKeyHashMask)
	var m HTreeItem
	var khash uint64
	size := len(leaf)
	for i := 0; i < size; i += lenItem {
		bytesToItem(leaf[i+lenKHash:], &m)
		khash = bytesToKhash(leaf[i:])
		khash &= mask
		khash |= nodeKHash
		f(khash, &m)
	}
	return
}
