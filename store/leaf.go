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
const TREE_ITEM_HEAD_SIZE = 11

// BlockArrayLeaf

type SliceHeader struct {
	Data uintptr
	Len  int
}

func (sh *SliceHeader) ToBytes() (b []byte) {
	sb := (*reflect.SliceHeader)((unsafe.Pointer(&b)))
	sb.Data = sh.Data
	sb.Cap = sh.Len
	sb.Len = sh.Len
	return
}

type ItemFunc func(uint64, *HTreeItem)

func getNodeKhash(path []int) uint32 {
	var khash uint32
	for i, off := range path {
		khash += uint32(((off & 0xf) << uint32((4 * (7 - i)))))
	}
	return khash
}

func bytesToItem(b []byte, item *HTreeItem) {
	item.Ver = int32(binary.LittleEndian.Uint32(b))
	item.Vhash = binary.LittleEndian.Uint16(b[4:])
	item.Pos.Offset = (uint32(b[6])<<8 | uint32(b[7])<<16 | uint32(b[8])<<24)
	//item.pos.ChunkID = int(uint32(b[9]))
	item.Pos.ChunkID = int(uint32(b[9]) | uint32(b[10])<<8)
}

func itemToBytes(b []byte, item *HTreeItem) {
	binary.LittleEndian.PutUint32(b, uint32(item.Ver))
	binary.LittleEndian.PutUint16(b[4:], item.Vhash)
	v := item.Pos.Offset
	b[6] = byte(v >> 8)
	b[7] = byte(v >> 16)
	b[8] = byte(v >> 24)
	v = uint32(item.Pos.ChunkID)
	b[9] = byte(v)
	b[10] = byte(v >> 8)
}

func khashToBytes(b []byte, khash uint64) {
	binary.LittleEndian.PutUint64(b, khash)
}

func bytesToKhash(b []byte) (khash uint64) {
	return binary.LittleEndian.Uint64(b)
}

func findInBytes(leaf []byte, keyhash uint64) int {
	lenKHash := Conf.TreeKeyHashLen
	lenItem := lenKHash + TREE_ITEM_HEAD_SIZE
	size := len(leaf)
	var khashBytes [8]byte
	khashToBytes(khashBytes[0:], keyhash)
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
func (sh *SliceHeader) enlarge(size int) {
	if sh.Len != 0 {
		sh.Data = uintptr(C.realloc(unsafe.Pointer(sh.Data), C.size_t(size)))
	} else {
		sh.Data = uintptr(C.malloc(C.size_t(size)))
	}
	sh.Len = size
}

func (sh *SliceHeader) free() {
	if sh.Len != 0 {
		C.free(unsafe.Pointer(sh.Data))
	}
}

func (sh *SliceHeader) Set(req *HTreeReq) (oldm HTreeItem, exist bool) {
	leaf := sh.ToBytes()
	lenKHash := Conf.TreeKeyHashLen
	idx := findInBytes(leaf, req.ki.KeyHash)
	exist = (idx >= 0)
	var dst []byte
	if exist {
		bytesToItem(leaf[idx+lenKHash:], &oldm)
		dst = leaf[idx:]
	} else {
		newSize := len(leaf) + lenKHash + TREE_ITEM_HEAD_SIZE
		sh.enlarge(newSize)
		dst = sh.ToBytes()[len(leaf):]
	}
	khashToBytes(dst, req.ki.KeyHash)
	itemToBytes(dst[lenKHash:], &req.item)
	return
}

func (sh *SliceHeader) Remove(ki *KeyInfo, oldPos Position) (oldm HTreeItem, removed bool) {
	leaf := sh.ToBytes()
	lenKHash := Conf.TreeKeyHashLen
	itemLen := lenKHash + TREE_ITEM_HEAD_SIZE
	idx := findInBytes(leaf, ki.KeyHash)
	if idx >= 0 {
		bytesToItem(leaf[idx+lenKHash:], &oldm)
		if oldPos.ChunkID == -1 || oldm.Pos.Offset == oldPos.Offset {
			removed = true
			copy(leaf[idx:], leaf[idx+itemLen:])
			sh.Len -= itemLen
		}
	}
	return
}

func (sh *SliceHeader) Get(req *HTreeReq) (exist bool) {
	leaf := sh.ToBytes()
	idx := findInBytes(leaf, req.ki.KeyHash)
	exist = (idx >= 0)
	if exist {
		//TODO
		bytesToItem(leaf[idx+Conf.TreeKeyHashLen:], &req.item)
	}
	return
}

func (sh *SliceHeader) Iter(f ItemFunc, ni *NodeInfo) {
	leaf := sh.ToBytes()
	lenKHash := Conf.TreeKeyHashLen
	lenItem := lenKHash + TREE_ITEM_HEAD_SIZE
	mask := Conf.TreeKeyHashMask

	nodeKHash := uint64(getNodeKhash(ni.path)) << 32 & (^Conf.TreeKeyHashMask)
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
