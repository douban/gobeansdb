package store

import (
	"fmt"
	"strconv"

	"github.com/spaolacci/murmur3"
)

const (
	MAX_KEY_LEN = 250
)

var (
	getKeyHash HashFuncType = getKeyHashDefalut
)

type HashFuncType func(key []byte) uint64

func IsValidKeyString(key string) bool {
	return !(key[0] == '?' || key[0] == '@')
}

func murmur(data []byte) (h uint32) {
	hasher := murmur3.New32()
	hasher.Write(data)
	return hasher.Sum32()
}

func fnv1a(data []byte) (h uint32) {
	PRIME := uint32(0x01000193)
	h = 0x811c9dc5
	for _, b := range data {
		h ^= uint32(int8(b))
		h = (h * PRIME)
	}
	return h
}

func getKeyHashDefalut(key []byte) uint64 {
	return (uint64(fnv1a(key)) << 32) | uint64(murmur(key))
}

// computed once, before being routed to a bucket
type KeyPos struct {
	KeyPathBuf [16]int
	KeyPath    []int

	// need depth
	BucketID        int
	KeyPathInBucket []int
}

type KeyInfo struct {
	KeyHash   uint64
	KeyIsPath bool
	Key       []byte
	StringKey string
	KeyPos
}

func getBucketFromKey(key string) int {
	return 0
}

func ParsePathUint64(khash uint64, buf []int) []int {
	for i := 0; i < 16; i++ {
		shift := uint32(4 * (15 - i))
		idx := int((khash >> shift)) & 0xf
		buf[i] = int(idx)
	}
	return buf
}

func ParsePathString(pathStr string, buf []int) []int {
	path := buf[:len(pathStr)]
	for i := 0; i < len(pathStr); i++ {
		idx, err := strconv.ParseInt(pathStr[i:i+1], 16, 0)
		if err != nil {
			return nil
		}
		path[i] = int(idx)
	}
	return path
}

func checkKey(key []byte) error {
	if len(key) > MAX_KEY_LEN {
		return fmt.Errorf("key too long: %s", string(key))
	}
	return nil
}

func NewKeyInfoFromBytes(key []byte, keyhash uint64, keyIsPath bool) (ki *KeyInfo) {
	ki = &KeyInfo{
		KeyIsPath: keyIsPath,
		Key:       key,
		StringKey: string(key),
		KeyHash:   keyhash,
	}
	ki.Prepare()
	return
}

func (ki *KeyInfo) getKeyHash() {
	v := ki.KeyPath
	shift := uint(60)
	ki.KeyHash = 0
	for i := 0; i < len(v); i++ {
		ki.KeyHash |= (uint64(v[i]) << shift)
		shift -= 4
	}
}

func (ki *KeyInfo) Prepare() {
	if ki.KeyIsPath {
		ki.KeyPath = ParsePathString(ki.StringKey, ki.KeyPathBuf[:16])
		ki.getKeyHash()
		if len(ki.KeyPath) < config.TreeDepth {
			ki.BucketID = -1
			return
		}
	} else {
		ki.KeyPath = ParsePathUint64(ki.KeyHash, ki.KeyPathBuf[:16])
	}
	ki.BucketID = 0
	for _, v := range ki.KeyPath[:config.TreeDepth] {
		ki.BucketID <<= 4
		ki.BucketID += v
	}

	return
}

func isSamePath(x, y []int, n int) bool {
	for i := 0; i < n; i++ {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}
