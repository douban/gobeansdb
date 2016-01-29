package store

import (
	"strconv"
	"unicode"

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
	length := len(key)
	if length == 0 || length > MAX_KEY_LEN {
		logger.Warnf("bad key len=%d", length)
		return false
	}

	if key[0] <= ' ' || key[0] == '?' || key[0] == '@' {
		logger.Warnf("bad key len=%d key[0]=%x", length, key[0])
		return false
	}

	for _, r := range key {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			logger.Warnf("bad key len=%d %s", length, key)
			return false
		}
	}
	return true
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

func ParsePathString(pathStr string, buf []int) ([]int, error) {
	path := buf[:len(pathStr)]
	for i := 0; i < len(pathStr); i++ {
		idx, err := strconv.ParseInt(pathStr[i:i+1], 16, 0)
		if err != nil {
			return nil, err
		}
		path[i] = int(idx)
	}
	return path, nil
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

func (ki *KeyInfo) setKeyHashByPath() {
	v := ki.KeyPath
	shift := uint(60)
	ki.KeyHash = 0
	for i := 0; i < len(v); i++ {
		ki.KeyHash |= (uint64(v[i]) << shift)
		shift -= 4
	}
}

func (ki *KeyInfo) Prepare() (err error) {
	if ki.KeyIsPath {
		ki.KeyPath, err = ParsePathString(ki.StringKey, ki.KeyPathBuf[:16])
		ki.setKeyHashByPath()
		if len(ki.KeyPath) < Conf.TreeDepth {
			ki.BucketID = -1
			return
		}
	} else {
		ki.KeyPath = ParsePathUint64(ki.KeyHash, ki.KeyPathBuf[:16])
	}
	ki.BucketID = 0
	for _, v := range ki.KeyPath[:Conf.TreeDepth] {
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
