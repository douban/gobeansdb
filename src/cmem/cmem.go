package cmem

/*
#include <stdlib.h>
*/
import "C"
import "unsafe"
import "sync/atomic"

const (
	TagSetData = iota
	TagFlushData
	TagGetData
	TagGuard
)

var (
	AllocedSize  []int64 = make([]int64, TagGuard)
	AllocedCount []int64 = make([]int64, TagGuard)
	Chans        []chan int
)

func init() {
	Chans = make([]chan int, TagGuard)
	for i := 0; i < TagGuard; i++ {
		Chans[i] = make(chan int, 1)
	}
}

func Add(tag int, size int) {
	atomic.AddInt64(&AllocedSize[tag], int64(size))
	atomic.AddInt64(&AllocedCount[tag], 1)
}

func Sub(tag int, size int) {
	atomic.AddInt64(&AllocedSize[tag], int64(size))
	atomic.AddInt64(&AllocedCount[tag], -1)
}

func Alloc(size int) *byte {
	return (*byte)(C.malloc(C.size_t(size)))
}

func Free(ptr *byte, size int) {
	C.free(unsafe.Pointer(ptr))
}

func Alloced() int64 {
	var allocedAll int64
	for i := 0; i < TagGuard; i++ {
		allocedAll += AllocedSize[i]
	}
	return int64(allocedAll)
}
