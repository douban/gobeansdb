package cmem

/*
#include <stdlib.h>
*/
import "C"
import "unsafe"
import "sync/atomic"

type ResourceLimiter struct {
	Count    int64
	Size     int64
	MaxCount int64
	MaxSize  int64
	Chan     chan int `json:"-"`
}

func (rl *ResourceLimiter) reset() {
	*rl = ResourceLimiter{}
	rl.Chan = make(chan int, 1)
}

func (rl *ResourceLimiter) AddSize(size int64) {
	//log.Printf("add %d %d", tag, size)
	atomic.AddInt64(&rl.Size, int64(size))
	atomic.AddInt64(&rl.Count, 1)
	if rl.Size > rl.MaxSize {
		rl.MaxSize = rl.Size
	}
	if rl.Count > rl.MaxCount {
		rl.MaxCount = rl.Count
	}
}

func (rl *ResourceLimiter) SubSize(size int64) {
	//log.Printf("add %d %d", tag, size)
	atomic.AddInt64(&rl.Size, -int64(size))
	atomic.AddInt64(&rl.Count, -1)
}

func (rl *ResourceLimiter) AddCount(count int64) {
	atomic.AddInt64(&rl.Count, count)
	if rl.Count > rl.MaxCount {
		rl.MaxCount = rl.Count
	}
}

func Alloc(size int) *byte {
	return (*byte)(C.malloc(C.size_t(size)))
}

func Free(ptr *byte, size int) {
	C.free(unsafe.Pointer(ptr))
}
