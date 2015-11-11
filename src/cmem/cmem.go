package cmem

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"reflect"
	"unsafe"
)
import "sync/atomic"

var (
	AllocRL ResourceLimiter
)

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

type CArray struct {
	Body []byte
	Addr uintptr
	Cap  int
}

func (arr *CArray) Alloc(size int) bool {
	if size <= MemConfig.AllocLimit {
		arr.Body = make([]byte, size)
		return true
	}

	arr.Addr = uintptr(C.malloc(C.size_t(size)))
	if arr.Addr == 0 {
		return false
	}
	AllocRL.AddSize(int64(size))
	arr.Cap = size
	sliceheader := (*reflect.SliceHeader)(unsafe.Pointer(&arr.Body))
	sliceheader.Data = arr.Addr
	sliceheader.Len = size
	sliceheader.Cap = size
	return true
}

func (arr *CArray) Free() {
	if arr.Addr != 0 {
		AllocRL.SubSize(int64(arr.Cap))
		C.free(unsafe.Pointer(arr.Addr))
		arr.Body = nil
		arr.Addr = 0
		arr.Cap = 0
	}
}

func (arr *CArray) Clear() {
	arr.Addr = 0
	arr.Body = nil
}

func (arr *CArray) Copy() (arrNew CArray, ok bool) {
	size := len(arr.Body)
	if size <= MemConfig.AllocLimit {
		arrNew.Body = make([]byte, size)
		copy(arrNew.Body, arr.Body)
		ok = true
		return
	}
	if !arrNew.Alloc(size) {
		return
	}
	ok = true
	C.memcpy(unsafe.Pointer(arrNew.Addr), unsafe.Pointer(arr.Addr), C.size_t(size))
	return
}
