package cmem

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/douban/gobeansdb/config"
)

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

func (rl *ResourceLimiter) IsZero() bool {
	return rl.Count == 0 && rl.Size == 0
}

func (rl *ResourceLimiter) AddSizeAndCount(size int) {
	rl.AddSize(size)
	rl.AddCount(1)
}

func (rl *ResourceLimiter) SubSizeAndCount(size int) {
	rl.SubSize(size)
	rl.SubCount(1)
}

func (rl *ResourceLimiter) AddSize(size int) {
	atomic.AddInt64(&rl.Size, int64(size))
	if rl.Size > rl.MaxSize {
		rl.MaxSize = rl.Size
	}
}

func (rl *ResourceLimiter) SubSize(size int) {
	atomic.AddInt64(&rl.Size, -int64(size))
}

func (rl *ResourceLimiter) AddCount(count int) {
	atomic.AddInt64(&rl.Count, int64(count))
	if rl.Count > rl.MaxCount {
		rl.MaxCount = rl.Count
	}
}

func (rl *ResourceLimiter) SubCount(count int) {
	atomic.AddInt64(&rl.Count, -int64(count))
}

type CArray struct {
	Body []byte
	Addr uintptr
	Cap  int
}

func (arr *CArray) Alloc(size int) bool {
	if size <= int(config.MCConf.BodyInC) {
		arr.Body = make([]byte, size)
		arr.Cap = size
		arr.Addr = 0
		return true
	}

	arr.Addr = uintptr(C.malloc(C.size_t(size)))
	if arr.Addr == 0 {
		return false
	}
	AllocRL.AddSizeAndCount(size)
	arr.Cap = size
	sliceheader := (*reflect.SliceHeader)(unsafe.Pointer(&arr.Body))
	sliceheader.Data = arr.Addr
	sliceheader.Len = size
	sliceheader.Cap = size
	return true
}

func (arr *CArray) Free() {
	if arr.Addr != 0 {
		AllocRL.SubSizeAndCount(arr.Cap)
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
	if arr.Addr == 0 {
		arrNew.Body = make([]byte, size)
		copy(arrNew.Body, arr.Body)
		ok = true
		return
	}
	if !arrNew.Alloc(size) {
		return
	}
	ok = true
	copy(arrNew.Body, arr.Body)
	return
}
