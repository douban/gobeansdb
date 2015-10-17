package cmem

/*
#include <stdlib.h>
*/
import "C"
import "unsafe"

var alloced int64
var alloc_ch chan int64

func init() {
	alloc_ch = make(chan int64, 10)
	go func() {
		for i := range alloc_ch {
			alloced += i
		}
	}()
}

func Alloc(size uintptr) *byte {
	alloc_ch <- int64(size)
	return (*byte)(C.malloc(C.size_t(size)))
}

func Free(ptr *byte, size uintptr) {
	alloc_ch <- -int64(size)
	C.free(unsafe.Pointer(ptr))
}

func Alloced() int64 {
	return alloced
}
