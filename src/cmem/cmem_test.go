package cmem

import (
	"runtime"
	"testing"
)

func TestCmem(t *testing.T) {
	size := uintptr(1024 * 1024 * 10)
	s := Alloc(size)
	Free(s, size)
	if Alloced() != 0 {
		runtime.Gosched()
		if Alloced() != 0 {
			t.Error("memory leak")
		}
	}
}
