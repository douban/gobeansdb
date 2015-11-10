package cmem

import "testing"

func TestCmem(t *testing.T) {
	size := 1024 * 1024 * 10
	s := Alloc(size)
	Free(s, size)
}
