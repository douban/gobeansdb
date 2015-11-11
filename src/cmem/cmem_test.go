package cmem

import "testing"

func TestCmem(t *testing.T) {
	size := 1024 * 1024 * 10
	var arr CArray
	if !arr.Alloc(size) {
		t.Fatalf("fail to alloc")
	}
	arr.Free()
}
