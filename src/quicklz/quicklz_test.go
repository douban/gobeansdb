package quicklz

import (
	"testing"
)

func TestQuicklz(t *testing.T) {
	//example from http://www.quicklz.com/manual.html
	orig := "LZ compression is based on finding repeated strings: Five, six, seven, eight, nine, fifteen, sixteen, seventeen, fifteen, sixteen, seventeen."
	compressed := Compress([]byte(orig), 3)
	l := len(orig)
	lc := len(compressed)
	if lc != 116 {
		t.Errorf("wrong compressed len %s", lc)
	}
	s := sizeDecompressed(compressed)
	sc := sizeCompressed(compressed)
	if s != l || sc != lc {
		t.Errorf("bad size meta: %d != %d  or %d != %d", s, l, sc, lc)
	}
	decompressed := Decompress(compressed)
	ld := len(decompressed)
	if ld != l {
		t.Errorf("wrong decompressed len %s", lc)
	}
	compressed2 := CCompress([]byte(orig))
	decompressed = CDecompress(compressed2)
	if string(decompressed) != orig {
		t.Errorf("cc fail %s", lc)
	}
}
