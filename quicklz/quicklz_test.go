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
		t.Errorf("wrong compressed len %d", lc)
	}
	s := SizeDecompressed(compressed)
	sc := SizeCompressed(compressed)
	if s != l || sc != lc {
		t.Errorf("bad size meta: %d != %d  or %d != %d", s, l, sc, lc)
	}
	decompressed := Decompress(compressed)
	ld := len(decompressed)
	if ld != l {
		t.Errorf("wrong decompressed len %d", lc)
	}
	compressed2, ok := CCompress([]byte(orig))
	if !ok {
		t.Fatalf("CCompress fail")
	}
	decompressed2, err := CDecompress(compressed2.Body, s)
	if err != nil || string(decompressed2.Body) != orig {
		t.Errorf("decompress fail %d %v", lc, err)
	}
}
