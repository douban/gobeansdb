package utils

import "testing"

func TestSizer(t *testing.T) {

	s := SizeToStr(100)
	if s != "100" {
		t.Fatal(s)
	}
	s = SizeToStr(1024)
	if s != "1K" {
		t.Fatal(s)
	}
	s = SizeToStr(1025)
	if s != "1025" {
		t.Fatal(s)
	}
	s = SizeToStr(1025 * 1024)
	if s != "1025K" {
		t.Fatal(s)
	}

	n := StrToSize("1")
	if n != 1 {
		t.Fatal(n)
	}
	n = StrToSize("1K")
	if n != 1024 {
		t.Fatal(n)
	}
	if LastSizeErr != nil {
		t.Fatal(LastSizeErr)
	}
	n = StrToSize("1p")
	if LastSizeErr == nil {
		t.Fatal(n)
	}
}
