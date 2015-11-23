package utils

import "testing"
import "os"

func TestDir(t *testing.T) {
	p := "testdir"
	err := os.Mkdir(p, 0777)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(p)

	f, err := os.OpenFile(p+"/a", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	f, err = os.OpenFile(p+"/b", os.O_CREATE|os.O_WRONLY, 0644)
	f.WriteString("abc")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	d := NewDir()
	d.Load(p)
	dir, diff1, diff2, err := d.CheckPath(p)
	if err != nil || diff1 != nil || diff2 != nil {
		t.Fatal(d.ToSlice(), dir.ToSlice(), diff1, diff2, err)
	}
	d.Delete("a")
	d.Set("b", 1)
	d.Set("c", 2)
	dir, diff1, diff2, err = d.CheckPath(p)
	if err != nil || diff1[0].Size != 1 || diff1[1].Size != 2 || diff2[0].Size != 0 || diff2[1].Size != 3 {
		t.Fatal(d.ToSlice(), dir.ToSlice(), diff1, diff2, err)
	}
}
