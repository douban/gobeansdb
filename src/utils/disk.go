package utils

import (
	"loghub"
	"os"
	"sort"
)

func Remove(path string) error {
	loghub.Default.Logf(loghub.INFO, "remove path: %s", path)
	return os.Remove(path)
}

type Dir struct {
	Files map[string]int64
}

func NewDir() *Dir {
	d := &Dir{}
	d.Files = make(map[string]int64)
	return d
}

type File struct {
	Name string
	Size int64
}

func (f *File) isSame(f2 *File) (same bool) {
	if f.Name != f2.Name {
		return false
	}
	return (f.Size == -1 || f2.Size == -1 || f.Size == f2.Size)
}

type FileList []File

func (by FileList) Len() int      { return len(by) }
func (by FileList) Swap(i, j int) { by[i], by[j] = by[j], by[i] }
func (by FileList) Less(i, j int) bool {
	return by[i].Name < by[j].Name
}

func (d *Dir) ToSlice() []File {
	s := make([]File, 0, len(d.Files))
	for k, v := range d.Files {
		s = append(s, File{k, v})
	}
	sort.Sort(FileList(s))
	return s
}

func (d *Dir) Set(name string, size int64) {
	d.Files[name] = size
}

func (d *Dir) SetMulti(files map[string]int64) {
	for name, size := range files {
		d.Files[name] = size
	}
}

func (d *Dir) SetMultiNoSize(files ...string) {
	for _, name := range files {
		d.Files[name] = -1
	}
}

func (d *Dir) Delete(name string) {
	delete(d.Files, name)
}

func (d *Dir) Load(path string) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	files, err := f.Readdir(-1)
	if err != nil {
		return
	}
	for _, fi := range files {
		d.Files[fi.Name()] = fi.Size()
	}
	return
}

func (d *Dir) CheckPath(path string) (d2 *Dir, r1, r2 []File, err error) {
	d2 = NewDir()
	err = d2.Load(path)
	if err != nil {
		return
	}
	r1, r2 = d.Diff(d2)
	return
}

func (d *Dir) Diff(d2 *Dir) (r1, r2 []File) {
	s := d.ToSlice()
	s2 := d2.ToSlice()
	i := 0
	j := 0
	for {
		if i >= len(s) {
			r2 = append(r2, s2[j:]...)
			break
		}
		if j >= len(s2) {
			r1 = append(r1, s[i:]...)
			break
		}
		if s[i].isSame(&s2[j]) {
			i += 1
			j += 1
		} else if s[i].Name < s2[j].Name {
			r1 = append(r1, s[i])
			i++
		} else if s[i].Name > s2[j].Name {

			r2 = append(r2, s2[j])
			j++
		} else {
			r1 = append(r1, s[i])
			r2 = append(r2, s2[j])
			i += 1
			j += 1
		}
	}
	return
}
