package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/douban/gobeansdb/loghub"
)

func Remove(path string) error {
	loghub.ErrorLogger.Logf(loghub.INFO, "remove path: %s", path)
	return os.Remove(path)
}

func Rename(path, newpath string) error {
	loghub.ErrorLogger.Logf(loghub.INFO, "rename path: %s to %s", path, newpath)
	return os.Rename(path, newpath)
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

type DiskStatus struct {
	Root    string
	All     int64
	Used    int64
	Free    int64
	Buckets []int `yaml:",flow"`
}

func DiskUsage(path string) (disk DiskStatus, err error) {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	realpath, err := filepath.EvalSymlinks(abspath)
	if err != nil {
		return
	}
	realpath = filepath.Clean(realpath)

	fs := syscall.Statfs_t{}
	err = syscall.Statfs(realpath, &fs)
	if err != nil {
		return
	}
	parts := strings.Split(realpath, "/")
	if len(parts) < 2 {
		err = fmt.Errorf("bad path <%s>", path)
		return
	}
	disk.Root = "/" + parts[1]
	disk.All = int64(fs.Blocks) * int64(fs.Bsize)
	disk.Free = int64(fs.Bfree) * int64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	return
}

func DirUsage(path string) (size int64, err error) {
	size = 0
	f, err := os.Open(path)
	if err != nil {
		size = -1
		return
	}
	fis, err := f.Readdir(-1)
	if err != nil {
		return
	}
	for _, fi := range fis {
		size += fi.Size()
	}
	return
}
