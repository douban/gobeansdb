package store

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"syscall"
)

var (
	doProf bool
)

func FreeMem() {
	runtime.GC()
	debug.FreeOSMemory()
}

var (
	profDir string
)

func GetMaxRSS() int64 {
	var rusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	return rusage.Maxrss
}

func StartCpuProfile(name string) *os.File {
	if !doProf {
		return nil
	}
	name = fmt.Sprintf("%s.cpu", name)
	w, err := os.Create(filepath.Join(profDir, name))
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(w)
	return w
}

func StopCpuProfile(f *os.File) {
	if !doProf {
		return
	}
	pprof.StopCPUProfile()
	f.Close()
}

func WriteHeapProfile(name string) {
	name = fmt.Sprintf("%s.heap", name)
	f, err := os.Create(filepath.Join(profDir, name))
	if err != nil {
		log.Fatal(err)
	}
	pprof.WriteHeapProfile(f)
	f.Close()
}
