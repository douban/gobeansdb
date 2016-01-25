package utils

import (
	"runtime"
	"syscall"
)

func GetStack(bytes int) string {
	b := make([]byte, bytes)
	all := false
	n := runtime.Stack(b, all)
	return string(b[:n])
}

func GetMaxRSS() int64 {
	return Getrusage().Maxrss
}

func Getrusage() syscall.Rusage {
	var rusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	// Mac OS (darwin) returns the RSS in bytes, Linux returns it in kilobytes (Check man getrusage).
	if runtime.GOOS == "darwin" {
		// Change Maxrss unit to kilobytes
		rusage.Maxrss = rusage.Maxrss / 1024
	}
	return rusage
}
