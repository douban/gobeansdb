package loghub

import (
	"log"
	"os"
	"runtime"
	"sync/atomic"
	"unsafe"
)

func GetStack(size int) string {
	b := make([]byte, size)
	n := runtime.Stack(b, false)
	stack := string(b[:n])
	return stack
}

func openLogWithFd(fd *os.File, logFlag int) *log.Logger {
	return log.New(fd, "", logFlag)
}

func openLog(path string, logFlag int) (logger *log.Logger, fd *os.File, err error) {
	if fd, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
		logger = openLogWithFd(fd, logFlag)
	}
	return
}

func reopenLogger(logger **log.Logger, fd **os.File, path string, logFlag int) (err error) {
	// start swap exist logger and new logger, and Close the older fd in later
	newLogger, newFd, err := openLog(path, logFlag)
	if err == nil {
		atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(logger)), unsafe.Pointer(newLogger))
		oldFd := (*os.File)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(fd)), unsafe.Pointer(newFd)))
		if e := oldFd.Close(); e != nil {
			log.Println("close old log fd failure with, ", e)
		}
	} else {
		log.Printf("reopenLogger %s failed: %s", path, err.Error())
	}
	return
}
