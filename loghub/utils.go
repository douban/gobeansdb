package loghub

import (
	"log"
	"os"
	"runtime"
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
