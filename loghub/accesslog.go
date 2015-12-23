package loghub

import (
	"io"
	"log"
	"os"
	"sync/atomic"
	"unsafe"
)

var (
	AccessLogFormat = "%s"
	AccessLogFlag   = (log.Ldate | log.Ltime | log.Lmicroseconds)
	AccessLog       *Logger
)

type AccessLogHub struct {
	logger *log.Logger
	logFd  *os.File
}

func init() {
	AccessLog = NewLogger("", nil, DEBUG)
}

func InitAccessLog(path string, level int) (err error) {
	if accessLog, accesssFd, err := openLog(path, AccessLogFlag); err == nil {
		hub := &AccessLogHub{logger: accessLog, logFd: accesssFd}
		AccessLog.Hub = hub
		AccessLog.SetLevel(level)
	} else {
		log.Fatalf("open accesss log error, path=[%s], err=[%s]", path, err.Error())
	}
	return
}

func (hub *AccessLogHub) Log(name string, level int, file string, line int, msg string) {
	hub.logger.Printf(AccessLogFormat, msg)
	if level == FATAL {
		os.Exit(1)
	}
}

func (hub *AccessLogHub) Reopen(path string) (success bool, err error) {
	// start swap exist logger and new logger, and Close the older fd in later
	accessLog, accessFd, err := openLog(path, AccessLogFlag)
	if err == nil {
		success = true
		accessLog = (*log.Logger)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&hub.logger)), unsafe.Pointer(accessLog)))
		accessFd = (*os.File)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&hub.logFd)), unsafe.Pointer(accessFd)))
		if e := accessFd.Close(); e != nil {
			log.Println("close the old accesslog fd failure with, ", e)
		}
	} else {
		log.Printf("open accesslog %s failed: %s", path, err.Error())
	}
	return
}

func (hub *AccessLogHub) GetLastLog() []byte {
	// not implement
	return nil
}

func (hub *AccessLogHub) DumpBuffer(all bool, out io.Writer) {
	// not implement
}
