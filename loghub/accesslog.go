package loghub

import (
	"io"
	"log"
	"os"
)

var (
	AccessLogFormat = "%s"
	AccessLogFlag   = (log.Ldate | log.Ltime | log.Lmicroseconds)
	AccessLogger    *Logger
)

type AccessLogHub struct {
	logger *log.Logger
	logFd  *os.File
}

func init() {
	AccessLogger = NewLogger("", nil, DEBUG)
}

func InitAccessLog(path string, level int) (err error) {
	if accessLog, accesssFd, err := openLog(path, AccessLogFlag); err == nil {
		hub := &AccessLogHub{logger: accessLog, logFd: accesssFd}
		AccessLogger.Hub = hub
		AccessLogger.SetLevel(level)
	} else {
		log.Fatalf("open access log error, path=[%s], err=[%s]", path, err.Error())
	}
	return
}

func (hub *AccessLogHub) Log(name string, level int, file string, line int, msg string) {
	hub.logger.Printf(AccessLogFormat, msg)
	if level == FATAL {
		os.Exit(1)
	}
}

func (hub *AccessLogHub) Reopen(path string) (err error) {
	return reopenLogger(&hub.logger, &hub.logFd, path, AccessLogFlag)
}

func (hub *AccessLogHub) GetLastLog() []byte {
	// not implement
	return nil
}

func (hub *AccessLogHub) DumpBuffer(all bool, out io.Writer) {
	// not implement
}
