package loghub

import (
	"log"
	"os"
	"time"
)

var (
	AnalysisLogFormat = "%s"
	AnalysisLogFlag   = log.LstdFlags | log.Lmicroseconds
	AnalysisLogger    *Logger
)

type AnalysisLogHub struct {
	logger *log.Logger
	logFd  *os.File
	BufferLog
}

func init() {
	//logger := openLogWithFd(os.Stderr, AnalysisLogFlag)
	//hub := &AnalysisLogHub{logger: logger}
	//hub.InitBuffer(200)
	AnalysisLogger = NewLogger("", nil, DEBUG)
}

func InitAnalysisLog(path string, level int, bufferSize int) (err error) {
	if analysisLog, analysisFd, err := openLog(path, AnalysisLogFlag); err == nil {
		hub := &AnalysisLogHub{logger: analysisLog, logFd: analysisFd}
		hub.InitBuffer(bufferSize)
		AnalysisLogger.Hub = hub
		AnalysisLogger.SetLevel(level)
	} else {
		log.Fatalf("open log error, path=[%s], err=[%s]", path, err.Error())
	}
	return
}

func (hub *AnalysisLogHub) Log(name string, level int, file string, line int, msg string) {
	hub.logger.Printf(AnalysisLogFormat, msg)
	bufline := &BufferLine{time.Now(), level, file, line, msg}
	hub.Add(bufline)
	hub.Lock()
	hub.Last[level] = bufline
	hub.Unlock()
	if level == FATAL {
		os.Exit(1)
	}
}

func (hub *AnalysisLogHub) Reopen(path string) (err error) {
	return reopenLogger(&hub.logger, &hub.logFd, path, AnalysisLogFlag)
}
