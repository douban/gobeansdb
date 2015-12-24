package loghub

import (
	"fmt"
	"io"
	"runtime"
	"sync"
)

const (
	DEBUG = iota
	INFO
	WARN
	ERROR
	FATAL
)

var (
	levelString = []string{"DEBUG", "INFO ", "WARN ", "ERROR", "FATAL"}
)

// LogHub as a centry point in main, to control log from diff pacakage
// enable:
// 1. change log lib (of all package) easily, just neet a wrapper
// 2. append level header and file:line before log
// 3. preproces before logging, e.g. :
//    a. buffer recent errs with diff "file:line"
//    b. sent error to network
// 4. logHub store the config (loglib specified) for diff "name",
//    and use it when receiving a log, simulating getLogger("name")
// 5. provide a Default, so don`t bother to choose a "right" log implementation at beginning
// 6. keep simple and lightweitght

type LogHub interface {
	Log(name string, level int, file string, line int, msg string)
	Reopen(path string) error
	GetLastLog() []byte
	DumpBuffer(all bool, out io.Writer)
}

type Logger struct {
	mu       sync.Mutex
	name     string
	minLevel int
	Hub      LogHub
}

func (l *Logger) SetLevel(level int) {
	l.minLevel = level
}

func NewLogger(name string, hub LogHub, minLevel int) *Logger {
	l := &Logger{name: name, Hub: hub, minLevel: minLevel}
	return l
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	l.Logf(DEBUG, format, v...)
}

func (l *Logger) Infof(format string, v ...interface{}) {
	l.Logf(INFO, format, v...)
}

func (l *Logger) Warnf(format string, v ...interface{}) {
	l.Logf(WARN, format, v...)
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.Logf(ERROR, format, v...)
}

func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.Logf(FATAL, format, v...)
}

func (l *Logger) Logf(level int, format string, v ...interface{}) {
	if level < l.minLevel {
		return
	}
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "???"
		line = 0
	} else {
		short := file
		for i := len(file) - 1; i > 0; i-- {
			if file[i] == '/' {
				short = file[i+1:]
				break
			}
		}
		file = short
	}
	msg := fmt.Sprintf(format, v...)
	l.Hub.Log(l.name, level, file, line, msg)
}
