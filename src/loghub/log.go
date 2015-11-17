package loghub

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
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
}

type Logger struct {
	mu       sync.Mutex
	name     string
	minLevel int
	hub      LogHub
	BufferLog
}

func (l *Logger) SetLevel(level int) {
	l.minLevel = level
}

func New(name string, hub LogHub, minLevel, buffersize int) *Logger {
	l := &Logger{name: name, hub: hub, minLevel: minLevel}
	l.InitBuffer(buffersize)
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
	l.hub.Log(l.name, level, file, line, msg)

	bufline := &BufferLine{time.Now(), level, file, line, msg}
	l.Add(bufline)
	l.Last[level] = bufline
	if level == FATAL {
		os.Exit(1)
	}
}

type BufferLine struct {
	TS    time.Time
	Level int
	File  string
	Line  int
	Msg   string
}

type queue struct {
	head   int
	Buffer []*BufferLine
}

func (q *queue) Add(line *BufferLine) {
	q.Buffer[q.head] = line
	q.head += 1
	if q.head >= len(q.Buffer) {
		q.head = 0
	}
}

func (q *queue) DumpBuffer(out io.Writer) {
	i := q.head
	for j := 0; j < len(q.Buffer); j++ {
		line := q.Buffer[i]
		if line != nil {
			out.Write([]byte(fmt.Sprintf("%v\n", line)))
		}
		i += 1
		if i >= len(q.Buffer) {
			i = 0
		}
	}
}

type BufferLog struct {
	sync.Mutex
	head   int
	Buffer []*BufferLine

	all  queue
	warn queue
	Last [FATAL + 1]*BufferLine
}

func (l *BufferLog) InitBuffer(size int) {
	l.all.Buffer = make([]*BufferLine, size)
	l.warn.Buffer = make([]*BufferLine, size)
}

func (l *BufferLog) DumpBuffer(all bool, out io.Writer) {
	l.Lock()
	defer l.Unlock()
	if all {
		l.all.DumpBuffer(out)
	} else {
		l.warn.DumpBuffer(out)
	}
}

func (l *BufferLog) Add(line *BufferLine) {
	l.Lock()
	defer l.Unlock()
	l.all.Add(line)
	if line.Level >= WARN {
		l.warn.Add(line)
	}
}

func (l *BufferLog) GetLast() []byte {
	b, _ := json.Marshal(l.Last[:])
	return b
}
