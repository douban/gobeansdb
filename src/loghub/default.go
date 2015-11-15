package loghub

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)
import "os"

var (
	Default       *Logger
	DefaultFormat = "%s %15s:%4d - %s"
)

func init() {
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	defauthub := &DefaultHub{logger: logger}
	defauthub.InitBuffer(200)
	Default = New("", defauthub, DEBUG)
}

type BufferLine struct {
	TS    time.Time
	Level int
	File  string
	Line  int
	Msg   string
}

type BufferLog struct {
	sync.Mutex
	head   int
	Buffer []*BufferLine
	Last   [FATAL]*BufferLine
}

func (l *BufferLog) InitBuffer(size int) {
	l.Buffer = make([]*BufferLine, size)
}

func (l *BufferLog) DumpBuffer(out io.Writer) {
	l.Lock()
	defer l.Unlock()
	i := l.head
	for j := 0; j < len(l.Buffer); j++ {
		line := l.Buffer[i]
		if line != nil {
			out.Write([]byte(fmt.Sprintf("%v\n", line)))
		}
		i += 1
		if i >= len(l.Buffer) {
			i = 0
		}
	}
}

func (l *BufferLog) Add(line *BufferLine) {
	l.Lock()
	defer l.Unlock()
	l.Buffer[l.head] = line
	l.head += 1
	if l.head >= len(l.Buffer) {
		l.head = 0
	}
}

func (l *BufferLog) GetLast() []byte {
	b, _ := json.Marshal(l.Last[:])
	return b
}

type DefaultHub struct {
	logger *log.Logger
	BufferLog
}

func (l *DefaultHub) Log(name string, level int, file string, line int, msg string) {
	l.logger.Printf(DefaultFormat, levelString[level], file[:len(file)-3], line, msg)
	bufline := &BufferLine{time.Now(), level, file, line, msg}
	if level >= WARN {
		l.Add(bufline)
	}
	l.Last[level] = bufline
}

func SetDefault(path string, level int, bufferSize int) {
	hub := &DefaultHub{}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("fai to to open log %s", path)
	}
	hub.logger = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
	hub.InitBuffer(bufferSize)
	Default.hub = hub
	Default.SetLevel(level)
}
