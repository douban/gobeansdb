package loghub

import "log"
import "os"

var (
	Default       *Logger
	defaultFormat = "%s %15s:%4d - %s"
)

func init() {
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	Default = New("", &defaultHub{logger}, DEBUG)
}

type defaultHub struct {
	logger *log.Logger
}

func (l *defaultHub) Log(name string, level int, file string, line int, msg string) {
	l.logger.Printf(defaultFormat, levelString[level], file[:len(file)-3], line, msg)
}
