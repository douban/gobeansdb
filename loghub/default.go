package loghub

import (
	"log"
	"os"
)

var (
	Default       *Logger
	DefaultFormat = "%s %15s:%4d - %s"
)

func init() {
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	defauthub := &DefaultHub{logger: logger}
	Default = New("", defauthub, DEBUG, 200)
}

type DefaultHub struct {
	logger *log.Logger
}

func (l *DefaultHub) Log(name string, level int, file string, line int, msg string) {
	l.logger.Printf(DefaultFormat, levelString[level], file[:len(file)-3], line, msg)
}

func SetDefault(path string, level int, bufferSize int) {
	hub := &DefaultHub{}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("fai to to open log %s", path)
	}
	hub.logger = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
	Default.hub = hub
	Default.SetLevel(level)
	Default.InitBuffer(bufferSize)
}
