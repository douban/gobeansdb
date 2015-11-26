package loghub

import "log"

type DemoHubConfig struct {
	logger *log.Logger
}

type DemoHub struct {
	configs map[string]*DemoHubConfig
}

func NewDemoHub() *DemoHub {
	s := new(DemoHub)
	s.configs = make(map[string]*DemoHubConfig)
	return s
}

func (l *DemoHub) Log(name string, level int, file string, line int, msg string) {
	l.configs[name].logger.Printf("%s (%10s:%4d) - %s", levelString[level], file, line, msg)
}

func (l *DemoHub) Bind(name string, config *DemoHubConfig) {
	l.configs[name] = config
}
