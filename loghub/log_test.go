package loghub

import (
	"bytes"
	"log"
	"testing"
)

func TestDemoHubger(t *testing.T) {
	w := new(bytes.Buffer)

	hub := NewDemoHub()
	backend := log.New(w, "", log.LstdFlags)
	name := "testSimple"

	userlogger := NewLogger(name, hub, WARN)
	hub.Bind(name, &DemoHubConfig{backend})

	userlogger.Errorf("error")
	userlogger.Debugf("debug")
	exp := "2015/09/01 18:09:40 ERROR (log_test.go:  19) - error\n"
	res := w.String()
	if len(exp) != len(res) || exp[20:] != res[20:] {
		t.Errorf("\nwant: [%s]\ngot : [%s]", exp, res)
	}
}
