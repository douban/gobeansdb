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
	userlogger := New(name, hub, WARN)
	hub.Bind(name, &DemoHubConfig{backend})

	userlogger.Fatalf("fatal")
	userlogger.Debugf("debug")
	exp := "2015/09/01 18:09:40 FATAL (log_test.go:  18) - fatal\n"
	res := w.String()
	if len(exp) != len(res) || exp[20:] != res[20:] {
		t.Errorf("\nwant: [%s]\ngot : [%s]", exp, res)
	}
}
