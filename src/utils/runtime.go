package utils

import (
	"fmt"
	"runtime"
)

func GetStack(bytes int) string {
	b := make([]byte, bytes)
	all := false
	n := runtime.Stack(b, all)
	return string(b[:n])
}

func PanicToError(s string) (err error) {
	if e := recover(); e != nil {
		switch t := e.(type) {
		case error:
			return fmt.Errorf("%s panic with err(%s), stack: %s", s, t.Error(), GetStack(1000))
		default:
			return fmt.Errorf("%s panic with non-err(%#v), stack: %s", s, t, GetStack(1000))
		}
	}
	return nil
}
