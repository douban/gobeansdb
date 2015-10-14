package loghub

import "runtime"

func GetStack(size int) string {
	b := make([]byte, size)
	n := runtime.Stack(b, false)
	stack := string(b[:n])
	return stack
}
