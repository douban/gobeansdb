package utils

import "runtime"

func GetStack(bytes int) string {
	b := make([]byte, bytes)
	all := false
	n := runtime.Stack(b, all)
	return string(b[:n])
}
