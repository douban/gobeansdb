package utils

import "strconv"

const (
	K = 1024
	M = 1024 * K
	G = 1024 * M
)

var (
	LastSizeErr error
	sizemap     = map[byte]int{'K': K, 'k': K, 'M': M, 'm': M, 'G': G, 'g': G}
)

func StrToSize(s string) (n int64) {
	l := len(s)
	if l == 0 {
		return 0
	}
	u := s[l-1]
	plus, found := sizemap[u]
	if found {
		s = s[:l-1]
	}
	n, e := strconv.ParseInt(s, 10, 32)
	if e != nil {
		LastSizeErr = e
	} else if found {
		n *= int64(plus)
	}
	return
}

func SizeToStr(n int64) (s string) {
	if n == 0 {
		return "0"
	}
	units := []string{"", "K", "M", "G"}
	i := 0
	for n&(1024-1) == 0 {
		n >>= 10
		i++
	}
	return strconv.FormatInt(n, 10) + units[i]
}
