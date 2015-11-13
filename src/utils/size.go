package utils

import (
	"reflect"
	"strconv"
	"strings"
)

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

func InitSizesPointer(c interface{}) (err error) {
	LastSizeErr = nil
	m := reflect.ValueOf(c).Elem()
	InitSizesForValue(m)
	err = LastSizeErr
	LastSizeErr = nil
	return
}

func InitSizesForValue(m reflect.Value) (err error) {
	t := m.Type()
	n := m.NumField()
	for i := 0; i < n; i++ {
		f := t.Field(i)
		//log.Printf("%#v", f.Name)

		if strings.HasSuffix(f.Name, "Config") { // nested config
			InitSizesForValue(m.Field(i))
		} else if strings.HasSuffix(f.Name, "Str") {
			str := m.Field(i).String()
			value := StrToSize(str)
			f2 := m.FieldByName(f.Name[:len(f.Name)-3])
			f2.SetInt(value)
		}
	}
	return
}
