package memcache

import (
	"cmem"
	"sync/atomic"
	"time"
)

type ReqLimiter struct {
	Chan   chan int
	Owners []*Request

	// TODO: more!
	NumWait int32
	MaxWait time.Duration
}

func NewReqLimiter(n int) *ReqLimiter {
	rl := &ReqLimiter{}
	rl.Chan = make(chan int, n)
	for i := 0; i < n; i++ {
		rl.Chan <- i
	}
	rl.Owners = make([]*Request, n)
	return rl
}

func (rl *ReqLimiter) Get(req *Request) (t int) {
	st := time.Now()
	atomic.AddInt32(&(rl.NumWait), 1)
	t = <-rl.Chan
	atomic.AddInt32(&rl.NumWait, -1)
	d := time.Since(st)
	if d > rl.MaxWait {
		rl.MaxWait = d
	}
	rl.Owners[t] = req
	return
}

func (rl *ReqLimiter) Put(t int) {
	rl.Chan <- t
	rl.Owners[t] = nil
}

func InitTokens() {
	n := cmem.GConfig.NumReqToken
	if n < 16 {
		n = 16
	}
	RL = NewReqLimiter(n)
}
