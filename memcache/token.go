package memcache

import (
	"sync/atomic"
	"time"

	"github.intra.douban.com/coresys/gobeansdb/config"
)

type ReqHistoy struct {
	Cmd        string
	Keys       []string
	WaitTime   time.Duration
	ServeStart time.Time
	ServeTime  time.Duration
	Working    bool
}

type ReqLimiter struct {
	Chan      chan int   `json:"-"`
	Owners    []*Request `json:"-"`
	Histories []ReqHistoy

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
	rl.Histories = make([]ReqHistoy, n)
	return rl
}

func (rl *ReqLimiter) Get(req *Request) {
	st := time.Now()
	atomic.AddInt32(&(rl.NumWait), 1)
	t := <-rl.Chan
	req.Token = t

	req.Working = true
	//logger.Debugf("get %d", t)
	atomic.AddInt32(&rl.NumWait, -1)
	ed := time.Now()

	d := ed.Sub(st)

	rl.Histories[t] = ReqHistoy{
		Cmd:        req.Cmd,
		Keys:       req.Keys,
		ServeStart: time.Now(),
		WaitTime: d,
		Working:    true,
	}
	if d > rl.MaxWait {
		rl.MaxWait = d
	}
	rl.Owners[t] = req

	return
}

func (rl *ReqLimiter) Put(req *Request) {
	t := req.Token
	//logger.Debugf("put %d", t)
	rl.Histories[t].ServeTime = time.Since(rl.Histories[t].ServeStart)
	rl.Histories[t].Working = false
	rl.Owners[t].Working = false
	rl.Chan <- t
}

func InitTokens() {
	n := config.MCConf.MaxReq
	if n == 0 {
		n = 16
	}
	RL = NewReqLimiter(n)
}
