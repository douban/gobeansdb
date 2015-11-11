package memcache

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"loghub"
	"net"
	"strings"
	"sync"
	"time"
)

var (
	SlowCmdTime = time.Millisecond * 100 // 100ms
	RL          *ReqLimiter
	logger      = loghub.Default
)

type ServerConn struct {
	RemoteAddr      string
	rwc             io.ReadWriteCloser // i/o connection
	closeAfterReply bool

	rbuf *bufio.Reader
	wbuf *bufio.Writer
	req  *Request
}

func newServerConn(conn net.Conn) *ServerConn {
	c := new(ServerConn)
	c.RemoteAddr = conn.RemoteAddr().String()
	c.rwc = conn

	c.rbuf = bufio.NewReader(c.rwc)
	c.wbuf = bufio.NewWriter(c.rwc)
	c.req = new(Request)
	return c
}

func (c *ServerConn) Close() {
	if c.rwc != nil {
		c.rwc.Close()
		c.rwc = nil
	}
}

func (c *ServerConn) Shutdown() {
	c.closeAfterReply = true
}

func (c *ServerConn) ServeOnce(storageClient StorageClient, stats *Stats) (err error) {
	req := c.req
	var resp *Response = nil
	defer func() {
		req.Clear()
		if resp != nil {
			resp.CleanBuffer()
		}
	}()

	err = req.Read(c.rbuf)

	t := time.Now()
	if err != nil {
		if strings.HasPrefix(err.Error(), "unknown") {
			// process non memcache commands, e.g. 'gc', 'optimize_stat'.
			status, msg, ok := storageClient.Process(req.Cmd, req.Keys)
			if ok {
				resp = new(Response)
				resp.status = status
				resp.msg = msg
				err = nil
			} else {
				logger.Errorf(err.Error())
				return
			}
		} else {
			resp = new(Response)
			resp.status = "CLIENT_ERROR"
			resp.msg = err.Error()
			err = nil
		}

	} else {
		resp, err = req.Process(storageClient, stats)
		dt := time.Since(t)
		if dt > SlowCmdTime {
			stats.UpdateStat("slow_cmd", 1)
		}
	}
	if resp == nil {
		if err == nil {
			return fmt.Errorf("nil resp")
		}
	}
	if !resp.noreply {
		if err = resp.Write(c.wbuf); err != nil {
			return
		}
		if err = c.wbuf.Flush(); err != nil {
			return
		}
	}
	return
}

func (c *ServerConn) Serve(storageClient StorageClient, stats *Stats) (e error) {
	for !c.closeAfterReply {
		e = c.ServeOnce(storageClient, stats)
		if e != nil {
			logger.Debugf("conn err: %s", e.Error())
			break
		}
	}
	c.Close()
	return
}

type Server struct {
	sync.Mutex
	addr  string
	l     net.Listener
	store StorageClient
	conns map[string]*ServerConn
	stats *Stats
	stop  bool
}

func NewServer(store Storage) *Server {
	s := new(Server)
	s.store = store.Client()
	s.conns = make(map[string]*ServerConn, 1024)
	s.stats = NewStats()
	return s
}

func (s *Server) Listen(addr string) (e error) {
	s.addr = addr
	s.l, e = net.Listen("tcp", addr)
	return
}

func (s *Server) Serve() (e error) {
	InitTokens()
	if s.l == nil {
		return errors.New("no listener")
	}
	for {
		rw, e := s.l.Accept()
		if e != nil {
			log.Print("Accept failed: ", e)
			return e
		}
		if s.stop {
			break
		}
		c := newServerConn(rw)
		go func() {
			s.Lock()
			s.conns[c.RemoteAddr] = c
			s.stats.curr_connections++
			s.stats.total_connections++
			s.Unlock()

			c.Serve(s.store, s.stats)

			s.Lock()
			s.stats.curr_connections--
			delete(s.conns, c.RemoteAddr)
			s.Unlock()
		}()
	}
	s.l.Close()
	// wait for connections to close
	for i := 0; i < 20; i++ {
		s.Lock()
		if len(s.conns) == 0 {
			return nil
		}
		s.Unlock()
		time.Sleep(1e8)
	}
	log.Print("shutdown ", s.addr, "\n")
	return nil
}

func (s *Server) Shutdown() {
	s.stop = true

	// try to connect
	net.Dial("tcp", s.addr)

	// notify conns
	s.Lock()
	defer s.Unlock()
	if len(s.conns) > 0 {
		// log.Print("have ", len(s.conns), " active connections")
		for _, conn := range s.conns {
			// log.Print(s)
			conn.Shutdown()
		}
	}
	//s.Unlock()
}
