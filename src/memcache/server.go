package memcache

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

var (
	SlowCmdTime = time.Millisecond * 100 // 100ms
	RL          *ReqLimiter
)

type ServerConn struct {
	RemoteAddr      string
	rwc             io.ReadWriteCloser // i/o connection
	closeAfterReply bool
}

func newServerConn(conn net.Conn) *ServerConn {
	c := new(ServerConn)
	c.RemoteAddr = conn.RemoteAddr().String()
	c.rwc = conn
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

func (c *ServerConn) Serve(storageClient StorageClient, stats *Stats) (e error) {
	rbuf := bufio.NewReader(c.rwc)
	wbuf := bufio.NewWriter(c.rwc)
	req := new(Request)
	var resp *Response
	for {
		e = req.Read(rbuf)
		if e != nil {
			if strings.HasPrefix(e.Error(), "unknown") {
				status, msg, ok := storageClient.Process(req.Cmd, req.Keys)
				if ok {
					resp = new(Response)
					resp.status = status
					resp.msg = msg
					if resp.Write(wbuf) != nil || wbuf.Flush() != nil {
						break
					}
					req.Clear()
					resp.CleanBuffer()
				}
			}
			break
		} else {
			t := time.Now()
			resp, _ = req.Process(storageClient, stats)
			if resp == nil {
				break
			}
			dt := time.Since(t)
			if dt > SlowCmdTime {
				stats.UpdateStat("slow_cmd", 1)
			}

			if !resp.noreply {
				if resp.Write(wbuf) != nil || wbuf.Flush() != nil {
					break
				}
			}
		}
		req.Clear()
		resp.CleanBuffer()
		if c.closeAfterReply {
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
