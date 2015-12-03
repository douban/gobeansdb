package memcache

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.intra.douban.com/coresys/gobeansdb/config"
	"github.intra.douban.com/coresys/gobeansdb/loghub"
	"github.intra.douban.com/coresys/gobeansdb/utils"
)

var (
	SlowCmdTime = time.Millisecond * 100 // 100ms
	RL          *ReqLimiter
	logger      = loghub.Default
	conf        = &config.DB
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
		if e := recover(); e != nil {
			logger.Errorf("mc panic(%#v), cmd %s, keys %v, stack: %s",
				e, req.Cmd, req.Keys, utils.GetStack(1000))
		}
		req.Clear()
		if resp != nil {
			resp.CleanBuffer()
		}
		if req.Working {
			RL.Put(req)
		}
	}()

	// 关于错误处理
	//
	// 目前这里能看到的错误主要有以下 3 类:
	// 1. Client 输入的格式错误(不满足 memcache 协议): 即 req.Read 里面解析命令时遇到的错误，
	//    这类错误 server 给客户端返回错误信息即可，然后继续尝试读取下一个命令。
	// 2. 网络连接错误: 即 server 在读取需要数据的时候遇到 Unexpected EOF，或者 server 写数据
	//    时失败，这类错误直接关闭连接。
	// 3. storageClient 里面错误: 这些错误应该在相应的 Process 函数里面处理掉，
	//    并设置好相应的 status 和 msg，在这里只是把处理后的结果返回给客户端即可。

	err = req.Read(c.rbuf)
	t := time.Now()

	if err != nil {
		if req.Item != nil {
			req.Item.CArray.Free()
		}
		if err == ErrNetworkError {
			// process client connection related error
			c.Shutdown()
			return nil
		} else if err == ErrNonMemcacheCmd {
			// process non memcache commands, e.g. 'gc', 'optimize_stat'.
			status, msg := storageClient.Process(req.Cmd, req.Keys)
			resp = new(Response)
			resp.status = status
			resp.msg = msg
			err = nil
		} else if err == ErrOOM {
			resp = new(Response)
			resp.status = "NOT_STORED"
			err = nil
		} else {
			// process client command format related error
			resp = new(Response)
			resp.status = "CLIENT_ERROR"
			resp.msg = err.Error()
			err = nil
		}
	} else {
		// process memcache commands, e.g. 'set', 'get', 'incr'.
		resp, _ = req.Process(storageClient, stats)
		dt := time.Since(t)
		if dt > SlowCmdTime {
			stats.UpdateStat("slow_cmd", 1)
		}
		if resp == nil {
			// quit\r\n command
			c.Shutdown()
			return nil
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
			logger.Infof("Accept failed: ", e)
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
	logger.Infof("mc server %s shutdown ", s.addr)
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
		for _, conn := range s.conns {
			conn.Shutdown()
		}
	}
	//s.Unlock()
}
