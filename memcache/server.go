package memcache

import (
	"bufio"
	"errors"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"sync/atomic"

	"github.com/douban/gobeansdb/config"
	"github.com/douban/gobeansdb/loghub"
	"github.com/douban/gobeansdb/utils"
)

var (
	SlowCmdTime    = time.Millisecond * 100 // 100ms
	RL             *ReqLimiter
	logger         = loghub.ErrorLogger
	accessLogger   = loghub.AccessLogger
	analysisLogger = loghub.AnalysisLogger
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

func overdue(recvtime, now time.Time) bool {
	return now.Sub(recvtime) > time.Duration(config.MCConf.TimeoutMS)*time.Millisecond
}

func (c *ServerConn) ServeOnce(storageClient StorageClient, stats *Stats) (err error) {
	req := c.req
	var resp *Response = nil
	defer func() {
		storageClient.Clean()
		if e := recover(); e != nil {
			logger.Errorf("mc panic(%#v), cmd %s, keys %v, stack: %s",
				e, req.Cmd, req.Keys, utils.GetStack(2000))
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
	readTimeout := false

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
			resp.Status = status
			resp.Msg = msg
			err = nil
		} else if err == ErrOOM {
			resp = new(Response)
			resp.Status = "NOT_STORED"
			err = nil
		} else {
			// process client command format related error
			resp = new(Response)
			resp.Status = "CLIENT_ERROR"
			resp.Msg = err.Error()
			err = nil
		}
	} else if overdue(req.ReceiveTime, t) {
		req.SetStat("recv_timeout")
		resp = new(Response)
		resp.Status = "RECV_TIMEOUT"
		resp.Msg = "recv_timeout"
		readTimeout = true
		logger.Errorf("recv_timeout cmd %s, keys %v", req.Cmd, req.Keys)
	} else {
		// 由于 set 等命令的 req.Item.Body 释放时间不确定，所以这里提前记录下 Body 的大小，
		// 后面在记录 access log 时会用到
		bodySize := 0
		if req.Item != nil {
			bodySize = len(req.Item.Body)
		}

		// process memcache commands, e.g. 'set', 'get', 'incr'.
		req.SetStat("process")
		resp, err = req.Process(storageClient, stats)
		dt := time.Since(t)
		if dt > SlowCmdTime {
			atomic.AddInt64(&(stats.slow_cmd), 1)
		}
		if resp == nil {
			// quit\r\n command
			c.Shutdown()
			return nil
		}

		if accessLogger.Hub != nil {
			c.writeAccessLog(resp, bodySize, err, dt, storageClient.GetSuccessedTargets())
		}
	}

	if !resp.Noreply {
		if !readTimeout && overdue(req.ReceiveTime, time.Now()) {
			req.SetStat("process_timeout")
			resp.CleanBuffer()
			resp = new(Response)
			resp.Status = "PROCESS_TIMEOUT"
			resp.Msg = "process_timeout"
			logger.Errorf("process_timeout cmd %s, keys %v", req.Cmd, req.Keys)
			return
		}

		req.SetStat("resp")
		if err = resp.Write(c.wbuf); err != nil {
			return
		}
		if err = c.wbuf.Flush(); err != nil {
			return
		}
	}

	return
}

// 记录 accesslog, 主要用于 proxy 中
func (c *ServerConn) writeAccessLog(resp *Response, bodySize int, processErr error, dt time.Duration, hosts []string) {
	req := c.req
	cmd := req.Cmd
	totalSize := 0
	sizeStr := "0"
	stat := "SUCC"

	switch req.Cmd {
	case "get", "gets":
		if len(req.Keys) > 1 {
			cmd += "m"
			sizes := make([]string, 0, len(req.Keys))
			for _, k := range req.Keys {
				s := 0
				if v, ok := resp.Items[k]; ok {
					s = len(v.Body)
				}
				totalSize += s
				sizes = append(sizes, strconv.Itoa(s))
			}
			sizeStr = strings.Join(sizes, ",")
		} else {
			for _, v := range resp.Items {
				totalSize += len(v.Body)
			}
			sizeStr = strconv.Itoa(totalSize)
		}

		if totalSize == 0 {
			stat = "FAILED"
		}

	case "set", "add", "replace":
		sizeStr = strconv.Itoa(bodySize)
		if processErr != nil {
			stat = "FAILED"
		}

	default:
		if processErr != nil {
			stat = "FAILED"
		}
	}

	if len(hosts) == 0 {
		hosts = append(hosts, "NoWhere")
	}
	hostStr := strings.Join(hosts, ",")
	keys := strings.Join(req.Keys, " ")

	accessLogger.Infof("%s %s %s %s %s %s %d %s",
		config.AccessLogVersion, c.RemoteAddr, strings.ToUpper(cmd),
		stat, sizeStr, hostStr, dt.Nanoseconds()/1e3, keys)
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
	store Storage
	conns map[string]*ServerConn
	stats *Stats
	stop  bool
}

func NewServer(store Storage) *Server {
	s := new(Server)
	s.store = store
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
			logger.Infof("Accept failed: %s", e)
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

			c.Serve(s.store.Client(), s.stats)

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

func (s *Server) HandleSignals(errorlog string, accesslog string, analysislog string) {
	sch := make(chan os.Signal, 10)
	signal.Notify(sch, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT,
		syscall.SIGHUP, syscall.SIGSTOP, syscall.SIGQUIT, syscall.SIGUSR1)
	go func(ch <-chan os.Signal) {
		for {
			sig := <-ch
			// SIGUSR1 信号是 logrotate 程序发送给的，表示已经完成了 roate 任务，
			// 通知 server 重新打开新的日志文件
			if sig == syscall.SIGUSR1 {
				// logger.Hub is always inited, so we call Reopen without check it.
				logger.Hub.Reopen(errorlog)

				if accesslog != "" && accessLogger != nil && accessLogger.Hub != nil {
					if err := accessLogger.Hub.Reopen(accesslog); err != nil {
						logger.Warnf("open accessLogger %s failed: %s", accesslog, err.Error())
					}
				}

				if analysislog != "" && analysisLogger != nil && analysisLogger.Hub != nil {
					if err := analysisLogger.Hub.Reopen(analysislog); err != nil {
						logger.Warnf("open analysisLogger %s failed: %s", analysislog, err.Error())
					}
				}
			} else {
				logger.Infof("signal received " + sig.String())
				s.Shutdown()
			}
		}
	}(sch)
}
