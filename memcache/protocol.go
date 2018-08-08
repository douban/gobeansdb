package memcache

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/douban/gobeansdb/cmem"
	"github.com/douban/gobeansdb/config"
)

const VERSION = "0.1.0"

// Client command parsing Errors

var (
	// ErrInvalidCmd means that the number of command parts is invalid,
	// or the type of a part if invalid.
	ErrInvalidCmd = errors.New("invalid cmd")

	// ErrNonMemcacheCmd means that the command is not defined in original memcache protocal.
	// refer: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
	ErrNonMemcacheCmd = errors.New("non memcache command")

	// ErrValueTooLarge means that the value of a store command (e.g. set) is too large
	ErrValueTooLarge = errors.New("value too large")

	// ErrKeyLength means that the length of key is invalid
	ErrKeyLength = errors.New("key length error")

	// ErrBadDataChunk means that data chunk of a value is not match its size flag.
	ErrBadDataChunk = errors.New("bad data chunk")

	// ErrNetworkError means that a failure happend at reading/writing to a client connection.
	ErrNetworkError = errors.New("network error")

	ErrOOM = errors.New("memory shortage")
)

func isSpace(r rune) bool {
	return r == ' '
}

func splitKeys(s string) []string {
	// s[:len(s) - 2] remove "\r\n"
	return strings.FieldsFunc(s[:len(s)-2], isSpace)
}

type Item struct {
	ReceiveTime time.Time
	Flag        int
	Exptime     int
	Cas         int
	cmem.CArray `json:"-"`
}

func (it *Item) String() (s string) {
	return fmt.Sprintf("Item(Flag:%d, Exptime:%d, Length:%d, Cas:%d, Body:%v",
		it.Flag, it.Exptime, len(it.Body), it.Cas, it.Body)
}

type Request struct {
	ReceiveTime time.Time
	Cmd         string   // get, set, delete, quit, etc.
	Keys        []string // keys
	Item        *Item
	NoReply     bool

	Token   int
	Working bool
}

func (req *Request) String() (s string) {
	return fmt.Sprintf("Request(Cmd:%s, Keys:%v, Item:%v, NoReply: %t)",
		req.Cmd, req.Keys, &req.Item, req.NoReply)
}

func (req *Request) Clear() {
	req.NoReply = false
	if req.Item != nil {
		req.Item = nil
	}
}

func (req *Request) SetStat(stat string) {
	his := RL.Histories[req.Token]
	his.Stat = stat
	his.StatStart = time.Now()
	RL.Histories[req.Token] = his
	return
}

func WriteFull(w io.Writer, buf []byte) error {
	n, e := w.Write(buf)
	for e != nil && n > 0 {
		buf = buf[n:]
		n, e = w.Write(buf)
	}
	return e
}

func (req *Request) Write(w io.Writer) (e error) {

	switch req.Cmd {

	case "get", "gets", "delete", "quit", "version", "stats", "flush_all":
		io.WriteString(w, req.Cmd)
		for _, key := range req.Keys {
			io.WriteString(w, " "+key)
		}
		if req.NoReply {
			io.WriteString(w, " noreply")
		}
		_, e = io.WriteString(w, "\r\n")

	case "set", "add", "replace", "cas", "prepend", "append":
		noreply := ""
		if req.NoReply {
			noreply = " noreply"
		}
		item := req.Item
		if req.Cmd == "cas" {
			fmt.Fprintf(w, "%s %s %d %d %d %d%s\r\n", req.Cmd, req.Keys[0], item.Flag,
				item.Exptime, item.Cas, len(item.Body), noreply)
		} else {
			fmt.Fprintf(w, "%s %s %d %d %d%s\r\n", req.Cmd, req.Keys[0], item.Flag,
				item.Exptime, len(item.Body), noreply)
		}
		if WriteFull(w, item.Body) != nil {
			return e
		}
		e = WriteFull(w, []byte("\r\n"))

	case "incr", "decr":
		io.WriteString(w, req.Cmd)
		fmt.Fprintf(w, " %s %s", req.Keys[0], string(req.Item.Body))
		if req.NoReply {
			io.WriteString(w, " noreply")
		}
		_, e = io.WriteString(w, "\r\n")

	default:
		logger.Errorf("unkown request cmd: %s", req.Cmd)
		return errors.New("unknown cmd: " + req.Cmd)
	}

	return e
}

func (req *Request) Read(b *bufio.Reader) error {
	var s string
	var e error
	if s, e = b.ReadString('\n'); e != nil {
		return ErrNetworkError
	}

	if !strings.HasSuffix(s, "\r\n") {
		return ErrInvalidCmd
	}
	req.ReceiveTime = time.Now()
	parts := splitKeys(s)
	if len(parts) < 1 {
		return ErrInvalidCmd
	}

	req.Cmd = parts[0]
	switch req.Cmd {

	case "get", "gets":
		if len(parts) < 2 {
			return ErrInvalidCmd
		}
		req.Keys = parts[1:]
		RL.Get(req)

	case "set", "add", "replace", "cas", "append", "prepend":
		if len(parts) < 5 || len(parts) > 7 {
			return ErrInvalidCmd
		}
		req.Keys = parts[1:2]
		req.Item = &Item{}
		item := req.Item
		item.ReceiveTime = time.Now()
		if item.Flag, e = strconv.Atoi(parts[2]); e != nil {
			return ErrInvalidCmd
		}
		if item.Exptime, e = strconv.Atoi(parts[3]); e != nil {
			return ErrInvalidCmd
		}
		length, e := strconv.Atoi(parts[4])
		if e != nil {
			return ErrInvalidCmd
		}
		if !config.IsValidValueSize(uint32(length)) {
			return ErrValueTooLarge
		}
		if length > int(config.MCConf.BodyBig) {
			if cmem.DBRL.FlushData.Size > int64(config.MCConf.FlushMax) {
				logger.Warnf("ErrOOM key %s, size %d", req.Keys[0], length)
				return ErrOOM
			}
		}
		if req.Cmd == "cas" {
			if len(parts) < 6 {
				return ErrInvalidCmd
			}
			item.Cas, e = strconv.Atoi(parts[5])
			if len(parts) > 6 && parts[6] != "noreply" {
				return ErrInvalidCmd
			}
			req.NoReply = len(parts) > 6 && parts[6] == "noreply"
		} else {
			if len(parts) > 5 && parts[5] != "noreply" {
				return ErrInvalidCmd
			}
			req.NoReply = len(parts) > 5 && parts[5] == "noreply"
		}

		RL.Get(req)

		if !item.Alloc(length) {
			e = fmt.Errorf("fail to alloc %d", length)
			return e
		}

		cmem.DBRL.SetData.AddSizeAndCount(item.CArray.Cap)

		if _, e = io.ReadFull(b, item.Body); e != nil {
			cmem.DBRL.SetData.SubSizeAndCount(item.CArray.Cap)
			item.CArray.Free()
			return ErrNetworkError
		}

		// check ending \r\n
		c1, e1 := b.ReadByte()
		c2, e2 := b.ReadByte()
		if e1 != nil || e2 != nil {
			cmem.DBRL.SetData.SubSizeAndCount(item.CArray.Cap)
			item.CArray.Free()
			return ErrNetworkError
		}
		if c1 != '\r' || c2 != '\n' {
			cmem.DBRL.SetData.SubSizeAndCount(item.CArray.Cap)
			item.CArray.Free()
			return ErrBadDataChunk
		}

	case "delete":
		if len(parts) < 2 || len(parts) > 4 {
			return ErrInvalidCmd
		}
		req.Keys = parts[1:2]
		req.NoReply = len(parts) > 2 && parts[len(parts)-1] == "noreply"

	case "incr", "decr":
		if len(parts) < 3 || len(parts) > 4 {
			return ErrInvalidCmd
		}
		req.Keys = parts[1:2]
		req.Item = &Item{}
		req.Item.Body = []byte(parts[2])
		req.NoReply = len(parts) > 3 && parts[3] == "noreply"
		// 因为 incr/decr 也会转化为 set 命令。SetData 做减法是在写入 flush buffer
		// 的时候，那时已经分不清是 incr 还是 set，所以这里也给 incr 命令加上统计信息。
		cmem.DBRL.SetData.AddCount(1)
		RL.Get(req)

	case "stats":
		req.Keys = parts[1:]

	case "quit", "version", "flush_all":
	case "verbosity":
		if len(parts) >= 2 {
			req.Keys = parts[1:]
		}

	default:
		req.Keys = parts[1:]
		return ErrNonMemcacheCmd
	}
	return nil
}

type Response struct {
	Status  string
	Msg     string
	Cas     bool
	Noreply bool
	Items   map[string]*Item
}

func (resp *Response) String() (s string) {
	return fmt.Sprintf("Response(Status:%s, msg:%s, Items:%v)",
		resp.Status, resp.Msg, resp.Items)
}

func (resp *Response) Read(b *bufio.Reader) error {
	resp.Items = make(map[string]*Item, 1)
	for {
		s, e := b.ReadString('\n')
		if e != nil {
			logger.Errorf("read response line failed %v", e)
			return e
		}
		parts := splitKeys(s)
		if len(parts) < 1 {
			return errors.New("invalid response")
		}

		resp.Status = parts[0]
		switch resp.Status {

		case "VALUE":
			if len(parts) < 4 {
				return errors.New("invalid response")
			}

			key := parts[1]
			// check key length
			flag, e1 := strconv.Atoi(parts[2])
			if e1 != nil {
				return errors.New("invalid response")
			}
			length, e2 := strconv.Atoi(parts[3])
			if e2 != nil {
				return errors.New("invalid response")
			}
			if !config.IsValidValueSize(uint32(length)) {
				return ErrValueTooLarge
			}
			item := &Item{Flag: flag}
			if len(parts) == 5 {
				cas, e := strconv.Atoi(parts[4])
				if e != nil {
					return errors.New("invalid response")
				}
				item.Cas = cas
			}

			if !item.Alloc(length) {
				e = fmt.Errorf("fail to alloc %d", length)
				// TODO: disconnect?
				return e
			}
			if _, e = io.ReadFull(b, item.Body); e != nil {
				item.Free()
				return e
			}
			b.ReadByte() // \r
			b.ReadByte() // \n

			if key[0] != '@' && key[0] != '?' {
				cmem.DBRL.GetData.AddSizeAndCount(item.CArray.Cap)
			}
			resp.Items[key] = item
			continue

		case "STAT":
			if len(parts) != 3 {
				return errors.New("invalid response")
			}
			var item Item
			item.Body = []byte(parts[2])
			resp.Items[parts[1]] = &item
			continue

		case "END":
		case "STORED", "NOT_STORED", "DELETED", "NOT_FOUND":
		case "OK":

		case "ERROR", "SERVER_ERROR", "CLIENT_ERROR":
			if len(parts) > 1 {
				resp.Msg = parts[1]
			}
			logger.Errorf("error: %v", resp)

		default:
			// try to convert to int
			_, err := strconv.Atoi(resp.Status)
			if err == nil {
				// response from incr,decr
				resp.Msg = resp.Status
				resp.Status = "INCR"
			} else {
				logger.Errorf("unknown status: %s %s", s, resp.Status)
				return errors.New("unknown response:" + resp.Status)
			}
		}
		break
	}
	return nil
}

func (resp *Response) Write(w io.Writer) error {
	if resp.Noreply {
		return nil
	}

	switch resp.Status {
	case "VALUE":
		for key, item := range resp.Items {
			if resp.Cas {
				fmt.Fprintf(w, "VALUE %s %d %d %d\r\n", key, item.Flag,
					len(item.Body), item.Cas)
			} else {
				fmt.Fprintf(w, "VALUE %s %d %d\r\n", key, item.Flag,
					len(item.Body))
			}
			e := WriteFull(w, item.Body)
			if e != nil {
				return e
			}
			WriteFull(w, []byte("\r\n"))
		}
		io.WriteString(w, "END\r\n")

	case "STAT":
		io.WriteString(w, resp.Msg)
		io.WriteString(w, "END\r\n")

	case "INCR", "DECR":
		fmt.Fprintf(w, resp.Msg)
		fmt.Fprintf(w, "\r\n")

	default:
		io.WriteString(w, resp.Status)
		if resp.Msg != "" {
			io.WriteString(w, " "+resp.Msg)
		}
		io.WriteString(w, "\r\n")
	}
	return nil
}

func (resp *Response) CleanBuffer() {
	for key, item := range resp.Items {
		if key[0] != '@' && key[0] != '?' {
			cmem.DBRL.GetData.SubSizeAndCount(item.CArray.Cap)
		}
		item.CArray.Free()
	}
	resp.Items = nil
}

func writeLine(w io.Writer, s string) {
	io.WriteString(w, s)
	io.WriteString(w, "\r\n")
}

func (req *Request) Process(store StorageClient, stat *Stats) (resp *Response, err error) {
	resp = new(Response)
	resp.Noreply = req.NoReply

	//var err error
	switch req.Cmd {

	case "get", "gets":
		for _, k := range req.Keys {
			if !config.IsValidKeySize(uint32(len(k))) {
				resp.Status = "CLIENT_ERROR"
				resp.Msg = ErrKeyLength.Error()
				return
			}
		}

		resp.Status = "VALUE"
		resp.Cas = req.Cmd == "gets"
		if len(req.Keys) > 1 {
			resp.Items, err = store.GetMulti(req.Keys)
			if err != nil {
				resp.Status = "SERVER_ERROR"
				resp.Msg = err.Error()
				return
			}
			atomic.AddInt64(&stat.cmd_get, int64(len(req.Keys)))
			atomic.AddInt64(&stat.get_hits, int64(len(resp.Items)))
			atomic.AddInt64(&stat.get_misses, int64(len(req.Keys)-len(resp.Items)))

			bytes := int64(0)
			for _, item := range resp.Items {
				bytes += int64(len(item.Body))
			}
			stat.bytes_written += bytes
		} else {
			atomic.AddInt64(&stat.cmd_get, 1)
			key := req.Keys[0]
			var item *Item
			item, err = store.Get(key)
			if err != nil {
				resp.Status = "SERVER_ERROR"
				resp.Msg = err.Error()
				return
			}
			if item == nil {
				atomic.AddInt64(&stat.get_misses, 1)
			} else {
				resp.Items = make(map[string]*Item, 1)
				resp.Items[key] = item
				atomic.AddInt64(&stat.get_hits, 1)
				stat.bytes_written += int64(len(item.Body))
			}
		}

	case "set", "add", "replace", "cas":
		// We MUST update stat at first, because req.Item will be released
		// in `store.Set`.
		atomic.AddInt64(&stat.cmd_set, 1)
		stat.bytes_read += int64(len(req.Item.Body))

		key := req.Keys[0]
		var suc bool
		suc, err = store.Set(key, req.Item, req.NoReply)
		if err != nil {
			resp.Status = "SERVER_ERROR"
			resp.Msg = err.Error()
			break
		}

		if suc {
			resp.Status = "STORED"
		} else {
			resp.Status = "NOT_STORED"
		}

	case "append":
		atomic.AddInt64(&stat.cmd_set, 1)
		stat.bytes_read += int64(len(req.Item.Body))

		key := req.Keys[0]
		var suc bool
		suc, err = store.Append(key, req.Item.Body)
		if err != nil {
			resp.Status = "SERVER_ERROR"
			resp.Msg = err.Error()
			return
		}

		if suc {
			resp.Status = "STORED"
		} else {
			resp.Status = "NOT_STORED"
		}

	case "incr":
		atomic.AddInt64(&stat.cmd_set, 1)
		stat.bytes_read += int64(len(req.Item.Body))

		resp.Noreply = req.NoReply
		key := req.Keys[0]
		add, err := strconv.Atoi(string(req.Item.Body))
		if err != nil {
			resp.Status = "CLIENT_ERROR"
			resp.Msg = "invalid number"
			break
		}
		var result int
		result, err = store.Incr(key, add)
		if err != nil {
			resp.Status = "SERVER_ERROR"
			resp.Msg = err.Error()
			break
		}

		resp.Status = "INCR"
		resp.Msg = strconv.Itoa(result)

	case "delete":
		key := req.Keys[0]
		var suc bool
		suc, err = store.Delete(key)
		if err != nil {
			resp.Status = "SERVER_ERROR"
			resp.Msg = err.Error()
			break
		}
		if suc {
			resp.Status = "DELETED"
		} else {
			resp.Status = "NOT_FOUND"
		}
		stat.cmd_delete++

	case "stats":
		st := stat.Stats()
		n := int64(store.Len())
		st["curr_items"] = n
		st["total_items"] = n
		resp.Status = "STAT"
		var ss []string
		if len(req.Keys) > 0 {
			ss = make([]string, len(req.Keys))
			for i, k := range req.Keys {
				v, _ := st[k]
				ss[i] = fmt.Sprintf("STAT %s %d\r\n", k, v)
			}
			resp.Msg = strings.Join(ss, "")
		} else {
			ss = make([]string, len(st)+1)
			cnt := 0
			for k, v := range st {
				ss[cnt] = fmt.Sprintf("STAT %s %d\r\n", k, v)
				cnt += 1
			}
			ss[cnt] = fmt.Sprintf("STAT version %s\r\n", config.Version)
		}
		resp.Msg = strings.Join(ss, "")

	case "version":
		resp.Status = "VERSION"
		resp.Msg = config.Version

	case "verbosity", "flush_all":
		resp.Status = "OK"

	case "quit":
		resp = nil

	default:
		resp = nil
		logger.Errorf("Should not reach here, req.Cmd: %s", req.Cmd)
	}

	return
}

func contain(vs []string, v string) bool {
	for _, i := range vs {
		if i == v {
			return true
		}
	}
	return false
}

func (req *Request) Check(resp *Response) error {
	switch req.Cmd {
	case "get", "gets":
		if resp.Items != nil {
			for key, _ := range resp.Items {
				if !contain(req.Keys, key) {
					logger.Errorf("unexpected key in response: %s", key)
					return errors.New("unexpected key in response: " + key)
				}
			}
		}

	case "incr", "decr":
		if !contain([]string{"INCR", "DECR", "NOT_FOUND"}, resp.Status) {
			return errors.New("unexpected status: " + resp.Status)
		}

	case "set", "add", "replace", "append", "prepend":
		if !contain([]string{"STORED", "NOT_STORED", "EXISTS", "NOT_FOUND"},
			resp.Status) {
			return errors.New("unexpected status: " + resp.Status)
		}
	}
	return nil
}
