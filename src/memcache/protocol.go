package memcache

import (
	"bufio"
	"cmem"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const VERSION = "0.1.0"

var (
	MaxKeyLength  = 200
	MaxBodyLength = 1024 * 1024 * 50
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
	Body        []byte
	alloc       *byte
}

func (it *Item) String() (s string) {
	return fmt.Sprintf("Item(Flag:%d, Exptime:%d, Length:%d, Cas:%d, Body:%v",
		it.Flag, it.Exptime, len(it.Body), it.Cas, it.Body)
}

type Request struct {
	Cmd     string   // get, set, delete, quit, etc.
	Keys    []string // keys
	Item    *Item
	NoReply bool
}

func (req *Request) String() (s string) {
	return fmt.Sprintf("Request(Cmd:%s, Keys:%v, Item:%v, NoReply: %t)",
		req.Cmd, req.Keys, &req.Item, req.NoReply)
}

func (req *Request) Clear() {
	req.NoReply = false
	if req.Item != nil && req.Item.alloc != nil {
		cmem.Free(req.Item.alloc, cap(req.Item.Body))
		req.Item.Body = nil
		req.Item.alloc = nil
		req.Item = nil
	}
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
		noreplay := ""
		if req.NoReply {
			noreplay = " noreply"
		}
		item := req.Item
		if req.Cmd == "cas" {
			fmt.Fprintf(w, "%s %s %d %d %d %d%s\r\n", req.Cmd, req.Keys[0], item.Flag,
				item.Exptime, item.Cas, len(item.Body), noreplay)
		} else {
			fmt.Fprintf(w, "%s %s %d %d %d%s\r\n", req.Cmd, req.Keys[0], item.Flag,
				item.Exptime, len(item.Body), noreplay)
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
		log.Printf("unkown request cmd:", req.Cmd)
		return errors.New("unknown cmd: " + req.Cmd)
	}

	return e
}

func (req *Request) Read(b *bufio.Reader) (e error) {
	var s string
	if s, e = b.ReadString('\n'); e != nil {
		return e
	}

	if !strings.HasSuffix(s, "\r\n") {
		return errors.New("not completed command")
	}

	parts := splitKeys(s)
	if len(parts) < 1 {
		return errors.New("invalid cmd")
	}

	req.Cmd = parts[0]
	switch req.Cmd {

	case "get", "gets":
		if len(parts) < 2 {
			return errors.New("invalid cmd")
		}
		req.Keys = parts[1:]

	case "set", "add", "replace", "cas", "append", "prepend":
		if len(parts) < 5 || len(parts) > 7 {
			return errors.New("invalid cmd")
		}
		req.Keys = parts[1:2]
		req.Item = &Item{}
		item := req.Item
		item.ReceiveTime = time.Now()
		item.Flag, e = strconv.Atoi(parts[2])
		if e != nil {
			return e
		}
		item.Exptime, e = strconv.Atoi(parts[3])
		if e != nil {
			return e
		}
		length, e := strconv.Atoi(parts[4])
		if e != nil {
			return e
		}
		if length > MaxBodyLength {
			return errors.New("body too large")
		}
		if req.Cmd == "cas" {
			if len(parts) < 6 {
				return errors.New("invalid cmd")
			}
			item.Cas, e = strconv.Atoi(parts[5])
			if len(parts) > 6 && parts[6] != "noreply" {
				return errors.New("invalid cmd")
			}
			req.NoReply = len(parts) > 6 && parts[6] == "noreply"
		} else {
			if len(parts) > 5 && parts[5] != "noreply" {
				return errors.New("invalid cmd")
			}
			req.NoReply = len(parts) > 5 && parts[5] == "noreply"
		}

		// FIXME
		if length > cmem.GConfig.AllocLimit {
			item.alloc = cmem.Alloc(length)
			item.Body = (*[1 << 30]byte)(unsafe.Pointer(item.alloc))[:length]
			(*reflect.SliceHeader)(unsafe.Pointer(&item.Body)).Cap = length
			runtime.SetFinalizer(item, func(item *Item) {
				if item.alloc != nil {
					//log.Print("free by finalizer: ", cap(item.Body))
					cmem.Free(item.alloc, cap(item.Body))
					item.Body = nil
					item.alloc = nil
				}
			})
		} else {
			item.Body = make([]byte, length)
		}
		cmem.Add(cmem.TagSetData, length)
		if _, e = io.ReadFull(b, item.Body); e != nil {
			return e
		}
		b.ReadByte() // \r
		b.ReadByte() // \n

	case "delete":
		if len(parts) < 2 || len(parts) > 4 {
			return errors.New("invalid cmd")
		}
		req.Keys = parts[1:2]
		req.NoReply = len(parts) > 2 && parts[len(parts)-1] == "noreply"

	case "incr", "decr":
		if len(parts) < 3 || len(parts) > 4 {
			return errors.New("invalid cmd")
		}
		req.Keys = parts[1:2]
		req.Item = &Item{Body: []byte(parts[2])}
		req.NoReply = len(parts) > 3 && parts[3] == "noreply"

	case "stats":
		req.Keys = parts[1:]

	case "quit", "version", "flush_all":
	case "verbosity":
		if len(parts) >= 2 {
			req.Keys = parts[1:]
		}

	default:
		req.Keys = parts[1:]
		log.Print("unknown command", req.Cmd)
		return errors.New("unknown command: " + req.Cmd)
	}

	return
}

type Response struct {
	status  string
	msg     string
	cas     bool
	noreply bool
	items   map[string]*Item
}

func (resp *Response) String() (s string) {
	return fmt.Sprintf("Response(Status:%s, msg:%s, Items:%v)",
		resp.status, resp.msg, resp.items)
}

func (resp *Response) Read(b *bufio.Reader) error {
	resp.items = make(map[string]*Item, 1)
	for {
		s, e := b.ReadString('\n')
		if e != nil {
			log.Print("read response line failed", e)
			return e
		}
		parts := splitKeys(s)
		if len(parts) < 1 {
			return errors.New("invalid response")
		}

		resp.status = parts[0]
		switch resp.status {

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
			if length > MaxBodyLength {
				return errors.New("body too large")
			}

			item := &Item{Flag: flag}
			if len(parts) == 5 {
				cas, e := strconv.Atoi(parts[4])
				if e != nil {
					return errors.New("invalid response")
				}
				item.Cas = cas
			}

			// FIXME
			if length > cmem.GConfig.AllocLimit {
				item.alloc = cmem.Alloc(length)
				item.Body = (*[1 << 30]byte)(unsafe.Pointer(item.alloc))[:length]
				(*reflect.SliceHeader)(unsafe.Pointer(&item.Body)).Cap = length
				runtime.SetFinalizer(item, func(item *Item) {
					if item.alloc != nil {
						//log.Print("free by finalizer: ", cap(item.Body))
						cmem.Free(item.alloc, cap(item.Body))
						item.Body = nil
						item.alloc = nil
					}
				})
			} else {
				item.Body = make([]byte, length)
			}
			if _, e = io.ReadFull(b, item.Body); e != nil {
				return e
			}
			b.ReadByte() // \r
			b.ReadByte() // \n
			resp.items[key] = item
			continue

		case "STAT":
			if len(parts) != 3 {
				return errors.New("invalid response")
			}
			var item Item
			item.Body = []byte(parts[2])
			resp.items[parts[1]] = &item
			continue

		case "END":
		case "STORED", "NOT_STORED", "DELETED", "NOT_FOUND":
		case "OK":

		case "ERROR", "SERVER_ERROR", "CLIENT_ERROR":
			if len(parts) > 1 {
				resp.msg = parts[1]
			}
			log.Print("error:", resp)

		default:
			// try to convert to int
			_, err := strconv.Atoi(resp.status)
			if err == nil {
				// response from incr,decr
				resp.msg = resp.status
				resp.status = "INCR"
			} else {
				log.Print("unknown status:", s, resp.status)
				return errors.New("unknown response:" + resp.status)
			}
		}
		break
	}
	return nil
}

func (resp *Response) Write(w io.Writer) error {
	if resp.noreply {
		return nil
	}

	switch resp.status {
	case "VALUE":
		for key, item := range resp.items {
			if resp.cas {
				fmt.Fprintf(w, "VALUE %s %d %d %d\r\n", key, item.Flag,
					len(item.Body), item.Cas)
			} else {
				fmt.Fprintf(w, "VALUE %s %d %d\r\n", key, item.Flag,
					len(item.Body))
			}
			if e := WriteFull(w, item.Body); e != nil {
				return e
			}
			cmem.Sub(cmem.TagGetData, len(item.Body))
			WriteFull(w, []byte("\r\n"))
		}
		io.WriteString(w, "END\r\n")

	case "STAT":
		io.WriteString(w, resp.msg)
		io.WriteString(w, "END\r\n")

	case "INCR", "DECR":
		fmt.Fprintf(w, resp.msg)
		fmt.Fprintf(w, "\r\n")

	default:
		io.WriteString(w, resp.status)
		if resp.msg != "" {
			io.WriteString(w, " "+resp.msg)
		}
		io.WriteString(w, "\r\n")
	}
	return nil
}

func (resp *Response) CleanBuffer() {
	for _, item := range resp.items {
		if item.alloc != nil {
			cmem.Free(item.alloc, cap(item.Body))
			item.alloc = nil
		}
		runtime.SetFinalizer(item, nil)
	}
	resp.items = nil
}

func writeLine(w io.Writer, s string) {
	io.WriteString(w, s)
	io.WriteString(w, "\r\n")
}

func (req *Request) Process(store StorageClient, stat *Stats) (resp *Response, err error) {
	resp = new(Response)
	resp.noreply = req.NoReply

	//var err error
	switch req.Cmd {

	case "get", "gets":
		for _, k := range req.Keys {
			if len(k) > MaxKeyLength {
				resp.status = "CLIENT_ERROR"
				resp.msg = "key too long"
				return
			}
		}

		resp.status = "VALUE"
		resp.cas = req.Cmd == "gets"
		if len(req.Keys) > 1 {
			resp.items, err = store.GetMulti(req.Keys)
			if err != nil {
				resp.status = "SERVER_ERROR"
				resp.msg = err.Error()
				return
			}
			stat.cmd_get += int64(len(req.Keys))
			stat.get_hits += int64(len(resp.items))
			stat.get_misses += int64(len(req.Keys) - len(resp.items))
			bytes := int64(0)
			for _, item := range resp.items {
				bytes += int64(len(item.Body))
			}
			stat.bytes_written += bytes
		} else {
			stat.cmd_get++
			key := req.Keys[0]
			var item *Item
			item, err = store.Get(key)
			if err != nil {
				resp.status = "SERVER_ERROR"
				resp.msg = err.Error()
				return
			}
			if item == nil {
				stat.get_misses++
			} else {
				resp.items = make(map[string]*Item, 1)
				resp.items[key] = item
				stat.get_hits++
				stat.bytes_written += int64(len(item.Body))
			}
		}

	case "set", "add", "replace", "cas":
		key := req.Keys[0]
		var suc bool
		suc, err = store.Set(key, req.Item, req.NoReply)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			break
		}

		stat.cmd_set++
		stat.bytes_read += int64(len(req.Item.Body))
		if suc {
			resp.status = "STORED"
		} else {
			resp.status = "NOT_STORED"
		}

	case "append":
		key := req.Keys[0]
		var suc bool
		suc, err = store.Append(key, req.Item.Body)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			return
		}

		stat.cmd_set++
		stat.bytes_read += int64(len(req.Item.Body))
		if suc {
			resp.status = "STORED"
		} else {
			resp.status = "NOT_STORED"
		}

	case "incr":
		stat.cmd_set++
		stat.bytes_read += int64(len(req.Item.Body))
		resp.noreply = req.NoReply
		key := req.Keys[0]
		add, err := strconv.Atoi(string(req.Item.Body))
		if err != nil {
			resp.status = "CLIENT_ERROR"
			resp.msg = "invalid number"
			break
		}
		var result int
		result, err = store.Incr(key, add)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			break
		}

		if result > 0 {
			resp.status = "INCR"
			resp.msg = strconv.Itoa(result)
		} else {
			resp.status = "NOT_FOUND"
		}

	case "delete":
		key := req.Keys[0]
		var suc bool
		suc, err = store.Delete(key)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			break
		}
		if suc {
			resp.status = "DELETED"
		} else {
			resp.status = "NOT_FOUND"
		}
		stat.cmd_delete++

	case "stats":
		st := stat.Stats()
		n := int64(store.Len())
		st["curr_items"] = n
		st["total_items"] = n
		resp.status = "STAT"
		var ss []string
		if len(req.Keys) > 0 {
			ss = make([]string, len(req.Keys))
			for i, k := range req.Keys {
				v, _ := st[k]
				ss[i] = fmt.Sprintf("STAT %s %d\r\n", k, v)
			}
			resp.msg = strings.Join(ss, "")
		} else {
			ss = make([]string, len(st))
			cnt := 0
			for k, v := range st {
				ss[cnt] = fmt.Sprintf("STAT %s %d\r\n", k, v)
				cnt += 1
			}
		}
		resp.msg = strings.Join(ss, "")

	case "version":
		resp.status = "VERSION"
		resp.msg = VERSION

	case "verbosity", "flush_all":
		resp.status = "OK"

	case "quit":
		resp = nil
		return

	default:
		// client error
		resp = nil
		return
		resp.status = "CLIENT_ERROR"
		resp.msg = "invalid cmd"
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
		if resp.items != nil {
			for key, _ := range resp.items {
				if !contain(req.Keys, key) {
					log.Print("unexpected key in response: ", key)
					return errors.New("unexpected key in response: " + key)
				}
			}
		}

	case "incr", "decr":
		if !contain([]string{"INCR", "DECR", "NOT_FOUND"}, resp.status) {
			return errors.New("unexpected status: " + resp.status)
		}

	case "set", "add", "replace", "append", "prepend":
		if !contain([]string{"STORED", "NOT_STORED", "EXISTS", "NOT_FOUND"},
			resp.status) {
			return errors.New("unexpected status: " + resp.status)
		}
	}
	return nil
}
