package memcache

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/douban/gobeansdb/config"
	"strings"
	"testing"
)

type reqTest struct {
	cmd     string
	answer  string
	maxSize int64
}

var reqTests = []reqTest{
	reqTest{
		cmd:    "get  abc cdf \r\n",
		answer: "END\r\n",
	},
	reqTest{
		cmd:    "set abc 2 3 2 noreply\r\nok\r\n",
		answer: "",
	},
	reqTest{
		cmd:    "set abc a 3 2 noreply\r\nok\r\n",
		answer: "CLIENT_ERROR invalid cmd\r\n",
	},
	reqTest{
		cmd:    "set cdf 0 0 2\r\nok\r\n",
		answer: "STORED\r\n",
	},
	reqTest{
		cmd:    "get cdf\r\n",
		answer: "VALUE cdf 0 2\r\nok\r\nEND\r\n",
	},
	reqTest{
		cmd:    "get  abc  \r\n",
		answer: "VALUE abc 2 2\r\nok\r\nEND\r\n",
	},
	reqTest{
		cmd: "stats curr_items cmd_get cmd_set get_hits get_misses\r\n",
		answer: "STAT curr_items 2\r\n" +
			"STAT cmd_get 4\r\n" +
			"STAT cmd_set 2\r\n" +
			"STAT get_hits 2\r\n" +
			"STAT get_misses 2\r\n" +
			"END\r\n",
	},
	reqTest{
		cmd:    "set abc 3 3 3 2 noreply\r\nok\r\n",
		answer: "CLIENT_ERROR invalid cmd\r\n",
	},
	reqTest{
		cmd:    "set abc a 3 2 noreply\r\nok\r\n",
		answer: "CLIENT_ERROR invalid cmd\r\n",
	},
	reqTest{
		cmd:    "set abc 3 3 10\r\nok\r\n",
		answer: "CLIENT_ERROR network error\r\n",
	},
	reqTest{
		cmd:    "get   \r\n",
		answer: "CLIENT_ERROR invalid cmd\r\n",
	},
	reqTest{
		cmd:    "get  " + strings.Repeat("a", 300) + " \r\n",
		answer: "CLIENT_ERROR key length error\r\n",
	},
	reqTest{
		cmd:     "set hello 0 0 92160\r\n" + strings.Repeat("a", 1024*90) + "\r\n",
		answer:  "CLIENT_ERROR value too large\r\n",
		maxSize: 1000,
	},
	reqTest{
		cmd:     "set hello 0 0 1000\r\n" + strings.Repeat("a", 1000) + "\r\n",
		answer:  "STORED\r\n",
		maxSize: 1000,
	},
	reqTest{
		cmd:     "set hello 0 0 1000\r\n" + strings.Repeat("a", 1000) + "\r\n",
		answer:  "CLIENT_ERROR value too large\r\n",
		maxSize: 999,
	},
	reqTest{
		cmd:     "set hello 0 0 1000\r\n" + strings.Repeat("a", 1000) + "\r\n",
		answer:  "STORED\r\n",
		maxSize: 1001,
	},
	/* no need to keep origin order
	reqTest{
		"get  abc  cdf\r\n",
		"VALUE abc 2 2\r\nok\r\nVALUE cdf 0 2\r\nok\r\nEND\r\n",
	},
	*/
	// reqTest{
	//     "cas abc -5 10 0 134020434\r\n\r\n",
	//     "STORED\r\n",
	// },
	reqTest{
		cmd:    "delete abc\r\n",
		answer: "DELETED\r\n",
	},
	reqTest{
		cmd:    "delete abc noreply\r\n",
		answer: "",
	},
	reqTest{
		cmd:    "append cdf 0 0 2\r\n 2\r\n",
		answer: "STORED\r\n",
	},
	// reqTest{
	//     "prepend cdf 0 0 2\r\n1 \r\n",
	//     "STORED",
	// },
	reqTest{
		cmd:    "get cdf\r\n",
		answer: "VALUE cdf 0 4\r\nok 2\r\nEND\r\n",
	},
	reqTest{
		cmd:    "append ap 0 0 2\r\nap\r\n",
		answer: "NOT_STORED\r\n",
	},

	reqTest{
		cmd:    "set n 4 0 1\r\n5\r\n",
		answer: "STORED\r\n",
	},
	reqTest{
		cmd:    "incr n 3\r\n",
		answer: "8\r\n",
	},
	reqTest{
		cmd:    "incr nn 7\r\n",
		answer: "7\r\n",
	},

	reqTest{
		cmd:    "flush_all\r\n",
		answer: "OK\r\n",
	},
	reqTest{
		cmd:    "verbosity 1\r\n",
		answer: "OK\r\n",
	},
	reqTest{
		cmd:    "version\r\n",
		answer: "VERSION " + config.Version + "\r\n",
	},

	reqTest{
		cmd:    "quit\r\n",
		answer: "",
	},
	reqTest{
		cmd:    "error\r\n",
		answer: "CLIENT_ERROR non memcache command\r\n",
	},
}

func TestRequest(t *testing.T) {
	InitTokens()
	store := NewMapStore()
	stats := NewStats()

	for i, test := range reqTests {
		if test.maxSize > 0 {
			config.MCConf.BodyMax = test.maxSize
		}
		buf := bytes.NewBufferString(test.cmd)
		req := new(Request)
		e := req.Read(bufio.NewReader(buf))
		var resp *Response
		if e != nil {
			resp = &Response{Status: "CLIENT_ERROR", Msg: e.Error()}
		} else {
			resp, _ = req.Process(store, stats)
		}

		r := make([]byte, 0)
		wr := bytes.NewBuffer(r)
		if resp != nil {
			resp.Write(wr)
		}
		ans := wr.String()
		if test.answer != ans {
			fmt.Print(req, resp)
			t.Errorf("test %d(%#v): expect %#v[%d], bug got %#v[%d]\n", i, test.cmd,
				test.answer, len(test.answer), ans, len(ans))
		}
		req.Clear()
	}
}
