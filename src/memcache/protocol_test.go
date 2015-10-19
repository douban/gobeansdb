package memcache

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"
)

type reqTest struct {
	cmd    string
	anwser string
}

var reqTests = []reqTest{
	reqTest{
		"get  abc cdf \r\n",
		"END\r\n",
	},
	reqTest{
		"set abc 2 3 2 noreply\r\nok\r\n",
		"",
	},
	reqTest{
		"set abc a 3 2 noreply\r\nok\r\n",
		"CLIENT_ERROR strconv.ParseInt: parsing \"a\": invalid syntax\r\n",
	},
	reqTest{
		"set cdf 0 0 2\r\nok\r\n",
		"STORED\r\n",
	},
	reqTest{
		"get cdf\r\n",
		"VALUE cdf 0 2\r\nok\r\nEND\r\n",
	},
	reqTest{
		"get  abc  \r\n",
		"VALUE abc 2 2\r\nok\r\nEND\r\n",
	},
	reqTest{
		"stats curr_items cmd_get cmd_set get_hits get_misses\r\n",
		"STAT curr_items 2\r\n" +
			"STAT cmd_get 4\r\n" +
			"STAT cmd_set 2\r\n" +
			"STAT get_hits 2\r\n" +
			"STAT get_misses 2\r\n" +
			"END\r\n",
	},
	reqTest{
		"set abc 3 3 3 2 noreply\r\nok\r\n",
		"CLIENT_ERROR invalid cmd\r\n",
	},
	reqTest{
		"set abc a 3 2 noreply\r\nok\r\n",
		"CLIENT_ERROR strconv.ParseInt: parsing \"a\": invalid syntax\r\n",
	},
	reqTest{
		"set abc 3 3 10\r\nok\r\n",
		"CLIENT_ERROR unexpected EOF\r\n",
	},
	reqTest{
		"get   \r\n",
		"CLIENT_ERROR invalid cmd\r\n",
	},
	reqTest{
		"get  " + strings.Repeat("a", 300) + " \r\n",
		"CLIENT_ERROR key too long\r\n",
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
		"delete abc\r\n",
		"DELETED\r\n",
	},
	reqTest{
		"delete abc noreply\r\n",
		"",
	},
	reqTest{
		"append cdf 0 0 2\r\n 2\r\n",
		"STORED\r\n",
	},
	// reqTest{
	//     "prepend cdf 0 0 2\r\n1 \r\n",
	//     "STORED",
	// },
	reqTest{
		"get cdf\r\n",
		"VALUE cdf 0 4\r\nok 2\r\nEND\r\n",
	},
	reqTest{
		"append ap 0 0 2\r\nap\r\n",
		"NOT_STORED\r\n",
	},

	reqTest{
		"set n 4 0 1\r\n5\r\n",
		"STORED\r\n",
	},
	reqTest{
		"incr n 3\r\n",
		"8\r\n",
	},
	reqTest{
		"incr nn 7\r\n",
		"NOT_FOUND\r\n",
	},

	reqTest{
		"flush_all\r\n",
		"OK\r\n",
	},
	reqTest{
		"verbosity 1\r\n",
		"OK\r\n",
	},
	reqTest{
		"version\r\n",
		"VERSION " + VERSION + "\r\n",
	},

	reqTest{
		"quit\r\n",
		"",
	},
	reqTest{
		"error\r\n",
		"CLIENT_ERROR unknown command: error\r\n",
	},
}

func TestRequest(t *testing.T) {
	store := NewMapStore()
	stats := NewStats()

	for i, test := range reqTests {
		buf := bytes.NewBufferString(test.cmd)
		req := new(Request)
		e := req.Read(bufio.NewReader(buf))
		var resp *Response
		if e != nil {
			resp = &Response{status: "CLIENT_ERROR", msg: e.Error()}
		} else {
			resp, _ = req.Process(store, stats)
		}

		r := make([]byte, 0)
		wr := bytes.NewBuffer(r)
		if resp != nil {
			resp.Write(wr)
		}
		ans := wr.String()
		if test.anwser != ans {
			fmt.Print(req, resp)
			t.Errorf("test %d: expect %s[%d], bug got %s[%d]\n", i,
				test.anwser, len(test.anwser), ans, len(ans))
		}
	}
}
