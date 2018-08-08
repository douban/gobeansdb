package memcache

import (
	"os"
	"runtime"
	"time"

	"github.com/douban/gobeansdb/utils"
)

type Stats struct {
	start                               time.Time
	curr_item, total_items              int64
	cmd_get, cmd_set, cmd_delete        int64
	get_hits, get_misses                int64
	threads                             int64
	curr_connections, total_connections int64
	bytes_read, bytes_written           int64
	slow_cmd                            int64 // slow_cmd is not a stats in memecached protocol
}

func NewStats() *Stats {
	s := new(Stats)
	s.start = time.Now()
	return s
}

func mem_in_go(include_zero bool) runtime.MemProfileRecord {
	var p []runtime.MemProfileRecord
	n, ok := runtime.MemProfile(nil, include_zero)
	for {
		// Allocate room for a slightly bigger profile,
		// in case a few more entries have been added
		// since the call to MemProfile.
		p = make([]runtime.MemProfileRecord, n+50)
		n, ok = runtime.MemProfile(p, include_zero)
		if ok {
			p = p[0:n]
			break
		}
		// Profile grew; try again.
	}

	var total runtime.MemProfileRecord
	for i := range p {
		r := &p[i]
		total.AllocBytes += r.AllocBytes
		total.AllocObjects += r.AllocObjects
		total.FreeBytes += r.FreeBytes
		total.FreeObjects += r.FreeObjects
	}
	return total
}

func (s *Stats) Stats() map[string]int64 {
	st := make(map[string]int64)
	st["cmd_get"] = s.cmd_get
	st["cmd_set"] = s.cmd_set
	st["cmd_delete"] = s.cmd_delete
	st["get_hits"] = s.get_hits
	st["get_misses"] = s.get_misses
	st["curr_connections"] = s.curr_connections
	st["total_connections"] = s.total_connections
	st["bytes_read"] = s.bytes_read
	st["bytes_written"] = s.bytes_written
	st["slow_cmd"] = s.slow_cmd

	t := time.Now()
	st["time"] = int64(t.Unix())
	st["uptime"] = int64(t.Sub(s.start).Seconds())
	st["pid"] = int64(os.Getpid())
	st["threads"] = int64(runtime.NumGoroutine())
	rusage := utils.Getrusage()
	st["rusage_user"] = int64(rusage.Utime.Sec)
	st["rusage_system"] = int64(rusage.Stime.Sec)
	st["rusage_maxrss"] = rusage.Maxrss
	st["avail_space"] = 0
	st["total_space"] = 0
	st["curr_items"] = 0
	return st
}
