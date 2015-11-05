package main

import (
	"cmem"
	"errors"
	"fmt"
	"log"
	mc "memcache"
	"runtime"
	"store"
	"strconv"
	"sync"
	"time"
)

var (
	ErrorNotSupport = errors.New("operation not support")
)

func getStack(bytes int) string {
	b := make([]byte, bytes)
	all := false
	n := runtime.Stack(b, all)
	return string(b[:n])
}

func handlePanic(s string) {
	if e := recover(); e != nil {
		switch t := e.(type) {
		case error:
			log.Printf("%s panic with err(%s), stack: %s", s, t.Error(), getStack(1000))
		default:
			log.Printf("%s panic with non-err(%#v), stack: %s", s, t, getStack(1000))
		}
	}
}

type Storage struct {
	numClient int
	hstore    *store.HStore
	sync.Mutex
}

func (s *Storage) Client() mc.StorageClient {
	return &StorageClient{
		s.hstore,
		store.KeyInfo{},
		&store.Payload{}}
}

type StorageClient struct {
	hstore  *store.HStore
	ki      store.KeyInfo
	payload *store.Payload
}

func (s *StorageClient) Set(key string, item *mc.Item, noreply bool) (bool, error) {
	defer handlePanic("set")
	if key[0] == '?' || key[0] == '@' {
		return false, fmt.Errorf("invalid key %s", key)
	}
	s.prepare(key, false)
	s.payload.Flag = uint32(item.Flag)
	s.payload.Value = item.Body
	s.payload.Ver = int32(item.Exptime)
	s.payload.TS = uint32(item.ReceiveTime.Unix())

	err := s.hstore.Set(&s.ki, s.payload)
	if err != nil {
		log.Printf("err to get %s: %s", key, err.Error())
		return false, err
	}
	return true, nil
}

func (s *StorageClient) prepare(key string, isPath bool) {
	s.ki.StringKey = key
	s.ki.Key = []byte(key)
	s.ki.KeyIsPath = isPath
}

func (s *StorageClient) listDir(path string) (*mc.Item, error) {
	// TODO: check valid
	s.prepare(path, true)
	body, err := s.hstore.ListDir(&s.ki)
	if err != nil {
		return nil, err
	}
	item := new(mc.Item)
	item.Body = []byte(body)
	item.Flag = 0
	return item, nil
}

func (s *StorageClient) getMeta(key string, extended bool) (*mc.Item, error) {
	s.prepare(key, false)
	payload, pos, err := s.hstore.Get(&s.ki, false)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}

	// TODO: use the one in htree
	vhash := uint16(0)
	if payload.Ver > 0 {
		vhash = store.Getvhash(payload.Value)
		cmem.Sub(cmem.TagGetData, len(payload.Value))
	}

	var body string
	if extended {
		body = fmt.Sprintf("%d %d %d %d %d %d %d",
			payload.Ver, vhash, payload.Flag, len(payload.Value), payload.TS, pos.ChunkID, pos.Offset)

	} else {
		body = fmt.Sprintf("%d %d %d %d",
			payload.Ver, vhash, payload.Flag, payload.TS)
	}
	item := new(mc.Item)
	item.Body = []byte(body)
	item.Flag = 0
	return item, nil
}

func (s *StorageClient) Get(key string) (*mc.Item, error) {

	defer handlePanic("get")
	if key[0] == '@' {

		if len(key) > 1 && key[1] == '@' {
			key2 := key[2:]
			if len(key2) != 16 {
				return nil, fmt.Errorf("bad command line format") //FIXME: SERVER_ERROR
			}
			s.prepare(key2, true)
			rec, err := s.hstore.GetRecordByKeyHash(&s.ki)
			if err != nil {
				return nil, err
			} else if rec == nil {
				return nil, nil
			}
			item := new(mc.Item) // TODO: avoid alloc?
			item.Body = rec.Dumps()
			item.Flag = 0
			return item, nil
		} else if len(key) > 11 && "collision_" == key[1:11] {
			if len(key) > 15 && "all_" == key[11:15] {
				return nil, nil
			} else {
				item := new(mc.Item) // TODO: avoid alloc?
				item.Body = []byte("0 0 0 0")
				item.Flag = 0
				return item, nil
			}
		} else {
			return s.listDir(key[1:])
		}

	} else if key[0] == '?' {
		extended := false
		if len(key) > 1 {
			if key[1] == '?' {
				extended = true
				key = key[2:]
			} else {
				key = key[1:]
			}

		} else {
			return nil, fmt.Errorf("bad key %s", key)
		}
		return s.getMeta(key, extended)
	}
	s.prepare(key, false)
	payload, _, err := s.hstore.Get(&s.ki, false)
	if err != nil {
		log.Printf("err to get %s: %s", key, err.Error())
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	item := new(mc.Item) // TODO: avoid alloc?
	item.Body = payload.Value
	item.Flag = int(payload.Flag)
	return item, nil
}

func (s *StorageClient) GetMulti(keys []string) (map[string]*mc.Item, error) {
	ret := make(map[string]*mc.Item)
	for _, key := range keys {
		item, _ := s.Get(key)
		if item != nil {
			ret[key] = item
		}
	}
	return ret, nil
}

func (s *StorageClient) Len() int {
	// TODO:
	return 0
}

func (s *StorageClient) Append(key string, value []byte) (bool, error) {
	return false, ErrorNotSupport
}

func (s *StorageClient) Incr(key string, value int) (int, error) {
	defer handlePanic("delete")
	if key[0] == '?' || key[0] == '@' {
		return 0, fmt.Errorf("invalid key %s", key)
	}
	s.prepare(key, false)
	newvalue := s.hstore.Incr(&s.ki, value)
	return newvalue, nil
}

func (s *StorageClient) Delete(key string) (bool, error) {
	defer handlePanic("delete")
	if !store.IsValidKeyString(key) {
		return false, fmt.Errorf("invalid key %s", key)
	}
	s.prepare(key, false)
	s.payload.Flag = 0
	s.payload.Value = nil
	s.payload.Ver = -1
	s.payload.TS = uint32(time.Now().Unix()) // TODO:

	err := s.hstore.Set(&s.ki, s.payload)
	if err != nil {
		if err.Error() == "NOT_FOUND" {
			return false, nil
		} else {
			log.Printf("err to delete %s: %s", key, err.Error())
			return false, err
		}
	}
	return true, nil
}

func (s *StorageClient) Close() {

}

func (s *StorageClient) Process(cmd string, args []string) (status string, msg string, ok bool) {

	status = "CLIENT_ERROR"
	msg = "bad command line format"
	switch cmd {
	case "gc":
		l := len(args)
		if !(l == 2 || l == 3) || args[0][0] != '@' {
			return
		}
		bucket, err := strconv.ParseUint(args[0][1:], 16, 32)
		if err != nil || bucket < 0 {
			return
		}

		start, err := strconv.Atoi(args[1])
		if err != nil {
			return
		}
		end := -1
		if l == 3 {
			end, err = strconv.Atoi(args[2])
			if err != nil {
				return
			}
		}
		err = s.hstore.GC(int(bucket), start, end)
		if err != nil {
			status = "ERROR"
			msg = err.Error()
		}
		status = "OK"
		msg = ""
		ok = true
	case "optimize_stat":
		msg = ""
		ok = true
		bucketid, gcstat := s.hstore.GCStat()
		if gcstat == nil {
			status = "none"
		} else {
			if gcstat.Running {
				status = "running"
				msg = fmt.Sprintf("bitcast 0x%x", bucketid)
			} else if gcstat.StopReason != nil {
				status = "success"
			} else {
				status = "fail"
			}
		}
	}
	return
}
