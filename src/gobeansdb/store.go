package main

import (
	"cmem"
	"errors"
	"fmt"
	mc "memcache"
	"store"
	"sync"
)

var (
	ErrorNotSupport = errors.New("operation not support")
)

type Storage struct {
	numClient int
	hstore    *store.HStore
	sync.Mutex
}

func (s *Storage) Client() mc.StorageClient {
	return &StorageClient{
		s.hstore,
	}
}

type StorageClient struct {
	hstore *store.HStore
}

func (s *StorageClient) Set(key string, item *mc.Item, noreply bool) (bool, error) {
	tofree := &item.CArray
	defer func() {
		if tofree != nil {
			tofree.Free()
		}
	}()
	if !store.IsValidKeyString(key) {
		return false, nil
	}
	ki := s.prepare(key, false)
	payload := &store.Payload{}
	payload.Flag = uint32(item.Flag)
	payload.CArray = item.CArray
	payload.Ver = int32(item.Exptime)
	payload.TS = uint32(item.ReceiveTime.Unix())

	tofree = nil
	err := s.hstore.Set(ki, payload)
	if err != nil {
		logger.Errorf("err to get %s: %s", key, err.Error())
		return false, err
	}
	return true, nil
}

func (s *StorageClient) prepare(key string, isPath bool) *store.KeyInfo {
	ki := &store.KeyInfo{}
	ki.StringKey = key
	ki.Key = []byte(key)
	ki.KeyIsPath = isPath
	return ki
}

func (s *StorageClient) listDir(path string) (*mc.Item, error) {
	// TODO: check valid
	ki := s.prepare(path, true)
	body, err := s.hstore.ListDir(ki)
	if err != nil {
		return nil, err
	}
	item := new(mc.Item)
	item.Body = []byte(body)
	item.Flag = 0
	return item, nil
}

func (s *StorageClient) getMeta(key string, extended bool) (*mc.Item, error) {
	ki := s.prepare(key, false)
	payload, pos, err := s.hstore.Get(ki, false)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}

	// TODO: use the one in htree
	vhash := uint16(0)
	if payload.Ver > 0 {
		vhash = store.Getvhash(payload.Body)
	}
	cmem.DBRL.GetData.SubSize(payload.AccountingSize)
	payload.Free()

	var body string
	if extended {
		body = fmt.Sprintf("%d %d %d %d %d %d %d",
			payload.Ver, vhash, payload.Flag, len(payload.Body), payload.TS, pos.ChunkID, pos.Offset)

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
	if key[0] == '@' {
		if len(key) > 1 && key[1] == '@' {
			key2 := key[2:]
			if len(key2) != 16 {
				return nil, fmt.Errorf("bad command line format") //FIXME: SERVER_ERROR
			}
			ki := s.prepare(key2, true)
			rec, _, err := s.hstore.GetRecordByKeyHash(ki)
			if err != nil {
				return nil, err
			} else if rec == nil {
				return nil, nil
			}
			item := new(mc.Item) // TODO: avoid alloc?
			item.Body = rec.Dumps()
			rec.Payload.Free()
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
			if !store.IsValidKeyString(key) {
				return nil, nil
			}
		} else {
			return nil, fmt.Errorf("bad key %s", key)
		}
		return s.getMeta(key, extended)
	}

	ki := s.prepare(key, false)
	payload, _, err := s.hstore.Get(ki, false)
	if err != nil {
		logger.Errorf("err to get %s: %s", key, err.Error())
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	if payload.Ver < 0 {
		cmem.DBRL.GetData.SubSize(payload.AccountingSize)
		payload.Free()
		return nil, nil
	}
	item := new(mc.Item) // TODO: avoid alloc?
	item.CArray = payload.CArray
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
	return s.hstore.NumKey()
}

func (s *StorageClient) Append(key string, value []byte) (bool, error) {
	return false, ErrorNotSupport
}

func (s *StorageClient) Incr(key string, value int) (int, error) {
	if !store.IsValidKeyString(key) {
		return 0, nil
	}
	ki := s.prepare(key, false)
	newvalue := s.hstore.Incr(ki, value)
	return newvalue, nil
}

func (s *StorageClient) Delete(key string) (bool, error) {
	if !store.IsValidKeyString(key) {
		return false, nil
	}
	ki := s.prepare(key, false)
	payload := store.GetPayloadForDelete()

	err := s.hstore.Set(ki, payload)
	if err != nil {
		if err.Error() == "NOT_FOUND" {
			return false, nil
		} else {
			logger.Errorf("err to delete %s: %s", key, err.Error())
			return false, err
		}
	}
	return true, nil
}

func (s *StorageClient) Close() {

}

func (s *StorageClient) Process(cmd string, args []string) (status string, msg string) {
	status = "CLIENT_ERROR"
	msg = "bad command line format"

	switch cmd {
	case "optimize_stat":
		msg = ""
		bucketid, gcstat := s.hstore.GCStat()
		if gcstat == nil {
			status = "none"
		} else {
			if gcstat.Running {
				status = "running"
				msg = fmt.Sprintf("bitcast 0x%x", bucketid)
			} else if gcstat.Err == nil {
				status = "success"
			} else {
				status = fmt.Sprintf("fail %s", gcstat.Err.Error())
			}
		}

	default:
		status = "ERROR"
		msg = ""
	}
	return
}
