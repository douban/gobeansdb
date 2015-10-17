package memcache

import (
	"math/rand"
	"strconv"
	"sync"
)

type Storage interface {
	Get(key string) (*Item, error)
	GetMulti(keys []string) (map[string]*Item, error)
	Set(key string, item *Item, noreply bool) (bool, error)
	Append(key string, value []byte) (bool, error)
	Incr(key string, value int) (int, error)
	Delete(key string) (bool, error)
	Len() int
}

type DistributeStorage interface {
	Get(key string) (*Item, []string, error)
	GetMulti(keys []string) (map[string]*Item, []string, error)
	Set(key string, item *Item, noreply bool) (bool, []string, error)
	Append(key string, value []byte) (bool, []string, error)
	Incr(key string, value int) (int, []string, error)
	Delete(key string) (bool, []string, error)
	Len() int
}

type mapStore struct {
	lock sync.Mutex
	data map[string]*Item
}

func NewMapStore() *mapStore {
	s := new(mapStore)
	s.data = make(map[string]*Item)
	return s
}

func (s *mapStore) Get(key string) (*Item, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	r, _ := s.data[key]
	return r, nil
}

func (s *mapStore) GetMulti(keys []string) (map[string]*Item, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	rs := make(map[string]*Item, len(keys))
	for _, key := range keys {
		r, _ := s.data[key]
		if r != nil {
			rs[key] = r
		}
	}
	return rs, nil
}

func (s *mapStore) Set(key string, item *Item, noreply bool) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	item.Cas = rand.Int()
	it := *item
	item.alloc = nil
	s.data[key] = &it
	return true, nil
}

func (s *mapStore) Append(key string, value []byte) (suc bool, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	r, ok := s.data[key]
	if ok && r.Flag == 0 {
		r.Body = append(r.Body, value...)
		s.data[key] = r
		return true, nil
	}
	return false, nil
}

func (s *mapStore) Incr(key string, v int) (n int, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	r, ok := s.data[key]
	if ok {
		n, err = strconv.Atoi(string(r.Body))
		if err != nil {
			return
		}
		n += v
		r.Body = []byte(strconv.Itoa(n))
	} else {
		n = 0
	}
	return
}

func (s *mapStore) Delete(key string) (r bool, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
		r = true
	}
	return
}

func (s *mapStore) Len() int {
	return len(s.data)
}
