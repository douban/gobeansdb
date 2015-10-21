package main

import (
	"errors"
	"log"
	mc "memcache"
	"runtime"
	"store"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"
)

var (
	S               *Storage
	ErrorNotSupport = errors.New("operation not support")
)

func murmur(data []byte) (h uint32) {
	hasher := murmur3.New32()
	hasher.Write(data)
	return hasher.Sum32()
}

func fnv1a(data []byte) (h uint32) {
	PRIME := uint32(0x01000193)
	h = 0x811c9dc5
	for _, b := range data {
		h ^= uint32(int8(b))
		h = (h * PRIME)
	}
	return h
}

func getKeyHash(key []byte) uint64 {
	return (uint64(fnv1a(key)) << 32) | uint64(murmur(key))
}

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
	keybytes := []byte(key)
	keyhash := getKeyHash(keybytes)

	s.payload.Flag = uint32(item.Flag)
	s.payload.Value = item.Body
	s.payload.Ver = int32(item.Exptime)
	s.payload.TS = uint32(time.Now().Second())

	err := s.hstore.Set([]byte(key), keyhash, s.payload)
	if err != nil {
		log.Printf("err to get %s: %s", key, err.Error())
		return false, err
	}
	return true, nil
}

func (s *StorageClient) Get(key string) (*mc.Item, error) {
	defer handlePanic("get")
	keybytes := []byte(key)
	keyhash := getKeyHash(keybytes)

	payload, _, err := s.hstore.Get(keybytes, keyhash, false)
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
	return 0, ErrorNotSupport
}

func (s *StorageClient) Delete(key string) (bool, error) {
	return false, ErrorNotSupport
}

func (s *StorageClient) Close() {

}
