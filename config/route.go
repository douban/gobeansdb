package config

import (
	"fmt"
	"io/ioutil"
	"strconv"

	yaml "gopkg.in/yaml.v2"
)

type Server struct {
	Addr       string
	Buckets    []int    `yaml:"-"`
	BucketsHex []string `yaml:"buckets,flow"`
}

func (s *Server) Decode() error {
	s.Buckets = make([]int, len(s.BucketsHex))
	for i, str := range s.BucketsHex {
		i64, err := strconv.ParseInt(str, 16, 16)
		if err != nil {
			return err
		}
		s.Buckets[i] = int(i64)
	}
	return nil
}

type RouteTable struct {
	NumBucket int
	Main      []Server
	Backup    []string

	Buckets map[int]map[string]bool `yaml:"-"`
	Servers map[string]map[int]bool `yaml:"-"`
}

func (rt *RouteTable) GetDBRouteConfig(addr string) (r DBRouteConfig, err error) {
	r = DBRouteConfig{NumBucket: rt.NumBucket}
	r.BucketsStat = make([]int, rt.NumBucket)
	buckets, found := rt.Servers[addr]
	if !found {
		err = fmt.Errorf("can not find self in route table")
		return
	}
	r.BucketsHex = make([]string, 0)
	for b, _ := range buckets {
		r.BucketsStat[b] = 1
		r.BucketsHex = append(r.BucketsHex, BucketIDHex(b, rt.NumBucket))
	}
	return r, nil
}

func (rt *RouteTable) LoadFromYaml(data []byte) error {
	if err := yaml.Unmarshal(data, &rt); err != nil {
		return err
	}
	rt.Servers = make(map[string]map[int]bool)
	rt.Buckets = make(map[int]map[string]bool)

	for i := 0; i < rt.NumBucket; i++ {
		rt.Buckets[i] = make(map[string]bool)
	}
	for _, server := range rt.Main {
		server.Decode()
		addr := server.Addr
		rt.Servers[addr] = make(map[int]bool)
		for _, bucket := range server.Buckets {
			rt.Servers[addr][bucket] = true
			rt.Buckets[bucket][addr] = true
		}
	}
	for _, addr := range rt.Backup {
		rt.Servers[addr] = make(map[int]bool)
		for bucket := 0; bucket < rt.NumBucket; bucket++ {
			rt.Servers[addr][bucket] = false
			rt.Buckets[bucket][addr] = false
		}
	}

	return nil
}

func LoadRouteTable(path, zkaddr string) (*RouteTable, error) {
	rt := &RouteTable{}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = rt.LoadFromYaml(data)
	return rt, err
}
