package config

import (
	"io/ioutil"
	"strconv"

	yaml "gopkg.in/yaml.v2"
)

//DefaultRouteConfig = route.RouteConfig{NumBucket: 256, Buckets: make([]int, 256)}
var DefaultRouteConfig = DBRouteConfig{NumBucket: 16, BucketsStat: make([]int, 16)}

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

type DBRouteConfig struct {
	NumBucket   int
	BucketsStat []int `json:"Buckets"` // TODO: `json:"-"`
	BucketsHex  []string
}

func (rt *RouteTable) GetDBRouteConfig(addr string) (r DBRouteConfig, err error) {
	r = DBRouteConfig{NumBucket: rt.NumBucket}
	r.BucketsStat = make([]int, rt.NumBucket)
	buckets, found := rt.Servers[addr]
	if !found {
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

func LoadRouteTableLocal(path string) (*RouteTable, error) {
	LocalRoutePath = path
	rt := &RouteTable{}
	data, err := ioutil.ReadFile(LocalRoutePath)
	if err != nil {
		return nil, err
	}
	err = rt.LoadFromYaml(data)
	if err != nil {
		return nil, err
	}
	return rt, nil
}

func LoadRouteTableZK(path, cluster string, zkservers []string) (*RouteTable, error) {
	LocalRoutePath = path

	rt := &RouteTable{}
	client, err := NewZK(cluster, zkservers)
	if err != nil {
		return nil, err
	}
	ZKClient = client
	data, stat, err := client.GetRouteRaw()

	err = rt.LoadFromYaml(data)
	if err != nil {
		return nil, err
	}
	ZKClient.Stat = stat
	UpdateLocalRoute(data)
	return rt, nil
}
