package route

import (
	"io/ioutil"
	"log"

	yaml "gopkg.in/yaml.v2"
)

// for backend
type RouteConfig struct {
	NumBucket int
	Buckets   []int
}

type RouteTable struct {
	NumBucket int                     `yaml:num",omitempty"`
	Buckets   map[int][]string        `yaml:",omitempty"`
	Nodes     map[string]map[int]bool `yaml:"-"`
}

func (rt *RouteTable) GetServerConfig(addr string) RouteConfig {
	r := RouteConfig{}
	r.Buckets = make([]int, r.NumBucket)
	buckets := rt.Nodes[addr]
	for b, _ := range buckets {
		r.Buckets[b] = 1
	}
	return r
}

func (rt *RouteTable) LoadFromYaml(data []byte) error {
	if err := yaml.Unmarshal(data, &rt); err != nil {
		log.Printf("unmarshal yaml format config failed")
		return err
	}
	rt.Nodes = make(map[string]map[int]bool)
	for i, nodes := range rt.Buckets {
		for _, node := range nodes {
			if _, found := rt.Nodes[node]; !found {
				rt.Nodes[node] = make(map[int]bool)
			}
			rt.Nodes[node][i] = true
		}
	}
	return nil
}

func LoadRouteTable(path, zkaddr string) (*RouteTable, error) {
	rt := &RouteTable{}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("read config failed", path, err.Error())
		return nil, err
	}
	err = rt.LoadFromYaml(data)
	return rt, err
}
