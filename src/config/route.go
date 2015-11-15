package config

import (
	"fmt"
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

type RouteTable struct {
	NumBucket int                     `yaml:num",omitempty"`
	Buckets   map[int][]string        `yaml:",omitempty"`
	Nodes     map[string]map[int]bool `yaml:"-"`
}

func (rt *RouteTable) GetDBRouteConfig(addr string) (r DBRouteConfig, err error) {
	r = DBRouteConfig{NumBucket: rt.NumBucket}
	r.Buckets = make([]int, rt.NumBucket)
	buckets, found := rt.Nodes[addr]
	if !found {
		err = fmt.Errorf("can not find self in route table")
		return
	}
	for b, _ := range buckets {
		r.Buckets[b] = 1
	}
	return r, nil
}

func (rt *RouteTable) LoadFromYaml(data []byte) error {
	if err := yaml.Unmarshal(data, &rt); err != nil {
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
		return nil, err
	}
	err = rt.LoadFromYaml(data)
	return rt, err
}
