package store

import (
	"io/ioutil"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

type cTable struct {
	m map[uint64]map[string]HintItem
}

func newCTable() *cTable {
	t := &cTable{}
	t.m = make(map[uint64]map[string]HintItem)
	return t
}

func (table *cTable) get(keyhash uint64, key string) *HintItem {
	items, ok := table.m[keyhash]
	if ok {
		item, ok := items[key]
		if ok {
			return &item
		}
	}
	return nil
}

func (table *cTable) set(it *HintItem) {
	items, ok := table.m[it.Keyhash]
	if ok {
		items[it.Key] = *it
	} else {
		items = make(map[string]HintItem)
		items[it.Key] = *it
		table.m[it.Keyhash] = items
	}
}

func (table *cTable) dump(path string) {
	if len(table.m) == 0 {
		return
	}
	content, err := yaml.Marshal(table.m)
	if err != nil {
		logger.Errorf("unmarshal yaml faild %s %s", path, err.Error())
		return
	}
	err = ioutil.WriteFile(path, content, 0644)
	if err != nil {
		logger.Errorf("write yaml failed %s ", path, err.Error())
	}
}

func (table *cTable) load(path string) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		if !strings.Contains(err.Error(), "no such file or directory") {
			logger.Errorf("read yaml failed %s ", path, err.Error())
		}
		return
	}
	if err := yaml.Unmarshal(content, table.m); err != nil {
		logger.Errorf("unmarshal yaml faild %s %s", path, err.Error())
	}
}
