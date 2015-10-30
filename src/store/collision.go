package store

import (
	"io/ioutil"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

type CollisionTable struct {
	HintID
	Items map[uint64]map[string]HintItem
}

func newCollisionTable() *CollisionTable {
	t := &CollisionTable{}
	t.Items = make(map[uint64]map[string]HintItem)
	return t
}

func (table *CollisionTable) get(keyhash uint64, key string) *HintItem {
	items, ok := table.Items[keyhash]
	if ok {
		item, ok := items[key]
		if ok {
			return &item
		}
	}
	return nil
}

func (table *CollisionTable) set(it *HintItem) {
	items, ok := table.Items[it.Keyhash]
	if ok {
		items[it.Key] = *it
	} else {
		items = make(map[string]HintItem)
		items[it.Key] = *it
		table.Items[it.Keyhash] = items
	}
}

func (table *CollisionTable) dump(path string) {
	content, err := yaml.Marshal(table)
	if err != nil {
		logger.Errorf("unmarshal yaml faild %s %s", path, err.Error())
		return
	}
	err = ioutil.WriteFile(path, content, 0644)
	if err != nil {
		logger.Errorf("write yaml failed %s ", path, err.Error())
	}
}

func (table *CollisionTable) load(path string) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		if !strings.Contains(err.Error(), "no such file or directory") {
			logger.Errorf("read yaml failed %s ", path, err.Error())
		}
		return
	}
	if err := yaml.Unmarshal(content, table); err != nil {
		logger.Errorf("unmarshal yaml faild %s %s", path, err.Error())
	}
}
