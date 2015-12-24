package store

import (
	"io/ioutil"
	"strings"
	"sync"

	yaml "gopkg.in/yaml.v2"
)

type CollisionTable struct {
	sync.Mutex `yaml:"-"`
	HintID
	Items map[uint64]map[string]HintItem
}

func newCollisionTable() *CollisionTable {
	t := &CollisionTable{}
	t.Items = make(map[uint64]map[string]HintItem)
	return t
}

func (table *CollisionTable) get(keyhash uint64, key string) (item *HintItem, ok bool) {
	table.Lock()
	defer table.Unlock()
	items, ok := table.Items[keyhash]
	if ok {
		if it, ok2 := items[key]; ok2 {
			item = &it
		}
	}
	return
}

// gc should not use this func
func (table *CollisionTable) compareAndSet(it *HintItem, reason string) {
	logger.Infof("%s, set collision %#v", reason, it)
	table.Lock()
	defer table.Unlock()
	items, ok := table.Items[it.Keyhash]
	if ok {
		old, ok := items[it.Key]
		if !ok || reason == "gc" || it.Pos.toInt64() >= old.Pos.toInt64() {
			items[it.Key] = *it
		}

	} else {
		items = make(map[string]HintItem)
		items[it.Key] = *it
		table.Items[it.Keyhash] = items
	}
}

func (table *CollisionTable) dumps() (content []byte) {
	table.Lock()
	content, _ = yaml.Marshal(table)
	table.Unlock()
	return
}

func (table *CollisionTable) dump(path string) {
	table.Lock()
	content, err := yaml.Marshal(table)
	table.Unlock()
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
	table.Lock()
	if err := yaml.Unmarshal(content, table); err != nil {
		logger.Errorf("unmarshal yaml faild %s %s", path, err.Error())
	}
	table.Unlock()
	logger.Infof("load collisoin %s: %s", path, table.dumps())
}
