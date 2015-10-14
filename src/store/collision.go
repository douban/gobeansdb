package store

type cTable struct {
	m map[uint64]map[string]hintItem
}

func newCTable() *cTable {
	t := &cTable{}
	t.m = make(map[uint64]map[string]hintItem)
	return t
}

func (table *cTable) get(keyhash uint64, key string) *hintItem {
	items, ok := table.m[keyhash]
	if ok {
		item, ok := items[key]
		if ok {
			return &item
		}
	}
	return nil
}

func (table *cTable) set(it *hintItem) {
	items, ok := table.m[it.keyhash]
	if ok {
		items[it.key] = *it
	} else {
		items = make(map[string]hintItem)
		items[it.key] = *it
		table.m[it.keyhash] = items
	}
}
