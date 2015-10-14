package store

import "bytes"

type Bucket struct {
	// pre open init
	state  int
	homeID int

	// init in open
	id        int
	home      string
	collisons *cTable
	HTree     *HTree
	hints     *hintMgr
	datas     *dataStore

	GCHistory []GCState
	lastGC    int
}

func (bkt *Bucket) open(id int, home string) error {
	// load HTree
	bkt.id = id
	bkt.home = home
	bkt.datas = NewdataStore(home)
	bkt.hints = newHintMgr(home)
	bkt.collisons = newCTable()
	bkt.HTree = newHTree(config.TreeDepth, id, config.TreeHeight)

	bkt.loadGCHistroy()
	return nil
}

// called by hstore, data already flushed
func (b *Bucket) close() {
	b.dumpGCHistroy()
	// TODO: dump indexes
}

func (bkt *Bucket) set(ki *KeyInfo, v *Payload) error {
	pos, err := bkt.datas.AppendRecord(&Record{ki.Key, v})
	if err != nil {
		return err
	}
	bkt.HTree.set(ki, &v.Meta, pos)
	bkt.hints.set(ki, &v.Meta, pos)
	return nil
}

func (bkt *Bucket) get(ki *KeyInfo, memOnly bool) (payload *Payload, pos Position, err error) {
	hintit := bkt.collisons.get(ki.KeyHash, ki.StringKey)
	if hintit == nil {
		var meta *Meta
		var found bool
		meta, pos, found = bkt.HTree.get(ki)
		if !found {
			return
		}
		_ = meta
		// TODO: memonly
	} else {
		pos = decodePos(hintit.pos)
	}

	var rec *Record
	rec, err = bkt.getRecordByPos(pos)
	if err != nil {
		return
	} else if rec == nil {
		return
	} else if bytes.Compare(rec.Key, ki.Key) != 0 {
		return
	}

	hintit, chunkID, err := bkt.hints.getItem(ki.KeyHash, ki.StringKey)
	if err != nil || hintit == nil {
		return
	}
	pos = Position{chunkID, hintit.pos}
	hintit.pos = pos.encode()

	bkt.collisons.set(hintit)
	hintit2 := newHintItem(ki.KeyHash, rec.Payload.Ver, rec.getvhash(), pos, string(rec.Key))
	bkt.collisons.set(hintit2)

	rec, err = bkt.getRecordByPos(pos)
	if err != nil {
		return
	} else if rec != nil {
		payload = rec.Payload
	}
	return
}

func (bkt *Bucket) getRecordByPos(pos Position) (*Record, error) {
	return bkt.datas.GetRecordByPos(pos)
}

func (bkt *Bucket) listDir(path []int) ([]byte, error) {
	return nil, nil
}

func (bkt *Bucket) getInfo(keys []string) ([]byte, error) {
	return nil, nil

}
