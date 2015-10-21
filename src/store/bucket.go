package store

import (
	"bytes"
	"sync"
)

type Bucket struct {
	writeLock sync.Mutex // todo replace with hashlock later (crc)

	// pre open init
	state  int
	homeID int

	// init in open
	id        int
	home      string
	collisons *cTable
	htree     *HTree
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
	bkt.htree = newHTree(config.TreeDepth, id, config.TreeHeight)

	bkt.loadGCHistroy()
	return nil
}

func abs(n int32) int32 {
	if n < 0 {
		return -n
	}
	return n
}

// called by hstore, data already flushed
func (b *Bucket) close() {
	b.dumpGCHistroy()
	// TODO: dump indexes
}

func (bkt *Bucket) checkVer(oldv, ver int32) (int32, bool) {
	// TODO: accounts
	if ver == 0 {
		if oldv > 0 {
			ver = oldv + 1
		} else {
			ver = -oldv + 1
		}
	} else if ver < 0 {
		ver = -abs(oldv) - 1
	} else {
		if abs(ver) <= abs(oldv) {
			return 1, false
		}
	}
	return ver, true
}

func (bkt *Bucket) getset(ki *KeyInfo, v *Payload) error {
	bkt.writeLock.Lock()
	defer bkt.writeLock.Unlock()
	payload, _, err := bkt.get(ki, true)
	if err != nil {
		return err
	}
	ver := v.Ver
	if payload != nil {
		var valid bool
		ver, valid = bkt.checkVer(payload.Ver, v.Ver)
		if !valid {
			return nil
		}
		if payload.Ver > 1 {
			vhash := getvhash(v.Value)
			if vhash == payload.ValueHash {
				return nil
			}
		}
	}
	v.Ver = ver
	bkt.set(ki, v)
	return nil
}

func (bkt *Bucket) set(ki *KeyInfo, v *Payload) error {
	pos, err := bkt.datas.AppendRecord(&Record{ki.Key, v})
	if err != nil {
		return err
	}
	bkt.htree.set(ki, &v.Meta, pos)
	bkt.hints.set(ki, &v.Meta, pos)
	return nil
}

func (bkt *Bucket) get(ki *KeyInfo, memOnly bool) (payload *Payload, pos Position, err error) {
	hintit := bkt.collisons.get(ki.KeyHash, ki.StringKey)
	var meta *Meta
	var found bool
	if hintit == nil {

		meta, pos, found = bkt.htree.get(ki)
		if !found {
			return
		}
		_ = meta
	} else {
		pos = decodePos(hintit.pos)
	}
	var rec *Record
	if memOnly {
		if hintit != nil {
			payload = new(Payload)
			payload.Ver = hintit.ver
			payload.ValueHash = hintit.vhash
		} else if found {
			payload = new(Payload)
			payload.Meta = *meta
		}
		return // omit collision
	}

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

func (bkt *Bucket) listDir(ki *KeyInfo) ([]byte, error) {
	return bkt.htree.ListDir(ki)
}

func (bkt *Bucket) getInfo(keys []string) ([]byte, error) {
	return nil, nil

}
