package store

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"
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

func (bkt *Bucket) buildHintFromData(chunkID int, start uint32, splitID int) (hintpath string, err error) {
	logger.Infof("buildHintFromData chunk %d split %d offset 0x%x", chunkID, splitID, start)
	r, err := bkt.datas.GetStreamReader(chunkID)
	defer r.Close()
	if err != nil {
		return
	}
	hintpath = bkt.hints.getPath(chunkID, splitID, false)
	w, err := newHintFileWriter(hintpath, bkt.datas.filesizes[chunkID], 1<<20)
	if err != nil {
		return
	}
	defer w.close()
	for {
		rec, offset, _, e := r.Next()
		if e != nil {
			err = e
			return
		}
		if rec == nil {
			break
		}
		khash := getKeyHash(rec.Key)
		p := rec.Payload
		vhash := Getvhash(p.Value)
		item := newHintItem(khash, p.Ver, vhash, Position{0, offset}, string(rec.Key))
		err = w.writeItem(item)
		if err != nil {
			return
		}
	}
	return
}

func (bkt *Bucket) updateHtreeFromHint(chunkID int, path string) (maxoffset uint32, err error) {
	logger.Infof("updateHtreeFromHint chunk %d, %s", chunkID, path)
	meta := Meta{}
	tree := bkt.htree
	var pos Position
	pos.ChunkID = chunkID
	r := newHintFileReader(path, chunkID, 1<<20)
	r.open()
	maxoffset = r.maxOffset
	defer r.close()
	for {
		item, e := r.next()
		if e != nil {
			err = e
			return
		}
		if item == nil {
			return
		}
		if item.ver > 0 {
			ki := NewKeyInfoFromBytes([]byte(item.key), item.keyhash, false)
			ki.Prepare()
			meta.ValueHash = item.vhash
			meta.Ver = item.ver
			pos.Offset = item.pos
			tree.set(ki, &meta, pos)
		}
	}
	return
}

func (bkt *Bucket) open(id int, home string) (err error) {
	// load HTree
	bkt.id = id
	bkt.home = home
	bkt.datas = NewdataStore(home)
	bkt.hints = newHintMgr(home)
	bkt.collisons = newCTable()
	bkt.htree = newHTree(config.TreeDepth, id, config.TreeHeight)

	bkt.hints.filesizes = bkt.datas.filesizes[:]

	maxdata, err := bkt.datas.ListFiles()
	if err != nil {
		return err
	}
	filesizes := bkt.datas.filesizes
	htreechunk := -1
	var treePathToLoad string
	for i := 0; i < MAX_CHUNK; i++ {
		htreePath := fmt.Sprintf("%s/%03d.hr", home, i)
		_, err = os.Stat(htreePath)
		if err == nil {
			if i > maxdata {
				logger.Errorf("remove htree beyond %d:%s", maxdata, htreePath)
				os.Remove(htreePath)
			} else {
				htreechunk = i
				treePathToLoad = htreePath
			}
		}
	}
	if htreechunk >= 0 {
		bkt.htree.load(treePathToLoad)
	}

	for i := htreechunk + 1; i < MAX_CHUNK; i++ {
		paths := bkt.hints.findChunk(i, filesizes[i] < 1)
		if len(paths) > 0 {
			splitid := 0
			maxoffset := uint32(0)
			for _, p := range paths {
				offset, e := bkt.updateHtreeFromHint(i, p)
				if e != nil {
					err = e
					return
				}
				if offset > maxoffset {
					maxoffset = offset
				}
				splitid++
			}
			if maxoffset < filesizes[i] {
				p, e := bkt.buildHintFromData(i, maxoffset, splitid)
				if e != nil {
					err = e
					return
				}
				if _, err = bkt.updateHtreeFromHint(i, p); err != nil {
					return
				}
			}
		} else if filesizes[i] > 0 {
			p, e := bkt.buildHintFromData(i, 0, 0)
			if e != nil {
				err = e
				return
			}
			if _, err = bkt.updateHtreeFromHint(i, p); err != nil {
				return
			}
		}
	}
	go func() {
		for i := 0; i < htreechunk+1; i++ {
			paths := bkt.hints.findChunk(i, filesizes[i] > 0)
			if (len(paths) == 0) && (filesizes[i] > 0) {
				if _, err = bkt.buildHintFromData(i, 0, 0); err != nil {
					return
				}
			}
			// TODO: hints may fall behand data?
		}
		go bkt.hints.merger(5 * time.Minute)
	}()

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
func (bkt *Bucket) close() {
	logger.Debugf("closing bucket %s", bkt.home)
	bkt.dumpGCHistroy()
	bkt.datas.flush(-1, true)
	bkt.hints.close()
	bkt.htree.dump(fmt.Sprintf("%s/%03d.hr", bkt.home, bkt.datas.newHead))

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
			vhash := Getvhash(v.Value)
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
	v.CalcValueHash()
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
		logger.Errorf("%s", err.Error())
		return
	} else if rec == nil {
		return
	} else if bytes.Compare(rec.Key, ki.Key) == 0 {
		payload = rec.Payload
		return
	}

	hintit, chunkID, err := bkt.hints.getItem(ki.KeyHash, ki.StringKey)
	if err != nil || hintit == nil {
		return
	}
	pos = Position{chunkID, hintit.pos}
	hintit.pos = pos.encode()

	bkt.collisons.set(hintit)
	hintit2 := newHintItem(ki.KeyHash, rec.Payload.Ver, rec.Payload.ValueHash, pos, string(rec.Key))
	bkt.collisons.set(hintit2)

	rec, err = bkt.getRecordByPos(pos)
	if err != nil {
		logger.Errorf("%s", err.Error())
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

func (b *Bucket) loadGCHistroy() {
	// TODO
}

func (b *Bucket) dumpGCHistroy() {
	// TODO
}
