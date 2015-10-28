package store

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
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

	htreechunk int

	GCHistory []GCState
	lastGC    int
}

func (bkt *Bucket) getHtreePath(chunkID int) string {
	return fmt.Sprintf("%s/%03d.hr", bkt.home, chunkID)
}

func (bkt *Bucket) getCollisionPath() string {
	return fmt.Sprintf("%s/collision.yaml", bkt.home)
}

func (bkt *Bucket) dumpCollisions() {
	bkt.collisons.dump(bkt.getCollisionPath())
}

func (bkt *Bucket) loadCollisions() {
	bkt.collisons.load(bkt.getCollisionPath())
}

func (bkt *Bucket) buildHintFromData(chunkID int, start uint32, splitID int) (hintpath string, err error) {
	logger.Infof("buildHintFromData chunk %d split %d offset 0x%x", chunkID, splitID, start)
	r, err := bkt.datas.GetStreamReader(chunkID)
	if err != nil {
		return
	}
	defer r.Close()
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
		if item.Ver > 0 {
			ki := NewKeyInfoFromBytes([]byte(item.Key), item.Keyhash, false)
			ki.Prepare()
			meta.ValueHash = item.Vhash
			meta.Ver = item.Ver
			pos.Offset = item.Pos
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
	bkt.loadCollisions()
	bkt.htree = newHTree(config.TreeDepth, id, config.TreeHeight)

	bkt.hints.filesizes = bkt.datas.filesizes[:]

	maxdata, err := bkt.datas.ListFiles()
	if err != nil {
		return err
	}
	filesizes := bkt.datas.filesizes
	bkt.htreechunk = -1
	var treePathToLoad string
	for i := 0; i < MAX_NUM_CHUNK; i++ {
		htreePath := bkt.getHtreePath(i)
		_, err = os.Stat(htreePath)
		if err == nil {
			if i > maxdata {
				logger.Errorf("remove htree beyond %d:%s", maxdata, htreePath)
				os.Remove(htreePath)
			} else {
				bkt.htreechunk = i
				treePathToLoad = htreePath
			}
		}
	}
	if bkt.htreechunk >= 0 {
		err := bkt.htree.load(treePathToLoad)
		if err != nil {
			bkt.htreechunk = -1
			bkt.htree = newHTree(config.TreeDepth, id, config.TreeHeight)
		}
	}

	for i := bkt.htreechunk + 1; i < MAX_NUM_CHUNK; i++ {
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
		for i := 0; i < bkt.htreechunk+1; i++ {
			paths := bkt.hints.findChunk(i, filesizes[i] < 1)
			if (len(paths) == 0) && (filesizes[i] > 0) {
				if _, err = bkt.buildHintFromData(i, 0, 0); err != nil {
					return
				}
			}
			// TODO: hints may fall behand data?
		}
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
	bkt.dumpCollisions()
	bkt.datas.flush(-1, true)
	bkt.hints.close()
	bkt.dumpHtree()
}

func (bkt *Bucket) dumpHtree() {
	if bkt.htreechunk > 0 {
		os.Remove(bkt.getHtreePath(bkt.htreechunk))
	}
	bkt.htreechunk = bkt.datas.newHead
	bkt.htree.dump(bkt.getHtreePath(bkt.htreechunk))
}

func (bkt *Bucket) removeHtree() {
	if bkt.htreechunk > 0 {
		os.Remove(bkt.getHtreePath(bkt.htreechunk))
	}
	bkt.htreechunk = -1
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
		pos = decodePos(hintit.Pos)
	}

	var rec *Record
	if memOnly {
		if hintit != nil {
			payload = new(Payload)
			payload.Ver = hintit.Ver
			payload.ValueHash = hintit.Vhash
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

	hintit, chunkID, err := bkt.hints.getItem(ki.KeyHash, ki.StringKey, false)
	if err != nil || hintit == nil {
		return
	}
	pos = Position{chunkID, hintit.Pos}
	hintit.Pos = pos.encode()

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

func (bkt *Bucket) incr(ki *KeyInfo, value int) int {
	payload, _, err := bkt.get(ki, false)
	if err != nil {
		return 0
	}

	if payload != nil {
		s := string(payload.Value)
		if payload.Flag != FLAG_INCR {
			logger.Errorf("incr with flag 0x%x", payload.Flag)
			return 0
		}
		if len(s) > 22 {
			logger.Errorf("incr with value %s", s)
			return 0
		}
		v, err := strconv.Atoi(s)
		if err != nil {
			logger.Errorf("incr with value %s", s)
			return 0
		}
		value += v
	}
	s := strconv.Itoa(value)
	payload.TS = uint32(time.Now().Unix())
	payload.Value = []byte(s)
	bkt.set(ki, payload)
	return value
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

func (bkt *Bucket) GetRecordByKeyHash(ki *KeyInfo) (rec *Record, err error) {
	_, pos, found := bkt.htree.get(ki)
	if !found {
		return
	}
	return bkt.datas.GetRecordByPos(pos)
}

func (b *Bucket) loadGCHistroy() {
	// TODO
}

func (b *Bucket) dumpGCHistroy() {
	// TODO
}
