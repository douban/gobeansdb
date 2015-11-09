package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	HTREE_SUFFIX       = "hash"
	HINT_SUFFIX        = "s"
	MERGED_HINT_SUFFIX = "m"
)

type Bucket struct {
	// TODO: replace with hashlock later (crc)
	writeLock sync.Mutex

	// pre open init
	state  int
	homeID int

	// init in open
	id   int
	home string

	htree   *HTree
	hints   *hintMgr
	datas   *dataStore
	htreeID HintID

	GCHistory []GCState
	lastGC    int
}

func (bkt *Bucket) getHtreePath(chunkID, SplitID int) string {
	return getIndexPath(bkt.home, chunkID, SplitID, "hash")
}

func (bkt *Bucket) getCollisionPath() string {
	return fmt.Sprintf("%s/collision.yaml", bkt.home)
}

func (bkt *Bucket) dumpCollisions() {
	bkt.hints.collisions.dump(bkt.getCollisionPath())
}

func (bkt *Bucket) loadCollisions() {
	bkt.hints.collisions.load(bkt.getCollisionPath())
}

func (bkt *Bucket) buildHintFromData(chunkID int, start uint32) (err error) {
	logger.Infof("buildHintFromData chunk %d offset 0x%x", chunkID, start)
	r, err := bkt.datas.GetStreamReader(chunkID)
	if err != nil {
		return
	}
	r.seek(start)
	defer r.Close()
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
		p.Decompress()
		vhash := Getvhash(p.Value)
		item := newHintItem(khash, p.Ver, vhash, Position{0, offset}, string(rec.Key))
		bkt.hints.setItem(item, chunkID, rec.Payload.RecSize)
	}
	bkt.hints.trydump(chunkID, true)
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
	maxoffset = r.datasize
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
		ki := NewKeyInfoFromBytes([]byte(item.Key), item.Keyhash, false)
		ki.Prepare()
		meta.ValueHash = item.Vhash
		meta.Ver = item.Ver
		pos.Offset = item.Pos
		if item.Ver > 0 {
			tree.set(ki, &meta, pos)
		} else {
			pos.ChunkID = -1
			tree.remove(ki, pos)
		}

	}
	return
}

func (bkt *Bucket) checkHintWithData(chunkID int) (err error) {
	if bkt.datas.filesizes[chunkID] == 0 {
		bkt.hints.RemoveHintfilesByChunk(chunkID)
		return
	}
	hintDataSize := bkt.hints.loadHintsByChunk(chunkID)
	if hintDataSize < bkt.datas.filesizes[chunkID] {
		err = bkt.buildHintFromData(chunkID, hintDataSize)
	}
	return
}

func (bkt *Bucket) open(bucketID int, home string) (err error) {
	st := time.Now()
	// load HTree
	bkt.id = bucketID
	bkt.home = home
	bkt.datas = NewdataStore(bucketID, home)
	bkt.hints = newHintMgr(bucketID, home)
	bkt.loadCollisions()
	bkt.htree = newHTree(config.TreeDepth, bucketID, config.TreeHeight)
	bkt.htreeID = HintID{0, -1}

	maxdata, err := bkt.datas.ListFiles()
	if err != nil {
		return err
	}
	htrees, ids := bkt.getAllIndex(HTREE_SUFFIX)
	for i := len(htrees) - 1; i >= 0; i-- {
		treepath := htrees[i]
		id := ids[i]
		if id.Chunk > maxdata {
			logger.Errorf("remove htree beyond data %d:%s", maxdata, treepath)
			os.Remove(treepath)
		} else {
			if bkt.htreeID.isLarger(id.Chunk, id.Split) {
				err := bkt.htree.load(treepath)
				if err != nil {
					bkt.htreeID = HintID{0, -1}
					bkt.htree = newHTree(config.TreeDepth, bucketID, config.TreeHeight)
					continue
				}
				bkt.htreeID = id
			} else {
				logger.Errorf("remove old htree %d:%s", maxdata, treepath)
				os.Remove(treepath)
			}
		}
	}

	for i := bkt.htreeID.Chunk; i < MAX_NUM_CHUNK; i++ {
		startsp := 0
		if i == bkt.htreeID.Chunk {
			startsp = bkt.htreeID.Split + 1
		}
		e := bkt.checkHintWithData(i)
		if e != nil {
			err = e
			logger.Fatalf("fail to start for bad data")
		}
		splits := bkt.hints.chunks[i].splits
		numhintfile := len(splits) - 1
		if startsp >= numhintfile { // rebuilt
			continue
		}
		for j, sp := range splits[:numhintfile] {
			bkt.updateHtreeFromHint(i, sp.file.path)
			if e != nil {
				err = e
				return
			}
			bkt.hints.maxDumpedHintID = HintID{i, startsp + j}
		}
	}
	go func() {
		for i := 0; i < bkt.htreeID.Chunk; i++ {
			bkt.checkHintWithData(i)
		}
	}()

	bkt.loadGCHistroy()
	logger.Infof("bucket %x opened, max rss = %d, use time %s", bucketID, GetMaxRSS(), time.Since(st))
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
	logger.Infof("closing bucket %s", bkt.home)
	bkt.datas.flush(-1, true)
	datas, _ := filepath.Glob(fmt.Sprintf("%s/*.data", bkt.home))
	if len(datas) == 0 {
		return
	}

	bkt.dumpCollisions()
	bkt.hints.close()
	bkt.dumpHtree()
	bkt.dumpGCHistroy()
}

func (bkt *Bucket) dumpHtree() {
	hintID := bkt.hints.maxDumpedHintID
	if bkt.htreeID.isLarger(hintID.Chunk, hintID.Split) {
		bkt.removeHtree()
		bkt.htreeID = hintID
		bkt.htree.dump(bkt.getHtreePath(bkt.htreeID.Chunk, bkt.htreeID.Split))
	}
}

func (bkt *Bucket) getAllIndex(suffix string) (paths []string, ids []HintID) {
	pattern := getIndexPath(bkt.home, -1, -1, suffix)
	paths0, _ := filepath.Glob(pattern)
	sort.Sort(sort.StringSlice(paths0))
	for _, p := range paths0 {
		id, ok := parseIDFromPath(p)
		if !ok {
			logger.Errorf("find index file with wrong name %s", p)
		} else {
			paths = append(paths, p)
			ids = append(ids, id)
		}
	}
	return
}

func (bkt *Bucket) removeHtree() {
	paths, _ := bkt.getAllIndex(HTREE_SUFFIX)
	for _, p := range paths {
		logger.Infof("rm htree: %s", p)
		os.Remove(p)
	}
	bkt.htreeID = HintID{0, 0}
}

func (bkt *Bucket) checkAndUpdateVerison(oldv, ver int32) (int32, bool) {
	if ver == 0 {
		if oldv >= 0 {
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

func (bkt *Bucket) checkAndSet(ki *KeyInfo, v *Payload) error {
	bkt.writeLock.Lock()
	defer bkt.writeLock.Unlock()
	oldv := int32(0)

	payload, _, err := bkt.get(ki, true)
	if err != nil {
		return err
	}
	if payload != nil {
		oldv = payload.Ver
		if oldv > 0 {
			vhash := Getvhash(v.Value)
			if vhash == payload.ValueHash {
				return nil
			}
		}
	}

	var valid bool
	v.Ver, valid = bkt.checkAndUpdateVerison(oldv, v.Ver)
	if !valid {
		return nil
	}
	if v.Ver < 0 && (payload == nil || oldv < 0) {
		return fmt.Errorf("NOT_FOUND")
	}
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
	bkt.hints.set(ki, &v.Meta, pos, v.RecSize)
	return nil
}

func (bkt *Bucket) get(ki *KeyInfo, memOnly bool) (payload *Payload, pos Position, err error) {
	hintit, _ := bkt.hints.collisions.get(ki.KeyHash, ki.StringKey)
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
		meta = &Meta{
			Ver:       hintit.Ver,
			ValueHash: hintit.Vhash,
			RecSize:   0,
			TS:        0,
			Flag:      0,
		}
	}

	var rec *Record
	if memOnly {
		payload = new(Payload)
		payload.Meta = *meta
		return // omit collision
	}
	if meta.Ver < 0 {
		return
	}

	rec, err = bkt.getRecordByPos(pos)
	if err != nil {
		logger.Errorf("%s", err.Error())
		return
	} else if rec == nil {
		err = fmt.Errorf("bad htree item, get nothing,  pos %v", pos)
		logger.Errorf("%s", err.Error())
		return
	} else if bytes.Compare(rec.Key, ki.Key) == 0 {
		payload = rec.Payload
		return
	}

	keyhash := getKeyHash(rec.Key)
	if keyhash != ki.KeyHash {
		bkt.htree.remove(ki, pos)
		err = fmt.Errorf("bad htree item %016x != %016x, pos %v", keyhash, ki.KeyHash, pos)
		logger.Errorf("%s", err.Error())
		return
	}

	hintit, chunkID, err := bkt.hints.getItem(ki.KeyHash, ki.StringKey, false)
	if err != nil || hintit == nil {
		return
	}

	vhash := uint16(0)
	if rec.Payload.Ver > 0 {
		vhash = Getvhash(rec.Payload.Value)
	}
	hintit2 := newHintItem(ki.KeyHash, rec.Payload.Ver, vhash, pos, string(rec.Key))
	bkt.hints.collisions.set(hintit2) // the one in htree

	pos = Position{chunkID, hintit.Pos}
	hintit.Pos = pos.encode()
	bkt.hints.collisions.set(hintit) // the one not in htree

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
