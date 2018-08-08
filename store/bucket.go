package store

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/douban/gobeansdb/cmem"
	"github.com/douban/gobeansdb/config"
	"github.com/douban/gobeansdb/loghub"
	"github.com/douban/gobeansdb/utils"
)

const (
	HTREE_SUFFIX       = "hash"
	HINT_SUFFIX        = "s"
	MERGED_HINT_SUFFIX = "m"
)

const (
	BUCKET_STAT_EMPTY = iota
	BUCKET_STAT_NOT_EMPTY
	BUCKET_STAT_READY
)

var analysisLogger = loghub.AnalysisLogger

type BucketStat struct {
	// pre open init
	State int

	// init in open
	ID   int
	Home string

	TreeID      HintID
	NextGCChunk int
}

type BucketInfo struct {
	BucketStat

	// tmp
	Pos             Position
	LastGC          *GCState
	HintState       int
	MaxDumpedHintID HintID
	DU              int64
	NumSameVhash    int64
	SizeSameVhash   int64
	SizeVhashKey    string
	NumSet          int64
	NumGet          int64
}

type Bucket struct {
	// TODO: replace with hashlock later (crc)
	writeLock sync.Mutex
	BucketInfo

	htree     *HTree
	hints     *hintMgr
	datas     *dataStore
	GCHistory []GCState
}

func (bkt *Bucket) release() {
	bkt.hints = nil
	bkt.datas = nil
	htree := bkt.htree
	bkt.htree = nil
	htree.release()
	bkt.BucketInfo = BucketInfo{}
}

func (bkt *Bucket) getHtreePath(chunkID, SplitID int) string {
	return getIndexPath(bkt.Home, chunkID, SplitID, "hash")
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
		vhash := Getvhash(p.Body)
		p.Free()
		item := newHintItem(khash, p.Ver, vhash, Position{0, offset}, string(rec.Key))
		bkt.hints.setItem(item, chunkID, rec.Payload.RecSize)
	}
	bkt.hints.trydump(chunkID, true)
	debug.FreeOSMemory()
	return
}

func (bkt *Bucket) updateHtreeFromHint(chunkID int, path string) (maxoffset uint32, err error) {
	logger.Infof("updateHtreeFromHint chunk %d, %s", chunkID, path)
	meta := Meta{}
	tree := bkt.htree
	var pos Position
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
		pos.Offset = item.Pos.Offset
		if item.Ver > 0 {
			pos.ChunkID = chunkID
			tree.set(ki, &meta, pos)
		} else {
			pos.ChunkID = -1
			tree.remove(ki, pos)
		}
	}
	return
}

func (bkt *Bucket) checkHintWithData(chunkID int) (err error) {
	size := bkt.datas.chunks[chunkID].size
	if size == 0 {
		bkt.hints.RemoveHintfilesByChunk(chunkID)
		return
	}
	hintDataSize := bkt.hints.loadHintsByChunk(chunkID)
	if hintDataSize < size {
		err = bkt.buildHintFromData(chunkID, hintDataSize)
	}
	return
}

func (bkt *Bucket) open(bucketID int, home string) (err error) {
	st := time.Now()
	// load HTree
	bkt.ID = bucketID
	bkt.Home = home
	bkt.datas = NewdataStore(bucketID, home)
	bkt.hints = newHintMgr(bucketID, home)
	bkt.hints.loadCollisions()
	htree := newHTree(Conf.TreeDepth, bucketID, Conf.TreeHeight)

	bkt.TreeID = HintID{0, -1}

	maxdata, err := bkt.datas.ListFiles()
	if err != nil {
		return err
	}
	htrees, ids := bkt.getAllIndex(HTREE_SUFFIX)
	for i := len(htrees) - 1; i >= 0; i-- {
		treepath := htrees[i]
		id := ids[i]
		if id.Chunk > maxdata {
			logger.Errorf("htree beyond data: htree=%s, maxdata=%d", treepath, maxdata)
			utils.Remove(treepath)
		} else {
			if bkt.TreeID.isLarger(id.Chunk, id.Split) {
				err := htree.load(treepath)
				if err != nil {
					bkt.TreeID = HintID{0, -1}
					htree = newHTree(Conf.TreeDepth, bucketID, Conf.TreeHeight)
				} else {
					bkt.TreeID = id
				}
			} else {
				logger.Errorf("found old htree: htree=%s, currenct_htree_id=%v", treepath, bkt.TreeID)
				utils.Remove(treepath)
			}
		}
	}
	bkt.htree = htree

	bkt.hints.maxDumpedHintID = bkt.TreeID
	for i := bkt.TreeID.Chunk; i < MAX_NUM_CHUNK; i++ {
		startsp := 0
		if i == bkt.TreeID.Chunk {
			startsp = bkt.TreeID.Split + 1
		}
		e := bkt.checkHintWithData(i)
		if e != nil {
			err = e
			logger.Fatalf("fail to start for bad data: %s", e.Error())
		}
		splits := bkt.hints.chunks[i].splits
		numhintfile := len(splits) - 1
		if startsp >= numhintfile { // rebuilt
			continue
		}
		for j, sp := range splits[:numhintfile] {
			_, e := bkt.updateHtreeFromHint(i, sp.file.path)
			if e != nil {
				err = e
				return
			}
			bkt.hints.maxDumpedHintID = HintID{i, startsp + j}
		}
	}
	go func() {
		for i := 0; i < bkt.TreeID.Chunk; i++ {
			bkt.checkHintWithData(i)
		}
	}()

	if bkt.checkForDump(Conf.TreeDump) {
		bkt.dumpHtree()
	}

	bkt.loadGCHistroy()
	logger.Infof("bucket %x opened, max rss = %d, use time %s",
		bucketID, utils.GetMaxRSS(), time.Since(st))
	return nil
}

func abs(n int32) int32 {
	if n < 0 {
		return -n
	}
	return n
}

// check hash file, return true if the htree need to dump
func (bkt *Bucket) checkForDump(dumpthreshold int) bool {
	maxdata, err := bkt.datas.ListFiles()
	if err != nil {
		return false
	}
	logger.Infof("maxdata %d", maxdata)
	if maxdata < 0 {
		return false
	}
	htrees, ids := bkt.getAllIndex(HTREE_SUFFIX)
	for i := len(htrees) - 1; i >= 0; i-- {
		id := ids[i]
		if maxdata > id.Chunk+dumpthreshold {
			return false
		}
	}

	if len(ids) > 0 {
		return false
	}
	return true
}

// called by hstore, data already flushed
func (bkt *Bucket) close() {
	logger.Infof("closing bucket %s", bkt.Home)
	bkt.datas.flush(-1, true)
	datas, _ := filepath.Glob(fmt.Sprintf("%s/*.data", bkt.Home))
	if len(datas) == 0 {
		return
	}

	bkt.hints.dumpCollisions()
	bkt.hints.close()
	bkt.dumpHtree()
}

func (bkt *Bucket) dumpHtree() {
	hintID := bkt.hints.maxDumpedHintID
	if bkt.TreeID.isLarger(hintID.Chunk, hintID.Split) {
		bkt.removeHtree()
		bkt.TreeID = hintID
		bkt.htree.dump(bkt.getHtreePath(bkt.TreeID.Chunk, bkt.TreeID.Split))
	}
}

func (bkt *Bucket) getAllIndex(suffix string) (paths []string, ids []HintID) {
	pattern := getIndexPath(bkt.Home, -1, -1, suffix)
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
		utils.Remove(p)
	}
	bkt.TreeID = HintID{0, 0}
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
	if v.Ver >= 0 {
		rec := &Record{ki.Key, v}
		v.CalcValueHash()

		oldCap := rec.Payload.CArray.Cap
		rec.TryCompress()
		cmem.DBRL.SetData.AddSize(rec.Payload.CArray.Cap - oldCap)
	}
	bkt.writeLock.Lock()
	ok := false
	defer func() {
		bkt.writeLock.Unlock()
		if !ok && v.Ver >= 0 {
			cmem.DBRL.SetData.SubSizeAndCount(v.CArray.Cap)
			v.Free()
		}
	}()
	oldv := int32(0)
	payload, pos, err := bkt.get(ki, true)
	if err != nil {
		return err
	}

	if payload != nil {
		oldv = payload.Ver
		if oldv > 0 && v.ValueHash == payload.ValueHash {
			if Conf.CheckVHash {
				if v.Ver != 0 {
					// sync script would be here, e.g. set_raw(k, v, rev=xxx)
					bkt.htree.set(ki, &v.Meta, pos)
				}
				return nil
			}
			atomic.AddInt64(&bkt.NumSameVhash, 1)
			atomic.AddInt64(&bkt.SizeSameVhash, int64(len(v.Body)))
			bkt.SizeVhashKey = ki.StringKey
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
	ok = true
	bkt.set(ki, v)
	return nil
}

func (bkt *Bucket) set(ki *KeyInfo, v *Payload) error {
	pos, err := bkt.datas.AppendRecord(&Record{ki.Key, v})
	if err != nil {
		return err
	}
	bkt.htree.set(ki, &v.Meta, pos)
	bkt.hints.set(ki, &v.Meta, pos, v.RecSize, "set")
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
		pos = hintit.Pos
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
	beforeGetRecord := time.Now()
	rec, inbuffer, err := bkt.datas.GetRecordByPos(pos)
	getRecordTimeCost := time.Now().Sub(beforeGetRecord).Seconds() * 1000 // Millisecond
	if err != nil {
		// not remove for now: it may cause many sync
		// bkt.htree.remove(ki, pos)
		return
	} else if rec == nil {
		err = fmt.Errorf("bad htree item, get nothing, key %s pos %v inbuffer %v", ki.Key, pos, inbuffer)
		logger.Errorf("%s", err.Error())
		return
	} else if bytes.Compare(rec.Key, ki.Key) == 0 {
		payload = rec.Payload
		payload.Ver = meta.Ver
		analysisLogger.Infof("%s %d %f BKT_%02x %d %d %d %v %s",
			config.AnalysisLogVersion, rec.Payload.TS, getRecordTimeCost, bkt.ID,
			pos.ChunkID, pos.Offset, rec.Payload.RecSize, inbuffer, ki.StringKey)
		return
	}
	defer func() {
		cmem.DBRL.GetData.SubSizeAndCount(rec.Payload.CArray.Cap)
		rec.Payload.CArray.Free()
	}()

	keyhash := getKeyHash(rec.Key)
	if keyhash != ki.KeyHash {
		if inbuffer && pos.ChunkID < bkt.datas.newHead-1 {
			logger.Warnf("get out-of-date pos during gc, should be rarely seen, omit it, pos %v", pos)
			payload = nil
			return
		}

		// not remove for now: it may cause many sync
		// bkt.htree.remove(ki, pos)
		err = fmt.Errorf("bad htree item want (%s, %016x) got (%s, %016x), pos %x, inbuffer %v",
			ki.Key, ki.KeyHash, rec.Key, keyhash, pos, inbuffer)
		logger.Errorf("%s", err.Error())
		return
	}

	// here: same key hash, diff key

	hintit, chunkID, err := bkt.hints.getItem(ki.KeyHash, ki.StringKey, false)
	if err != nil || hintit == nil {
		return
	}

	vhash := uint16(0)
	if rec.Payload.Ver > 0 {
		vhash = Getvhash(rec.Payload.Body)
	}
	hintit2 := newHintItem(ki.KeyHash, rec.Payload.Ver, vhash, pos, string(rec.Key))
	bkt.hints.collisions.compareAndSet(hintit2, "get1") // the one in htree

	pos = Position{chunkID, hintit.Pos.Offset}
	hintit.Pos = pos
	bkt.hints.collisions.compareAndSet(hintit, "get2") // the one not in htree

	rec2, _, err := bkt.datas.GetRecordByPos(pos)
	if err != nil {
		logger.Errorf("%s", err.Error())
		return
	} else if rec2 != nil {
		payload = rec2.Payload
	}
	return
}

func (bkt *Bucket) incr(ki *KeyInfo, value int) int {
	var tofree *Payload
	var errFlag bool

	tofree, _, err := bkt.get(ki, false)
	if err != nil {
		errFlag = true
	}
	ver := int32(1)
	if tofree != nil {
		if tofree.Ver > 0 {
			if tofree.Flag != FLAG_INCR {
				logger.Warnf("incr with flag 0x%x", tofree.Flag)
				errFlag = true
			}
			if len(tofree.Body) > 22 {
				logger.Warnf("incr with large value %s...", string(tofree.Body[:22]))
				errFlag = true
				return 0
			}
			s := string(tofree.Body)
			v, err := strconv.Atoi(s)
			if err != nil {
				errFlag = true
				logger.Warnf("incr with value %s", s)
			}
			ver += tofree.Ver
			value += v
		}
	}

	if errFlag {
		if tofree != nil {
			cmem.DBRL.GetData.SubSizeAndCount(tofree.CArray.Cap)
			tofree.CArray.Free()
		}
		cmem.DBRL.SetData.SubCount(1)
		return 0
	}

	payload := &Payload{}
	payload.Flag = FLAG_INCR
	payload.Ver = ver
	payload.TS = uint32(time.Now().Unix())
	s := strconv.Itoa(value)
	payload.Body = []byte(s)
	payload.CalcValueHash()
	bkt.set(ki, payload)
	return value
}

func (bkt *Bucket) listDir(ki *KeyInfo) ([]byte, error) {
	return bkt.htree.ListDir(ki)
}

func (bkt *Bucket) getInfo() *BucketInfo {
	head := bkt.datas.newHead
	bkt.Pos = Position{head, bkt.datas.chunks[head].size}
	bkt.HintState = bkt.hints.state
	bkt.MaxDumpedHintID = bkt.hints.maxDumpedHintID
	n := len(bkt.GCHistory)
	if n > 0 {
		bkt.LastGC = &bkt.GCHistory[n-1]
	}
	bkt.DU, _ = utils.DirUsage(bkt.Home)
	return &bkt.BucketInfo
}

func (bkt *Bucket) GetRecordByKeyHash(ki *KeyInfo) (rec *Record, inbuffer bool, err error) {
	_, pos, found := bkt.htree.get(ki)
	if !found {
		return
	}
	return bkt.datas.GetRecordByPos(pos)
}

func (bkt *Bucket) getGCHistoryPath() string {
	return fmt.Sprintf("%s/%s", bkt.Home, "nextgc.txt")
}

func (bkt *Bucket) loadGCHistroy() (err error) {
	fd, err := os.Open(bkt.getGCHistoryPath())
	if err != nil {
		logger.Infof("%v", err)
		return
	}
	defer fd.Close()
	buf := make([]byte, 10)
	n, e := fd.Read(buf)
	if e != nil && e != io.EOF {
		err = e
		logger.Errorf("%v", err)
		return
	}
	s := string(buf[:n])
	s = strings.TrimSpace(s)
	n, err = strconv.Atoi(s)
	if err == nil {
		bkt.NextGCChunk = n
		logger.Infof("bucket %d load nextgc %d", bkt.ID, n)
	}
	return
}

func (bkt *Bucket) dumpGCHistroy() {

	p := bkt.getGCHistoryPath()
	fd, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logger.Errorf("%v", err)
		return
	}
	defer fd.Close()
	fd.WriteString(fmt.Sprintf("%d", bkt.NextGCChunk))
	logger.Infof("dump %s %d", p, bkt.NextGCChunk)
}
