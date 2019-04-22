package store

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
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

var (
	logger       = loghub.ErrorLogger
	mergeChan    chan int
	gcLock       sync.Mutex
	gcContext    = context.Background()
	gcContextMap = struct {
		m  map[int]GCCancelCtx
		rw sync.RWMutex
	}{m: make(map[int]GCCancelCtx)}
)

type HStore struct {
	buckets   []*Bucket
	gcMgr     *GCMgr
	htree     *HTree
	htreeLock sync.Mutex
}

// TODO: allow rescan
func (store *HStore) scanBuckets() (err error) {
	for id := 0; id < Conf.NumBucket; id++ {
		path := GetBucketPath(id)
		fi, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			logger.Infof("%s", err.Error())
			return err
		}
		if !fi.IsDir() {
			err = fmt.Errorf("%s is not dir", path)
			logger.Errorf("%s", err.Error())
			return err
		}

		datas, err := filepath.Glob(filepath.Join(path, "*.data"))
		if err != nil {
			logger.Errorf("%s", err.Error())
			return err
		}
		if len(datas) == 0 {
			if Conf.NumBucket > 1 {
				logger.Warnf("remove empty bucket dir %s", path)
				if err = os.RemoveAll(path); err != nil {
					logger.Errorf("fail to delete empty bucket %s", path)
				}
			}
		} else {
			logger.Infof("found bucket %x", id)
			store.buckets[id].State = BUCKET_STAT_NOT_EMPTY
		}
	}
	return nil
}

func (store *HStore) allocBucket(bucketID int) (err error) {
	dirpath := GetBucketPath(bucketID)
	if _, err = os.Stat(dirpath); err != nil {
		err = os.MkdirAll(dirpath, 0755)
	}
	logger.Infof("allocBucket %s", dirpath)
	return
}

func NewHStore() (store *HStore, err error) {
	home := Conf.Home
	if err := os.MkdirAll(home, os.ModePerm); err != nil {
		logger.Fatalf("fail to init home %s", home)
	}
	mergeChan = nil
	cmem.DBRL.ResetAll()
	st := time.Now()
	store = new(HStore)
	store.gcMgr = &GCMgr{stat: make(map[int]*GCState)}
	store.buckets = make([]*Bucket, Conf.NumBucket)
	for i := 0; i < Conf.NumBucket; i++ {
		store.buckets[i] = &Bucket{}
		store.buckets[i].ID = i
	}
	err = store.scanBuckets()
	if err != nil {
		return
	}

	for i := 0; i < Conf.NumBucket; i++ {
		need := Conf.BucketsStat[i] > 0
		found := store.buckets[i].State >= BUCKET_STAT_NOT_EMPTY
		if need {
			if !found {
				err = store.allocBucket(i)
				if err != nil {
					return
				}

			}
			store.buckets[i].State = BUCKET_STAT_READY
		} else {
			if found {
				logger.Warnf("found unexpect bucket %d", i)
			}
		}
	}

	var n int32
	var wg = sync.WaitGroup{}
	wg.Add(Conf.NumBucket)
	errs := make(chan error, Conf.NumBucket)
	for i := 0; i < Conf.NumBucket; i++ {
		go func(id int) {
			defer wg.Done()
			bkt := store.buckets[id]
			if Conf.BucketsStat[id] > 0 {
				err = bkt.open(id, GetBucketPath(id))
				if err != nil {
					logger.Errorf("Error in bkt open %s", err.Error())
					errs <- err
				} else {
					atomic.AddInt32(&n, 1)
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			err = e
			return
		}
	}
	if Conf.TreeDepth > 0 {
		store.htree = newHTree(0, 0, Conf.TreeDepth+1)
	}
	logger.Infof("all %d bucket loaded, ready to serve, maxrss = %d, use time %s",
		n, utils.GetMaxRSS(), time.Since(st))
	return
}

func (store *HStore) Flusher() {

	for {
		select {
		case <-cmem.DBRL.FlushData.Chan:
		case <-time.After(time.Duration(Conf.FlushInterval) * time.Second):
		}
		store.flushdatas(false)
	}
}

func (store *HStore) flushdatas(force bool) {
	for _, b := range store.buckets {
		if b.datas != nil {
			b.datas.flush(-1, force)
		}
	}
}

func (store *HStore) Close() {
	for _, b := range store.buckets {
		if b.datas != nil {
			b.close()
		}
	}
}

func (store *HStore) NumKey() (n int) {
	for _, b := range store.buckets {
		if b.State == BUCKET_STAT_READY {
			n += int(b.htree.levels[0][0].count)
		}
	}
	return
}

func (store *HStore) updateNodesUpper(level, offset int) (node *Node) {
	tree := store.htree
	node = &tree.levels[level][offset]
	node.hash = 0
	node.count = 0
	if level < len(tree.levels)-1 {
		for i := 0; i < 16; i++ {
			cnode := store.updateNodesUpper(level+1, offset*16+i)
			node.hash *= 97
			node.hash += cnode.hash
			node.count += cnode.count
		}
	} else {
		bkt := store.buckets[offset]
		if bkt.State == BUCKET_STAT_READY && bkt.htree != nil {
			root := bkt.htree.Update()
			node.hash = root.hash
			node.count = root.count
		}
	}
	return
}

func (store *HStore) ListUpper(ki *KeyInfo) ([]byte, error) {
	store.htreeLock.Lock()
	defer store.htreeLock.Unlock()
	tree := store.htree
	l := len(ki.KeyPath)
	offset := 0
	for h := 0; h < l; h += 1 {
		offset = offset*16 + ki.KeyPath[h]
	}
	store.updateNodesUpper(l, offset)
	l += 1
	offset *= 16
	var buffer bytes.Buffer
	nodes := tree.levels[l][offset : offset+16]
	for i := 0; i < 16; i++ {
		n := &nodes[i]
		s := fmt.Sprintf("%x/ %d %d\n", i, n.hash, int(n.count))
		buffer.WriteString(s)
	}
	return buffer.Bytes(), nil
}

func (store *HStore) ListDir(ki *KeyInfo) ([]byte, error) {
	err := ki.Prepare()
	if err != nil {
		return nil, nil
	}
	if len(ki.Key) >= Conf.TreeDepth {
		bkt := store.buckets[ki.BucketID]
		if bkt.State != BUCKET_STAT_READY {
			return nil, nil
		}
		return bkt.listDir(ki)
	}
	return store.ListUpper(ki)
}

func (store *HStore) GCBuckets() string {
	var result strings.Builder
	store.gcMgr.mu.Lock()
	for k := range store.gcMgr.stat {
		result.WriteString(strconv.FormatInt(int64(k), 16))
		result.WriteString(",")
	}
	store.gcMgr.mu.Unlock()
	return strings.TrimSuffix(result.String(), ",")
}

func (store *HStore) GC(bucketID, beginChunkID, endChunkID, noGCDays int, merge, pretend bool) (begin, end int, err error) {
	if bucketID >= Conf.NumBucket {
		err = fmt.Errorf("bad bucket id: %d", bucketID)
		return
	}
	bkt := store.buckets[bucketID]
	if bkt.State != BUCKET_STAT_READY {
		err = fmt.Errorf("no datay for bucket id: %d", bucketID)
		return
	}

	checkGC := func() error {
		store.gcMgr.mu.RLock()
		defer store.gcMgr.mu.RUnlock()
		if _, exists := store.gcMgr.stat[bucketID]; exists {
			err := fmt.Errorf("gc on bkt: %d already running", bucketID)
			return err
		}
		return nil
	}
	if err = checkGC(); err != nil {
		return
	}

	begin, end, err = bkt.gcCheckRange(beginChunkID, endChunkID, noGCDays)
	if err != nil {
		return
	}
	if pretend {
		return
	}

	ctx, cancel := context.WithCancel(gcContext)
	gcContextMap.rw.Lock()
	gcContextMap.m[bucketID] = GCCancelCtx{Cancel: cancel, ChunkChan: make(chan int, 1)}
	ch := gcContextMap.m[bucketID].ChunkChan
	gcContextMap.rw.Unlock()

	go store.gcMgr.gc(ctx, ch, bkt, begin, end, merge)
	return
}

func (store *HStore) CancelGC(bucketID int) (bktID, chunkID int) {
	// prevent same bucket concurrent request
	gcLock.Lock()
	defer gcLock.Unlock()

	// will delete key at goroutine in store/gc.go
	gcContextMap.rw.RLock()
	bktGCCancelCtx, ok := gcContextMap.m[bucketID]
	gcContextMap.rw.RUnlock()

	bktID = bucketID
	if ok {
		bktGCCancelCtx.Cancel()
		chunkID = <-bktGCCancelCtx.ChunkChan
	} else {
		chunkID = -1
	}
	return
}

func (store *HStore) IsGCRunning() bool {
	var result bool
	store.gcMgr.mu.RLock()
	defer store.gcMgr.mu.RUnlock()
	if len(store.gcMgr.stat) > 0 {
		result = true
	}

	return result
}

func (store *HStore) GetBucketInfo(bucketID int) *BucketInfo {
	if bucketID < 0 || bucketID >= len(store.buckets) {
		return nil
	}
	bkt := store.buckets[bucketID]
	if bkt.State != BUCKET_STAT_READY {
		return nil
	}
	return bkt.getInfo()
}

func (store *HStore) Get(ki *KeyInfo, memOnly bool) (payload *Payload, pos Position, err error) {
	ki.KeyHash = getKeyHash(ki.Key)
	ki.Prepare()
	bkt := store.buckets[ki.BucketID]
	atomic.AddInt64(&bkt.NumGet, 1)
	if bkt.State != BUCKET_STAT_READY {
		return
	}
	return bkt.get(ki, memOnly)
}

func (store *HStore) Set(ki *KeyInfo, p *Payload) error {
	ki.KeyHash = getKeyHash(ki.Key)
	ki.Prepare()

	bkt := store.buckets[ki.BucketID]
	atomic.AddInt64(&bkt.NumSet, 1)
	if bkt.State != BUCKET_STAT_READY {
		cmem.DBRL.SetData.SubSizeAndCount(p.CArray.Cap)
		p.CArray.Free()
		return nil
	}

	return bkt.checkAndSet(ki, p)
}

func (store *HStore) GetRecordByKeyHash(ki *KeyInfo) (*Record, bool, error) {
	ki.Prepare()
	bkt := store.buckets[ki.BucketID]
	if bkt.State != BUCKET_STAT_READY {
		return nil, false, nil
	}
	return bkt.GetRecordByKeyHash(ki)
}

func (store *HStore) Incr(ki *KeyInfo, value int) int {
	ki.KeyHash = getKeyHash(ki.Key)
	ki.Prepare()
	bkt := store.buckets[ki.BucketID]
	if bkt.State != BUCKET_STAT_READY {
		return 0
	}
	return bkt.incr(ki, value)
}

func (store *HStore) HintDumper(interval time.Duration) {
	logger.Infof("hint merger started")
	mergeChan = make(chan int, 2)
	for {
		for _, bkt := range store.buckets {
			if bkt.State == BUCKET_STAT_READY {
				bkt.hints.dumpAndMerge(false)
			}
		}
		select {
		case _ = <-mergeChan:
		case <-time.After(interval):
		}
	}
}

func (store *HStore) GetCollisionsByBucket(bucketID int) (content []byte) {
	bkt := store.buckets[bucketID]
	if bkt.State == BUCKET_STAT_READY {
		return bkt.hints.collisions.dumps()
	}
	return
}

func GetPayloadForDelete() *Payload {
	payload := &Payload{}
	payload.Flag = 0
	payload.Body = nil
	payload.Ver = -1
	payload.TS = uint32(time.Now().Unix())
	return payload
}

type DU struct {
	Disks      map[string]utils.DiskStatus
	BucketsHex map[string]int64
	Buckets    map[int]int64 `json:"-"`
	Errs       []string
}

func NewDU() (du *DU) {
	du = &DU{}
	du.Disks = make(map[string]utils.DiskStatus)
	du.Buckets = make(map[int]int64)
	du.BucketsHex = make(map[string]int64)
	return
}

func (store *HStore) GetDU() (du *DU) {
	du = NewDU()
	for i, bkt := range store.buckets {
		if bkt.State == BUCKET_STAT_READY {
			fsu, e := utils.DiskUsage(bkt.Home)
			if e != nil {
				du.Errs = append(du.Errs, e.Error())
			} else {
				old, ok := du.Disks[fsu.Root]
				if ok {
					fsu.Buckets = append(old.Buckets, i)
				} else {
					fsu.Buckets = []int{i}
				}
				du.Disks[fsu.Root] = fsu
			}

			diru, e := utils.DirUsage(bkt.Home)
			if e != nil {
				du.Errs = append(du.Errs, e.Error())
			} else {
				du.Buckets[i] = diru
				du.BucketsHex[config.BucketIDHex(i, Conf.NumBucket)] = diru
			}
		}
	}
	return
}

func (store *HStore) ChangeRoute(newConf config.DBRouteConfig) (loaded, unloaded []int, err error) {
	for i := 0; i < Conf.NumBucket; i++ {
		bkt := store.buckets[i]
		oldc := Conf.BucketsStat[i]
		newc := newConf.BucketsStat[i]
		if newc != oldc {
			if newc >= BUCKET_STAT_NOT_EMPTY {

				logger.Infof("hot load bucket %d", i)
				err = store.allocBucket(i)
				if err != nil {
					return
				}

				err = bkt.open(i, GetBucketPath(i))
				if err != nil {
					return
				}
				// check status before serve
				bkt.State = BUCKET_STAT_READY
				Conf.BucketsStat[i] = BUCKET_STAT_READY
				loaded = append(loaded, i)
			} else {
				logger.Infof("hot unload bucket %d", i)
				Conf.BucketsStat[i] = BUCKET_STAT_EMPTY
				bkt.State = BUCKET_STAT_EMPTY
				time.Sleep(time.Second * 10)
				bkt.close()
				bkt.release()
				logger.Infof("bucket %d unload done", i)
				unloaded = append(unloaded, i)
			}
		}
	}
	return
}

func (store *HStore) GetNumCmdByBuckets() (counts [][]int64) {
	n := Conf.NumBucket
	counts = make([][]int64, n)
	for i := 0; i < n; i++ {
		bkt := store.buckets[i]
		counts[i] = []int64{int64(bkt.State), bkt.NumGet, bkt.NumSet}
	}
	return
}
