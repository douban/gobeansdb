package store

import (
	"bytes"
	"cmem"
	"config"
	"fmt"
	"loghub"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

var (
	logger                 = loghub.Default
	bucketPattern []string = []string{"0", "%x", "%02x", "%03x"}
	mergeChan     chan int

	conf       = &config.DB
	hintConfig = &conf.HintConfig
)

type HStore struct {
	buckets       []*Bucket
	homeToBuckets []map[int]bool
	gcMgr         *GCMgr
	htree         *HTree
	htreeLock     sync.Mutex
}

func checkBucketDir(fi os.FileInfo) (valid bool, bucketID int) {
	if !fi.IsDir() {
		return
	}
	name := fi.Name()
	if conf.TreeDepth == 0 {
		if name == "0" {
			return true, 0
		} else {
			return
		}
	} else if len(name) != conf.TreeDepth {
		return
	}
	s := "09af"
	for i := 0; i < conf.TreeDepth; i++ {
		b := name[i]
		if !((b >= s[0] && b <= s[1]) || (b >= s[2] && b <= s[3])) {
			return
		}
	}
	bucketID64, _ := strconv.ParseInt(name, 16, 32)
	bucketID = int(bucketID64)
	if bucketID >= conf.NumBucket {
		logger.Fatalf("Bug: wrong bucketid %s->%d", name, bucketID)
		return
	}
	valid = true
	return
}

// TODO: allow rescan
func (store *HStore) scanBuckets() (err error) {

	for homeid, home := range conf.Homes {
		homefile, err := os.Open(home)
		if err != nil {
			return err
		}
		fileinfos, err := homefile.Readdir(-1)
		if err != nil {
			return err
		}

		homefile.Close()
		for _, fi := range fileinfos {
			fullpath := filepath.Join(home, fi.Name())
			if fi.Mode()&os.ModeSymlink != 0 {
				fi, err = os.Stat(fullpath)
				if err != nil {
					return err
				}
			}
			valid, bucketID := checkBucketDir(fi)

			datas, err := filepath.Glob(filepath.Join(fullpath, "*.data"))
			if err != nil {
				logger.Errorf("%s", err.Error())
				return err
			}
			empty := (len(datas) == 0)
			if valid {
				if empty {
					logger.Warnf("remove empty bucket dir %s", fullpath)
					if err = os.RemoveAll(fullpath); err != nil {
						logger.Errorf("fail to delete empty bucket %s", fullpath)
					}
				} else {
					if store.buckets[bucketID].state > BUCKET_STAT_EMPTY {
						return fmt.Errorf("found dup bucket %d", bucketID)
					}
					logger.Infof("found bucket %x in %s", bucketID, conf.Homes[homeid])
					store.buckets[bucketID].state = BUCKET_STAT_NOT_EMPTY
					store.buckets[bucketID].homeID = homeid
					store.homeToBuckets[homeid][bucketID] = true
				}
			} else {
				logger.Warnf("found unknown file %#v in %s", fi.Name(), home)
			}
		}
	}
	return nil
}

func (store *HStore) allocBucket(bucketID int) (err error) {
	homeid := 0
	min := conf.NumBucket
	for i := 0; i < len(conf.Homes); i++ {
		l := len(store.homeToBuckets[i])
		if l < min {
			homeid = i
			min = l
		}
	}
	store.buckets[bucketID].homeID = homeid
	store.homeToBuckets[homeid][bucketID] = true

	dirpath := store.getBucketPath(homeid, bucketID)
	err = os.Mkdir(dirpath, 0755)
	logger.Infof("allocBucket %s", dirpath)
	return
}

func (store *HStore) getBucketPath(homeID, bucketID int) string {
	dirname := fmt.Sprintf(bucketPattern[len(conf.Homes)], bucketID)
	return filepath.Join(conf.Homes[homeID], dirname)
}

func initHomes() {
	for _, s := range conf.Homes {
		if err := os.MkdirAll(s, os.ModePerm); err != nil {
			logger.Fatalf("fail to init home %s", s)
		}
	}
}

func NewHStore() (store *HStore, err error) {
	initHomes()
	mergeChan = nil
	cmem.DBRL.ResetAll()
	st := time.Now()
	store = new(HStore)
	store.gcMgr = new(GCMgr)
	store.buckets = make([]*Bucket, conf.NumBucket)
	for i := 0; i < conf.NumBucket; i++ {
		store.buckets[i] = &Bucket{id: i}
	}
	nhome := len(conf.Homes)
	store.homeToBuckets = make([]map[int]bool, nhome)
	for i := 0; i < nhome; i++ {
		store.homeToBuckets[i] = make(map[int]bool)
	}

	err = store.scanBuckets()
	if err != nil {
		return
	}

	for i := 0; i < conf.NumBucket; i++ {
		need := conf.Buckets[i] > 0
		found := (store.buckets[i].state >= BUCKET_STAT_NOT_EMPTY)
		if need {
			if !found {
				err = store.allocBucket(i)
				if err != nil {
					return
				}

			}
			store.buckets[i].state = BUCKET_STAT_READY
		} else {
			if found {
				logger.Warnf("found unexpect bucket %d", i)
			}
		}
	}

	n := 0
	for i := 0; i < conf.NumBucket; i++ {
		bkt := store.buckets[i]
		if conf.Buckets[i] > 0 {
			err = bkt.open(i, store.getBucketPath(bkt.homeID, i))
			if err != nil {
				return
			}
			n += 1
		}
	}
	if conf.TreeDepth > 0 {
		store.htree = newHTree(0, 0, conf.TreeDepth+1)
	}
	logger.Infof("all %d bucket loaded, ready to serve, maxrss = %d, use time %s", n, GetMaxRSS(), time.Since(st))
	return
}

func (store *HStore) Flusher() {

	for {
		select {
		case <-cmem.DBRL.FlushData.Chan:
		case <-time.After(time.Duration(conf.FlushInterval) * time.Second):
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
		if b.state == BUCKET_STAT_READY {
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
		}
	} else {
		bkt := store.buckets[offset]
		if bkt.htree != nil {
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
	if len(ki.Key) >= conf.TreeDepth {
		bkt := store.buckets[ki.BucketID]
		if bkt.state != BUCKET_STAT_READY {
			return nil, nil
		}
		return bkt.listDir(ki)
	}
	return store.ListUpper(ki)
}

func (store *HStore) GC(bucketID, beginChunkID, endChunkID int) error {
	if bucketID >= conf.NumBucket {
		return fmt.Errorf("bad bucket id")
	}
	bkt := store.buckets[bucketID]
	if bkt.state != BUCKET_STAT_READY {
		return nil
	}
	if store.gcMgr.stat != nil && store.gcMgr.stat.Running {
		return fmt.Errorf("already running")
	}
	go store.gcMgr.gc(store.buckets[bucketID], beginChunkID, endChunkID)
	return nil
}

func (store *HStore) GCStat() (int, *GCState) {
	return store.gcMgr.bucketID, store.gcMgr.stat
}

func (store *HStore) GetBucketInfo(bucketID int, keys []string) ([]byte, error) {
	bkt := store.buckets[bucketID]
	if bkt.state != BUCKET_STAT_READY {
		return nil, nil
	}
	return bkt.getInfo(keys)
}

func (store *HStore) Get(ki *KeyInfo, memOnly bool) (payload *Payload, pos Position, err error) {
	ki.KeyHash = getKeyHash(ki.Key)
	ki.Prepare()
	bkt := store.buckets[ki.BucketID]
	if bkt.state != BUCKET_STAT_READY {
		return
	}
	return bkt.get(ki, memOnly)
}

func (store *HStore) Set(ki *KeyInfo, p *Payload) error {
	p.AccountingSize = int64(len(p.Body) + len(ki.Key))
	ki.KeyHash = getKeyHash(ki.Key)
	ki.Prepare()
	bkt := store.buckets[ki.BucketID]
	if bkt.state != BUCKET_STAT_READY {
		return nil
	}
	return bkt.checkAndSet(ki, p)
}

func (store *HStore) GetRecordByKeyHash(ki *KeyInfo) (*Record, error) {
	ki.Prepare()
	bkt := store.buckets[ki.BucketID]
	if bkt.state != BUCKET_STAT_READY {
		return nil, nil
	}
	return bkt.GetRecordByKeyHash(ki)
}

func (store *HStore) Incr(ki *KeyInfo, value int) int {
	ki.KeyHash = getKeyHash(ki.Key)
	ki.Prepare()
	bkt := store.buckets[ki.BucketID]
	if bkt.state != BUCKET_STAT_READY {
		return 0
	}
	return bkt.incr(ki, value)
}

func (store *HStore) HintDumper(interval time.Duration) {
	logger.Infof("hint merger started")
	mergeChan = make(chan int, 2)
	for {
		for _, bkt := range store.buckets {
			if bkt.state == BUCKET_STAT_READY {
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
	if bkt.state == BUCKET_STAT_READY {
		return bkt.hints.collisions.dumps()
	}
	return
}
