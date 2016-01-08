package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.intra.douban.com/coresys/gobeansdb/cmem"
	"github.intra.douban.com/coresys/gobeansdb/config"
	"github.intra.douban.com/coresys/gobeansdb/loghub"
	"github.intra.douban.com/coresys/gobeansdb/utils"
)

var (
	logger                 = loghub.ErrorLogger
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
					if store.buckets[bucketID].State > BUCKET_STAT_EMPTY {
						return fmt.Errorf("found dup bucket %d", bucketID)
					}
					logger.Infof("found bucket %x in %s", bucketID, conf.Homes[homeid])
					store.buckets[bucketID].State = BUCKET_STAT_NOT_EMPTY
					store.buckets[bucketID].HomeID = homeid
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
	store.buckets[bucketID].HomeID = homeid
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
		store.buckets[i] = &Bucket{}
		store.buckets[i].ID = i

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
		need := conf.BucketsStat[i] > 0
		found := (store.buckets[i].State >= BUCKET_STAT_NOT_EMPTY)
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

	n := 0
	for i := 0; i < conf.NumBucket; i++ {
		bkt := store.buckets[i]
		if conf.BucketsStat[i] > 0 {
			err = bkt.open(i, store.getBucketPath(bkt.HomeID, i))
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
		if bkt.State != BUCKET_STAT_READY {
			return nil, nil
		}
		return bkt.listDir(ki)
	}
	return store.ListUpper(ki)
}

func (store *HStore) GC(bucketID, beginChunkID, endChunkID, noGCDays int, merge, pretend bool) (begin, end int, err error) {
	if bucketID >= conf.NumBucket {
		err = fmt.Errorf("bad bucket id: %d", bucketID)
		return
	}
	bkt := store.buckets[bucketID]
	if bkt.State != BUCKET_STAT_READY {
		err = fmt.Errorf("no datay for bucket id: %d", bucketID)
		return
	}
	if store.gcMgr.stat != nil && store.gcMgr.stat.Running {
		err = fmt.Errorf("already running")
		return
	}
	begin, end, err = bkt.gcCheckRange(beginChunkID, endChunkID, noGCDays)
	if err != nil {
		return
	}
	if pretend {
		return
	}
	go store.gcMgr.gc(bkt, begin, end, merge)
	return
}

func (store *HStore) GCStat() (int, *GCState) {
	return store.gcMgr.bucketID, store.gcMgr.stat
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
	if bkt.State != BUCKET_STAT_READY {
		return
	}
	return bkt.get(ki, memOnly)
}

func (store *HStore) Set(ki *KeyInfo, p *Payload) error {
	ki.KeyHash = getKeyHash(ki.Key)
	ki.Prepare()
	bkt := store.buckets[ki.BucketID]
	if bkt.State != BUCKET_STAT_READY {
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
				du.BucketsHex[config.BucketIDHex(i, conf.NumBucket)] = diru
			}
		}
	}
	return
}
