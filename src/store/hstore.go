package store

import (
	"bytes"
	"cmem"
	"fmt"
	"loghub"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

/*
hstore  do NOT known relation between khash and key
*/

var (
	logger                 = loghub.Default
	bucketPattern []string = []string{"0", "%x", "%02x", "%03x"}
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
	if config.TreeDepth == 0 {
		if name == "0" {
			return true, 0
		} else {
			return
		}
	} else if len(name) != config.TreeDepth {
		return
	}
	s := "09af"
	for i := 0; i < config.TreeDepth; i++ {
		b := name[i]
		if !((b >= s[0] && b <= s[1]) || (b >= s[2] && b <= s[3])) {
			return
		}
	}
	bucketID64, _ := strconv.ParseInt(name, 16, 32)
	bucketID = int(bucketID64)
	if bucketID >= config.NumBucket {
		logger.Fatalf("Bug: wrong bucketid %s->%d", name, bucketID)
		return
	}
	valid = true
	return
}

// TODO: allow rescan
func (store *HStore) scanBuckets() (err error) {

	for homeid, home := range config.Homes {
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
			valid, bucketID := checkBucketDir(fi)
			fullpath := filepath.Join(home, fi.Name())
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
					if store.buckets[bucketID].state > 0 {
						return fmt.Errorf("found dup bucket %d", bucketID)
					}
					logger.Infof("found bucket %x in %s", bucketID, config.Homes[homeid])
					store.buckets[bucketID].state = 1
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
	min := config.NumBucket
	for i := 0; i < len(config.Homes); i++ {
		l := len(store.homeToBuckets[i])
		if l < min {
			homeid = i
			min = l
		}
	}

	store.buckets[bucketID].state = 1
	store.buckets[bucketID].homeID = homeid
	store.homeToBuckets[homeid][bucketID] = true

	dirpath := store.getBucketPath(homeid, bucketID)
	err = os.Mkdir(dirpath, 0755)
	logger.Infof("allocBucket %s", dirpath)
	return
}

func (store *HStore) getBucketPath(homeID, bucketID int) string {
	dirname := fmt.Sprintf(bucketPattern[len(config.Homes)], bucketID)
	return filepath.Join(config.Homes[homeID], dirname)
}

func NewHStore() (store *HStore, err error) {
	store = new(HStore)
	store.gcMgr = new(GCMgr)
	store.buckets = make([]*Bucket, config.NumBucket)
	for i := 0; i < config.NumBucket; i++ {
		store.buckets[i] = &Bucket{id: i}
	}
	nhome := len(config.Homes)
	store.homeToBuckets = make([]map[int]bool, nhome)
	for i := 0; i < nhome; i++ {
		store.homeToBuckets[i] = make(map[int]bool)
	}

	err = store.scanBuckets()
	if err != nil {
		return
	}

	for i := 0; i < config.NumBucket; i++ {
		need := config.Buckets[i] > 0
		found := store.buckets[i].state > 0
		if need {
			if !found {
				err = store.allocBucket(i)
				if err != nil {
					return
				}
			}
		} else {
			if found {
				logger.Warnf("found unexpect bucket %d", i)
			}
		}
	}

	for i := 0; i < config.NumBucket; i++ {
		bkt := store.buckets[i]
		if config.Buckets[i] > 0 {
			err = bkt.open(i, store.getBucketPath(bkt.homeID, i))
			if err != nil {
				return
			}
		}
	}
	if config.TreeDepth > 0 {
		store.htree = newHTree(0, 0, config.TreeDepth+1)
	}

	go store.merger(1 * time.Minute)
	return
}

func (store *HStore) Flusher() {

	for {
		select {
		case <-cmem.Chans[cmem.TagFlushData]:
		case <-time.After(time.Duration(dataConfig.DataFlushSec) * time.Second):
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
	if len(ki.Key) >= config.TreeDepth {
		return store.buckets[ki.BucketID].listDir(ki)
	}
	return store.ListUpper(ki)
}

func (store *HStore) GC(bucketID, beginChunkID, endChunkID int) error {
	if bucketID >= config.NumBucket {
		return fmt.Errorf("bad bucket id")
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
	return store.buckets[bucketID].getInfo(keys)
}

func (store *HStore) Get(ki *KeyInfo, memOnly bool) (payload *Payload, pos Position, err error) {
	ki.KeyHash = getKeyHash(ki.Key)
	ki.Prepare()
	return store.buckets[ki.BucketID].get(ki, memOnly)
}

func (store *HStore) Set(ki *KeyInfo, p *Payload) error {
	ki.KeyHash = getKeyHash(ki.Key)
	ki.Prepare()
	return store.buckets[ki.BucketID].getset(ki, p)
}

func (store *HStore) GetRecordByKeyHash(ki *KeyInfo) (*Record, error) {
	ki.Prepare()
	return store.buckets[ki.BucketID].GetRecordByKeyHash(ki)
}
func (store *HStore) Incr(ki *KeyInfo, value int) int {
	ki.KeyHash = getKeyHash(ki.Key)
	ki.Prepare()
	return store.buckets[ki.BucketID].incr(ki, value)
}

func (store *HStore) merger(interval time.Duration) {
	for {
		<-time.After(interval)
		for _, bkt := range store.buckets {
			if bkt.state > 0 {
				bkt.hints.dumpAndMerge(false)
			}
		}
	}
}

func (store *HStore) GetCollisionsByBucket(bucketID int) (content []byte) {
	bkt := store.buckets[bucketID]
	if bkt.state > 0 {
		return bkt.hints.collisions.dumps()
	}
	return
}
