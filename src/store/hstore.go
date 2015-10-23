package store

import (
	"fmt"
	"loghub"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

/*
hstore  do NOT known relation between khash and key
*/

var (
	logger                 = loghub.Default
	bucketPattern []string = []string{"0", "%x", "%02x", "%03x"}
	flushDataChan          = make(chan int, 1)
)

type HStore struct {
	buckets       []*Bucket
	homeToBuckets []map[int]bool
	gcMgr         *gcMgr
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
	s := "09AF"
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
	}
	valid = true
	return
}

// TODO: allow rescan
func (store *HStore) scanBuckets() (err error) {
	for _, home := range config.Homes {
		homefile, err := os.Open(home)
		if err != nil {
			return err
		}
		fileinfos, err := homefile.Readdir(-1)
		if err != nil {
			return err
		}

		homefile.Close()
		for i, fi := range fileinfos {
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
					os.Remove(fullpath)
				} else {
					if store.buckets[bucketID].state > 0 {
						return fmt.Errorf("found dup bucket %d", bucketID)
					}
					logger.Debugf("found bucket %d", bucketID)
					store.buckets[bucketID].state = 1
					store.buckets[bucketID].homeID = i
					store.homeToBuckets[i][bucketID] = true
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
	return
}

func (store *HStore) Flusher() {

	for {
		select {
		case <-flushDataChan:
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

func (store *HStore) ListDir(ki *KeyInfo) ([]byte, error) {
	ki.Prepare()
	if ki.BucketID >= 0 {
		return store.buckets[ki.BucketID].listDir(ki)
	} else {
		// TODO: summarize to 16 groups according to path
	}
	return nil, nil
}

func (store *HStore) GC(bucketID, beginChunkID, endChunkID int) error {
	return store.gcMgr.gc(store.buckets[bucketID], beginChunkID, endChunkID)
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
