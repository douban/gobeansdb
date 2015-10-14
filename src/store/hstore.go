package store

import (
	"fmt"
	"loghub"
	"os"
	"path/filepath"
	"strconv"
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
	gcMgr         *gcMgr
}

func (store *HStore) prepare(key []byte, keyhash uint64) (keyinfo *KeyInfo) {
	return getKeyInfo(key, keyhash, false)
}

func (store *HStore) Get(key []byte, khash uint64, memOnly bool) (payload *Payload, pos Position, err error) {
	ki := store.prepare(key, khash)
	return store.buckets[ki.BucketID].get(ki, memOnly)
}

func (store *HStore) Set(key []byte, khash uint64, p *Payload) error {
	keyinfo := store.prepare(key, khash)
	return store.buckets[keyinfo.BucketID].set(keyinfo, p)
}

func checkBucketDir(fi os.FileInfo) (valid bool, bucketID int) {
	if !fi.IsDir() {
		return
	}
	name := fi.Name()
	if len(name) != config.TreeDepth {
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
				return err
			}
			empty := (len(datas) > 0)
			if valid {
				if empty {
					os.Remove(fullpath)
				} else {
					if store.buckets[bucketID].state > 0 {
						return fmt.Errorf("found dup bucket %d", bucketID)
					}
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
	err = os.Mkdir(dirpath, 0644)
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

func (store *HStore) Close() {
	for _, b := range store.buckets {
		b.close()
	}
}

func (store *HStore) GetValue(key []byte, keyhash uint64) ([]byte, error) {
	payload, _, err := store.Get(key, keyhash, false)
	if err != nil {
		return nil, err
	}
	return payload.Value, nil
}

func (store *HStore) GetRecord(key []byte, keyhash uint64) ([]byte, error) {
	payload, _, err := store.Get(key, keyhash, false)
	if err != nil {
		return nil, err
	}
	return DumpRecord(&Record{key, payload}), err
}

func (store *HStore) GetMeta(key []byte, keyhash uint64, memOnly bool) (string, error) {
	payload, pos, err := store.Get(key, keyhash, memOnly)
	if err != nil {
		return "", err
	}
	res := fmt.Sprintf("%d %d %d %d %d %d", payload.TS, payload.Ver, payload.ValueHash, pos.ChunkID, pos.Offset)
	return res, nil // TODO
}

func (store *HStore) ListDir(path []byte) ([]byte, error) {
	keyinfo := getKeyInfo(path, 0, true)
	if keyinfo == nil {
		return nil, fmt.Errorf("bad key to list")
	}
	if keyinfo.BucketID >= 0 {
		return store.buckets[keyinfo.BucketID].listDir(keyinfo.KeyPath)
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
