package store

import (
	"fmt"
	"path/filepath"

	"github.com/douban/gobeansdb/config"
	"github.com/douban/gobeansdb/utils"
)

var (
	KHASH_LENS = [8]int{8, 8, 7, 7, 6, 6, 5, 5}
	Conf       *HStoreConfig
)

func init() {
	Conf = &HStoreConfig{}
	Conf.InitDefault()
}

type HStoreConfig struct {
	config.DBRouteConfig `yaml:"-"` // from route table
	DBLocalConfig        `yaml:"local,omitempty"`

	DataConfig  `yaml:"data,omitempty"`
	HintConfig  `yaml:"hint,omitempty"`
	HTreeConfig `yaml:"htree,omitempty"`
}

type HtreeDerivedConfig struct {
	TreeDepth       int // from NumBucket
	TreeKeyHashMask uint64
	TreeKeyHashLen  int
}

type DBLocalConfig struct {
	Home string `yaml:",omitempty"`
}

type DataConfig struct {
	FlushWake     int64 `yaml:"-"`                        // after set to flush buffer, wake up flush go routine if buffer size > this
	DataFileMax   int64 `yaml:"-"`                        // data rotate when reach the size
	CheckVHash    bool  `yaml:"check_vhash,omitempty"`    // not really set if vhash is the same
	FlushInterval int   `yaml:"flush_interval,omitempty"` // the flush go routine run at this interval
	NoGCDays      int   `yaml:"no_gc_days,omitempty"`     // not data files whose mtime in recent NoGCDays days

	FlushWakeStr   string          `yaml:"flush_wake_str"` //
	DataFileMaxStr string          `yaml:"datafile_max_str,omitempty"`
	NotCompress    map[string]bool `yaml:"not_compress,omitempty"` // kind do not compress
}

type HTreeConfig struct {
	TreeHeight int `yaml:"tree_height,omitempty"`
	TreeDump   int `yaml:"tree_dump,omitempty"`

	HtreeDerivedConfig `yaml:"-"`
}

type HintConfig struct {
	NoMerged          bool  `yaml:"hint_no_merged,omitempty"`      // merge only used to find collision, but not dump idx.m to save disk space
	MergeInterval     int   `yaml:"hint_merge_interval,omitempty"` // merge after rotating each MergeInterval chunk
	IndexIntervalSize int64 `yaml:"-"`                             // max diff of offsets of two adjacent hint index items
	SplitCap          int64 `yaml:"-"`                             // pre alloc SplitCap slot for each split, when slots are all filled, slot is dumped

	SplitCapStr          string `yaml:"hint_split_cap_str,omitempty"`
	IndexIntervalSizeStr string `yaml:"hint_index_interval_str,omitempty"`
}

// for test
func (c *HStoreConfig) Init() error {
	e := utils.InitSizesPointer(c)
	if e != nil {
		return e
	}
	return c.InitTree()
}

// must be called before use
// NumBucket => TreeDepth => (TreeKeyHashLen & TreeKeyHashMask)
func (c *HStoreConfig) InitTree() error {
	// TreeDepth
	n := c.NumBucket
	c.TreeDepth = 0
	for n > 1 {
		c.TreeDepth += 1
		n /= 16
	}
	// TreeKeyHashLen & TreeKeyHashMask
	c.TreeKeyHashLen = KHASH_LENS[c.TreeDepth+c.TreeHeight-1]
	shift := 64 - uint32(c.TreeKeyHashLen)*8
	c.TreeKeyHashMask = (uint64(0xffffffffffffffff) << shift) >> shift

	return nil
}

func GetBucketDir(numBucket, bucketID int) string {
	if numBucket == 1 {
		return ""
	} else if numBucket == 16 {
		return fmt.Sprintf("%x", bucketID)
	} else if numBucket == 256 {
		return fmt.Sprintf("%x/%x", bucketID/16, bucketID%16)
	}
	panic(fmt.Sprintf("wrong numBucket: %d", numBucket))
}

func GetBucketPath(bucketID int) string {
	return filepath.Join(Conf.Home, GetBucketDir(Conf.NumBucket, bucketID))
}
