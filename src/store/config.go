package store

import (
	"route"
	"utils"
)

var (
	config HStoreConfig
)

var (
	DefaultHintConfig = HintConfig{
		NotDumpMerged:        false,
		SplitCountStr:        "1k",
		IndexIntervalSizeStr: "4K",
		SecondsBeforeDump:    60,
	}

	DefaultHTreeConfig HTreeConfig = HTreeConfig{
		ThresholdListKey: 64 * 4,
		ThresholdBigHash: 64 * 4,
		TreeHeight:       3,
	}
	DefaultDataConfig = DataConfig{
		MaxKeySize:      250,
		MaxValueSizeStr: "50M",
		MaxFileSizeStr:  "4000M",
		DataFlushSec:    5, // 5s
		FlushSizeStr:    "4M",
		FlushSizeMin:    256,
	}

	DefaultLocalConfig = LocalConfig{
		Hostname:    "127.0.0.1",
		Homes:       []string{"./test"},
		maxOldChunk: 255,
	}
	DefaultRouteConfig = route.RouteConfig{NumBucket: 16, Buckets: make([]int, 16)}

	htreeConfig = &config.HTreeConfig
	hintConfig  = &config.HintConfig
	dataConfig  = &config.DataConfig
)

func SetConfig(c HStoreConfig) {
	config = c
}

func GetConfig() HStoreConfig {
	return config
}

func InitDefaultGlobalConfig() {
	config = HStoreConfig{}
	config.HintConfig = DefaultHintConfig
	config.HTreeConfig = DefaultHTreeConfig
	config.DataConfig = DefaultDataConfig
}

func InitDefaultConfig() {
	InitDefaultGlobalConfig()
	config.LocalConfig = DefaultLocalConfig
	config.RouteConfig = DefaultRouteConfig
	for i := 0; i < 16; i++ {
		DefaultRouteConfig.Buckets[i] = 1
	}
}

type HStoreConfig struct {
	route.RouteConfig `yaml:"-"`
	LocalConfig       `yaml:"local,omitempty"`
	GlobalConfig      `yaml:"global,omitempty"`
}

type GlobalConfig struct {
	DataConfig  `yaml:"data,omitempty"`
	HintConfig  `yaml:"hint,omitempty"`
	HTreeConfig `yaml:"htree,omitempty"`
}

type HtreeDerivedConfig struct {
	TreeDepth       int // from NumBucket
	TreeKeyHashMask uint64
	TreeKeyHashLen  int
}

type LocalConfig struct {
	Hostname    string   `yaml:",omitempty"`
	Homes       []string `yaml:",omitempty"`
	maxOldChunk int      `yaml:",omitempty"`
}

type DataConfig struct {
	MaxKeySize   int   `yaml:",omitempty"`
	MaxFileSize  int64 `yaml:"-"`
	MaxValueSize int64 `yaml:"-"`
	DataFlushSec int   `yaml:",omitempty"`
	FlushSize    int64 `yaml:"-"`
	FlushSizeMin int   `yaml:",omitempty"`

	// sizes

	MaxValueSizeStr string `yaml:",omitempty"`
	MaxFileSizeStr  string `yaml:",omitempty"`
	FlushSizeStr    string `yaml:",omitempty"`
}

type HTreeConfig struct {
	ThresholdListKey   uint32 `yaml:",omitempty"`
	ThresholdBigHash   uint32 `yaml:",omitempty"`
	TreeHeight         int    `yaml:",omitempty"`
	HtreeDerivedConfig `yaml:"-"`
}

type HintConfig struct {
	NotDumpMerged     bool  `yaml:",omitempty"`
	SplitCount        int64 `yaml:",omitempty"`
	IndexIntervalSize int64 `yaml:",omitempty"`
	SecondsBeforeDump int64 `yaml:",omitempty"`

	// sizes
	SplitCountStr        string `yaml:",omitempty"`
	IndexIntervalSizeStr string `yaml:",omitempty"`
}

func (c *HStoreConfig) InitForYaml() (err error) {
	utils.LastSizeErr = nil
	c.MaxValueSize = utils.StrToSize(c.MaxValueSizeStr)
	c.MaxFileSize = utils.StrToSize(c.MaxFileSizeStr)
	c.IndexIntervalSize = utils.StrToSize(c.IndexIntervalSizeStr)
	c.FlushSize = utils.StrToSize(c.FlushSizeStr)
	c.SplitCount = utils.StrToSize(c.SplitCountStr)
	err = utils.LastSizeErr
	utils.LastSizeErr = nil
	return
}

// must be called before use
func (c *HStoreConfig) Init() error {

	e := c.InitForYaml()
	if e != nil {
		return e
	}
	// TreeDepth
	n := c.NumBucket
	c.TreeDepth = 0
	for n > 1 {
		c.TreeDepth += 1
		n /= 16
	}

	// TreeHeight
	if c.TreeHeight == 0 {
		c.TreeHeight = 8 - c.TreeDepth
	}

	c.TreeKeyHashLen = KHASH_LENS[c.TreeDepth+c.TreeHeight-1]
	shift := 64 - uint32(c.TreeKeyHashLen)*8
	c.TreeKeyHashMask = (uint64(0xffffffffffffffff) << shift) >> shift
	return nil
}
