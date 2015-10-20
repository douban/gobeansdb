package store

import (
	"route"
)

var (
	config HStoreConfig
)

var (
	defaultHintConfig = HintConfig{
		SplitCount:        1 << 20,
		IndexIntervalSize: 4 << 10,
		SecondsBeforeDump: 60,
	}

	defaultHTreeConfig HTreeConfig = HTreeConfig{
		ThresholdListKey: 64 * 4,
		ThresholdBigHash: 64 * 4,
		TreeHeight:       0,
	}
	defaultDataConfig = DataConfig{
		MaxKeySize:   250,
		MaxValueSize: 50 << 20,
		MaxFileSize:  4000 << 20,
		FlushPeriod:  5, // 5s
		FlushSize:    4 << 20,
	}

	htreeConfig = &config.HTreeConfig
	hintConfig  = &config.HintConfig
	dataConfig  = &config.DataConfig
)

func SetConfig(c HStoreConfig) {
	config = c
}

func initDefaultConfig() {
	config = HStoreConfig{}
	config.HintConfig = defaultHintConfig
	config.HTreeConfig = defaultHTreeConfig
	config.DataConfig = defaultDataConfig
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
	maxChunk    int      `yaml:",omitempty"` // may <= 255, for test
}

type DataConfigYaml struct {
	MaxValueSizeMB int    `yaml:",omitempty"`
	MaxFileSizeMB  uint32 `yaml:",omitempty"`
	FlushSizeMB    int    `yaml:",omitempty"`
}

type DataConfig struct {
	MaxKeySize     int    `yaml:",omitempty"`
	MaxFileSize    uint32 `yaml:"-"`
	MaxValueSize   int    `yaml:"-"`
	FlushPeriod    int    `yaml:",omitempty"`
	FlushSize      int    `yaml:"-"`
	DataConfigYaml `yaml:",inline"`
}

type HTreeConfig struct {
	ThresholdListKey   uint32 `yaml:",omitempty"`
	ThresholdBigHash   uint32 `yaml:",omitempty"`
	TreeHeight         int    `yaml:",omitempty"`
	HtreeDerivedConfig `yaml:"-"`
}

type HintConfigYaml struct {
	SplitCountKB        int `yaml:",omitempty"`
	IndexIntervalSizeKB int `yaml:",omitempty"`
}

type HintConfig struct {
	SplitCount        int   `yaml:",omitempty"`
	IndexIntervalSize int   `yaml:",omitempty"`
	SecondsBeforeDump int64 `yaml:",omitempty"`
	HintConfigYaml    `yaml:",inline"`
}

func (c *HStoreConfig) InitForYaml() {
	c.MaxValueSize = c.MaxValueSizeMB << 20
	c.MaxFileSize = c.MaxFileSizeMB << 20
	c.IndexIntervalSize = c.IndexIntervalSizeKB << 10
	c.FlushSize = c.FlushSizeMB << 20
	c.SplitCount = c.SplitCountKB << 10
}

// must be called before use
func (c *HStoreConfig) Init() {
	// TreeDepth
	n := c.NumBucket
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
}
