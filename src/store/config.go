package store

import (
	"route"
)

var (
	config HStoreConfig
)

var (
	defaultHintConfig = HintConfig{
		SplitCount:         1 << 20,
		SplitSize:          50 * (1 << 20), // 50M
		IndexIntervalBytes: (4 << 10),      // 4KB
		SecondsBeforeDump:  60,
	}

	defaultHTreeConfig HTreeConfig = HTreeConfig{
		ThresholdListKey: 64 * 4,
		ThresholdBigHash: 64 * 4,
		TreeHeight:       -1,
	}
	defaultDataConfig = DataConfig{
		MaxKeySize:   250,
		MaxValueSize: 50 * (1 << 20),   // 50M
		MaxFileSize:  4000 * (1 << 20), // 4000M
		FlushPeriod:  5,                // 5s
		FlushSize:    4 * (1 << 20),    // 4M
	}

	htreeConfig = &config.HTreeConfig
	hintConfig  = &config.HintConfig
	dataConfig  = &config.DataConfig
)

func initDefaultConfig() {
	config.HintConfig = defaultHintConfig
	config.HTreeConfig = defaultHTreeConfig
	config.DataConfig = defaultDataConfig
}

type HStoreConfig struct {
	route.RouteConfig
	LocalConfig
	CommonConfig
	DerivedConfig
}

type CommonConfig struct {
	DataConfig
	HintConfig
	HTreeConfig
}

type DerivedConfig struct {
	TreeDepth int // from NumBucket
}

type LocalConfig struct {
	Homes       []string
	maxOldChunk int
	maxChunk    int // <= 255, for test
}

type DataConfig struct {
	MaxKeySize   int
	MaxValueSize int
	MaxFileSize  uint32
	FlushPeriod  int
	FlushSize    int
}

type HTreeConfig struct {
	ThresholdListKey uint32
	ThresholdBigHash uint32
	TreeHeight       int
}

type HintConfig struct {
	SplitCount         int
	SplitSize          int
	IndexIntervalBytes int
	SecondsBeforeDump  int64
}

func (c *HStoreConfig) init() {
	// TreeDepth
	n := c.NumBucket
	for n > 1 {
		c.TreeDepth += 1
		n /= 16
	}

	// TreeHeight
	if c.TreeHeight == -1 {
		c.TreeHeight = 8 - c.TreeDepth
	}

}
