package store

import "github.intra.douban.com/coresys/gobeansdb/config"

/*

1. easy to start:
	Homes: []string{"./testdb"},
	TreeHeight: 3
	Hostname: "127.0.0.1"
	serve all buckets

2. easy for test:
	flush at once
		FlushInterval: 0,
		FlushWakeStr: "0"
	CheckVHash: false
	NoMerged: false
	MergeInterval: 1

3. reasonabe setting for others

*/

var (
	DefaultHintConfig = HintConfig{
		NoMerged:             false,
		SplitCapStr:          "1M",
		IndexIntervalSizeStr: "4K",
		MergeInterval:        1,
	}

	DefaultHTreeConfig HTreeConfig = HTreeConfig{
		TreeHeight: 3,
		TreeDump:   3,
	}

	DefaultDataConfig = DataConfig{
		DataFileMaxStr: "4000M",
		CheckVHash:     false,
		FlushInterval:  0,
		FlushWakeStr:   "0",

		NoGCDays: 0,
	}

	DefaultDBLocalConfig = DBLocalConfig{
		Home: "./testdb",
	}
)

func (c *HStoreConfig) InitDefault() {
	c.HintConfig = DefaultHintConfig
	c.HTreeConfig = DefaultHTreeConfig
	c.DataConfig = DefaultDataConfig
	c.DBLocalConfig = DefaultDBLocalConfig
	c.DBRouteConfig = config.DefaultRouteConfig
}
