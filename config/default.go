package config

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
	DefaultMCConfig = MCConfig{
		MaxReq:     16,
		MaxKeyLen:  250,
		BodyMaxStr: "50M",
		BodyBigStr: "1M",
		BodyInCStr: "4K",
	}

	DefaultServerConfig = ServerConfig{
		Hostname:  "127.0.0.1",
		Listen:    "0.0.0.0",
		Port:      7900,
		WebPort:   7903,
		Threads:   4,
		ZK:        "NO",
		ErrLog:    "./gobeansdb.log",
		AccessLog: "./access.log",
		StaticDir: "./",
	}

	DefaultHintConfig = HintConfig{
		NoMerged:             false,
		SplitCapStr:          "1M",
		IndexIntervalSizeStr: "4K",
		MergeInterval:        1,
	}

	DefaultHTreeConfig HTreeConfig = HTreeConfig{
		TreeHeight: 3,
	}

	DefaultDataConfig = DataConfig{
		DataFileMaxStr: "4000M",
		CheckVHash:     false,
		FlushInterval:  0,
		FlushWakeStr:   "0",
		FlushMaxStr:    "100M",
		NoGCDays:       0,
	}

	DefaultDBLocalConfig = DBLocalConfig{
		Homes: []string{"./testdb"},
	}

	//DefaultRouteConfig = route.RouteConfig{NumBucket: 256, Buckets: make([]int, 256)}
	DefaultRouteConfig = DBRouteConfig{NumBucket: 16, BucketsStat: make([]int, 16)}
)

func init() {
	for i := 0; i < DefaultRouteConfig.NumBucket; i++ {
		DefaultRouteConfig.BucketsStat[i] = 1
		DefaultRouteConfig.BucketsHex = append(DefaultRouteConfig.BucketsHex, BucketIDHex(i, DefaultRouteConfig.NumBucket))
	}
}

func (config *HStoreConfig) InitDefault() {
	config.HintConfig = DefaultHintConfig
	config.HTreeConfig = DefaultHTreeConfig
	config.DataConfig = DefaultDataConfig
	config.DBLocalConfig = DefaultDBLocalConfig
	config.DBRouteConfig = DefaultRouteConfig
}
