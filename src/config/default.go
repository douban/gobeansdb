package config

var (
	DefaultMCConfig = MCConfig{
		MaxReq:     16,
		MaxKeyLen:  250,
		BodyMaxStr: "50M",
		BodyBigStr: "1M",
		BodyInCStr: "4K",
	}

	DefaultServerConfig = ServerConfig{
		Hostname: "127.0.0.1",
		Listen:   "0.0.0.0",
		Port:     7900,
		WebPort:  7908,
		Threads:  4,
		ZK:       "NO",
		LogDir:   "./",
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
		FlushInterval:  5, // 5s
		FlushWakeStr:   "0",
		FlushMaxStr:    "100M",
	}

	DefaultDBLocalConfig = DBLocalConfig{
		Homes: []string{"./testdb"},
	}

	//DefaultRouteConfig = route.RouteConfig{NumBucket: 256, Buckets: make([]int, 256)}
	DefaultRouteConfig = DBRouteConfig{NumBucket: 16, Buckets: make([]int, 16)}
)

func init() {
	for i := 0; i < DefaultRouteConfig.NumBucket; i++ {
		DefaultRouteConfig.Buckets[i] = 1
	}
}

func (config *HStoreConfig) InitDefault() {
	config.HintConfig = DefaultHintConfig
	config.HTreeConfig = DefaultHTreeConfig
	config.DataConfig = DefaultDataConfig
	config.DBLocalConfig = DefaultDBLocalConfig
	config.DBRouteConfig = DefaultRouteConfig
}
