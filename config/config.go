package config

import (
	"fmt"
	"io/ioutil"
	"log"

	yaml "gopkg.in/yaml.v2"

	"github.intra.douban.com/coresys/gobeansdb/utils"
)

// `Version` can be changed in gobeansproxy.
var Version = "2.1.0.13"

const AccessLogVersion = "V1"

var (
	ServerConf  ServerConfig = DefaultServerConfig
	Route       RouteTable
	MCConf      MCConfig = DefaultMCConfig
	AllowReload bool
)

func init() {
	utils.InitSizesPointer(&MCConf)
	for i := 0; i < DefaultRouteConfig.NumBucket; i++ {
		DefaultRouteConfig.BucketsStat[i] = 1
		DefaultRouteConfig.BucketsHex = append(DefaultRouteConfig.BucketsHex, BucketIDHex(i, DefaultRouteConfig.NumBucket))
	}
}

func LoadYamlConfig(config interface{}, path string) error {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(content, config)
}

func DumpConfig(config interface{}) {
	b, err := yaml.Marshal(config)
	if err != nil {
		log.Fatalf("%s", err)
	} else {
		fmt.Println(string(b))
	}
}

func BucketIDHex(id, numBucket int) string {
	if numBucket == 16 {
		return fmt.Sprintf("%x", id)
	} else if numBucket == 256 {
		return fmt.Sprintf("%02x", id)
	}
	return "0"
}
