package config

var (
	DefaultMCConfig = MCConfig{
		MaxReq:      16,
		MaxKeyLen:   250,
		BodyMaxStr:  "50M",
		BodyBigStr:  "1M",
		BodyInCStr:  "4K",
		FlushMaxStr: "100M",
		TimeoutMS: "3000",
	}
)

type MCConfig struct {
	MaxKeyLen int `yaml:"max_key_len,omitempty"`
	MaxReq    int `yaml:"max_req,omitempty"` // max num of requsets serve at the same time

	BodyMax int64 `yaml:"-"` // fail set/read_file if larger then this
	BodyBig int64 `yaml:"-"` // set may fail if memory is in shorage (determine by "storage")
	BodyInC int64 `yaml:"-"` // alloc body in cgo if larger then this

	FlushMax int64 `yaml:"-"` // if flush buffer is larger, may fail BIG set request (return NOT_FOUND)

	FlushMaxStr string `yaml:"flush_max_str"`
	BodyMaxStr  string `yaml:"body_max_str,omitempty"`
	BodyBigStr  string `yaml:"body_big_str,omitempty"`

	BodyInCStr string `yaml:"body_c_str,omitempty"`
	TimeoutMS  int `yaml:"timeout_ms,omitempty`
}

func IsValidKeySize(ksz uint32) bool {
	return ksz != 0 && ksz <= uint32(MCConf.MaxKeyLen)
}

func IsValidValueSize(vsz uint32) bool {
	return vsz <= uint32(MCConf.BodyMax)
}
