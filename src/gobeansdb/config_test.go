package main

import (
	"bytes"
	"io/ioutil"
	"testing"

	yaml "gopkg.in/yaml.v2"
)

func TestConfig(t *testing.T) {
	path := "config_test.yaml"
	data1, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal("read config failed", path, err.Error())
	}
	var c1 GoBeansdbConfig
	c1.Port = 1
	c1.WebPort = 1
	err = yaml.Unmarshal([]byte(data1), &c1)
	if err != nil {
		t.Fatalf("unmarshal err:%v", err)
	}
	if c1.HStoreConfig.MaxKeySize != 200 || c1.WebPort != 1 {
		t.Fatalf("\nc1:\n%#v\n", c1)
	}
	c1.WebPort = 0

	data2, e := yaml.Marshal(c1)
	if e != nil {
		t.Fatalf("err:%s", e.Error())
	}
	if 0 != bytes.Compare(data1, data2) {
		tmp := path + ".tmp"
		ioutil.WriteFile(tmp, data2, 0644)
		t.Fatalf("diff %s %s", path, tmp)
	}
}
