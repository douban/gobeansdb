package store

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
)

func randomValue(size int) []byte {
	b := make([]byte, size)
	for i := 0; i < size; i++ {
		b[i] = byte(rand.Intn(256))
	}
	return b
}

func checkFileSize(t *testing.T, chunkID int, size uint32) {
	path := genPath(config.Homes[0], chunkID)
	stat, err := os.Stat(path)
	if size == 0 {
		if err == nil {
			t.Fatalf("%s should not exist, size = %d", path, stat.Size())
		}
	} else {
		if err != nil {
			t.Fatalf("%s", err.Error())
		} else if stat.Size() != int64(size) {
			t.Fatalf("bad file size chunk %d, path %s, %d != %d", chunkID, path, stat.Size(), size)
		}
	}
}

func TestDataSameKeyValue1(t *testing.T) {
	testDataSameKeyValue(t, 1, []byte("key"), []byte("value"), 1)
}
func TestDataSameKeyValue2(t *testing.T) {
	testDataSameKeyValue(t, 2, []byte("key"), []byte(strings.Repeat("v", 255)), 1)
}

func TestDataSameKeyValue3(t *testing.T) {
	testDataSameKeyValue(t, 3, []byte("key"), randomValue(400), 2)
}

func TestDataSameKeyValue4(t *testing.T) {
	testDataSameKeyValue(t, 4, []byte(strings.Repeat("k", 200)), randomValue(512), 3)
}

func TestDataCompatibility(t *testing.T) {
	if *tDataFile == "" {
		t.Logf("no data file provided, pass")
		return
	}
	rd, err := newDataStreamReader(*tDataFile, 1<<20)
	if err != nil {
		t.Fatalf("%s not exist", *tDataFile)
	}
	count := 0
	for {
		r, _, sizeBroken, err := rd.Next()
		if err != nil {
			t.Fatalf(err.Error())
		}
		if r == nil {
			break
		}
		if err := r.Payload.Decompress(); err != nil {
			t.Fatalf("broken datafile")
		}
		if r.Payload.Value == nil {
			t.Fatal("nil value")
		}

		if sizeBroken != 0 {
			t.Fatalf("broken datafile")
		}

		if *tDumpRecord {
			logger.Infof("%s", r.LogString())
		}
		count++

	}
	t.Logf("record count = %d", count)
}

func testDataSameKeyValue(t *testing.T, seq int, key, value []byte, recsize uint32) {
	initDefaultConfig()
	setupTest(fmt.Sprintf("TestDataSameKeyValue_%d", seq), 1)
	defer clearTest()
	config.Init()
	dataConfig.MaxFileSize = 256 * uint32(recordPerFile) * recsize

	p := &Payload{Value: value}
	ds := NewdataStore(config.Homes[0])

	for i := 0; i < recordPerFile+1; i++ {
		p.Ver = int32(i)
		r := &Record{key, p}
		pos, err := ds.AppendRecord(r)
		// TODO: check pos
		ds.flush(-1, true)
		r = nil
		r, err = ds.GetRecordByPos(pos)
		if err != nil {
			log.Fatal(err.Error())
		}
		if r.Payload.Ver != int32(i) {
			t.Fatalf("%d %#v", i, r.Payload)
		}
	}
	checkFileSize(t, 0, dataConfig.MaxFileSize)
	checkFileSize(t, 1, 256*recsize)
}
