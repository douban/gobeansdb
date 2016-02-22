package store

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func randomValue(size int) []byte {
	b := make([]byte, size)
	for i := 0; i < size; i++ {
		b[i] = byte(rand.Intn(256))
	}
	return b
}

func checkFileSize(t *testing.T, chunkID int, size uint32) {
	path := genDataPath(Conf.Homes[0], chunkID)
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
		if r.Payload.Body == nil {
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
	Conf.InitDefault()
	setupTest(fmt.Sprintf("TestDataSameKeyValue_%d", seq), 1)
	defer clearTest()

	Conf.DataFileMaxStr = strconv.Itoa(int(256 * uint32(recordPerFile) * recsize))
	Conf.Init()

	ds := NewdataStore(0, Conf.Homes[0])

	for i := 0; i < recordPerFile+1; i++ {
		p := &Payload{}
		p.Body = value
		p.Ver = int32(i)
		r := &Record{key, p}
		pos, err := ds.AppendRecord(r)
		// TODO: check pos
		ds.flush(-1, true)
		r = nil
		r, _, err = ds.GetRecordByPos(pos)
		if err != nil {
			log.Fatal(err.Error())
		}
		if r.Payload.Ver != int32(i) {
			t.Fatalf("%d %#v", i, r.Payload)
		}
	}
	checkFileSize(t, 0, uint32(Conf.DataFileMax))
	checkFileSize(t, 1, 256*recsize)
	time.Sleep(time.Second)
}

func breakdata(f *os.File, start, offset int) {
	f.Seek(int64(start*256+offset), os.SEEK_SET)
	b := []byte("0")
	f.Write(b)
}

func TestDataBroken(t *testing.T) {
	Conf.InitDefault()
	setupTest("TestDataBroken", 1)
	defer clearTest()

	//Conf.DataFileMaxStr = strconv.Itoa(int(256 * uint32(recordPerFile) * recsize))
	Conf.Init()

	ds := NewdataStore(0, Conf.Homes[0])

	key := []byte("key")
	for i := 0; i < 7; i++ {
		p := &Payload{}
		if i == 4 {
			p.Flag = FLAG_CLIENT_COMPRESS
			p.Body = []byte(strings.Repeat("x", 256*3))
		} else {
			p.Body = []byte(fmt.Sprintf("value_%d", i))
		}

		p.Ver = int32(i)
		r := &Record{key, p}
		ds.AppendRecord(r)
	}
	ds.flush(-1, true)

	path := genDataPath(Conf.Homes[0], 0)
	fd, err := os.OpenFile(path, os.O_WRONLY, 0664)
	if err != nil {
		t.Fatalf(err.Error())
	}

	breakdata(fd, 0, 16)   // ksz
	breakdata(fd, 1, 20)   // vsz
	breakdata(fd, 2, 24)   // key
	breakdata(fd, 3, 24+3) // value
	breakdata(fd, 4, 256)  // value
	fd.Close()

	reader, _ := ds.GetStreamReader(0)
	rec, offset, sizeBroken, err := reader.Next()
	if err != nil || offset != 8*256 || sizeBroken != 8*256 ||
		rec == nil || string(rec.Payload.Body) != fmt.Sprintf("value_%d", 5) {
		if rec != nil {
			t.Errorf("%s %s", rec.Key, string(rec.Payload.Body))
		}
		t.Fatalf("%d %d %d, %v", reader.offset, offset, sizeBroken, err)
	}
	rec, offset, sizeBroken, err = reader.Next()
	if err != nil || offset != 9*256 || sizeBroken != 0 ||
		rec == nil || string(rec.Payload.Body) != fmt.Sprintf("value_%d", 6) {
		if rec != nil {
			t.Errorf("%s %s", rec.Key, string(rec.Payload.Body))
		}
		t.Fatalf("%d %d %d, %v", reader.offset, offset, sizeBroken, err)
	}
	reader.Close()

}
