package store

import (
	"bufio"
	"cmem"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	MAX_CHUNK_ID  = 255
	MAX_NUM_CHUNK = 256
)

type dataStore struct {
	bucketID int
	home     string

	sync.Mutex
	flushLock sync.Mutex

	oldHead int // old tail == 0
	newHead int
	newTail int

	chunks        [MAX_NUM_CHUNK]dataChunk
	wbufSize      uint32
	lastFlushTime time.Time
}

func NewdataStore(bucketID int, home string) *dataStore {
	ds := new(dataStore)
	ds.bucketID = bucketID
	ds.home = home
	for i := 0; i < MAX_NUM_CHUNK; i++ {
		ds.chunks[i].chunkid = i
		ds.chunks[i].path = genDataPath(ds.home, i)
	}
	return ds
}

func WakeupFlush() {
	select {
	case cmem.DBRL.FlushData.Chan <- 1:
	default:
	}
}

func genDataPath(home string, chunkID int) string {
	return fmt.Sprintf("%s/%03d.data", home, chunkID)
}

func (ds *dataStore) genPath(chunkID int) string {
	return genDataPath(ds.home, chunkID)
}

func (ds *dataStore) nextChunkID(chunkID int) int {
	return chunkID + 1
}

func (ds *dataStore) AppendRecord(rec *Record) (pos Position, err error) {
	// must  CalcValueHash before compress
	rec.TryCompress()

	wrec := wrapRecord(rec)
	ds.Lock()
	size := rec.Payload.RecSize
	currOffset := ds.chunks[ds.newHead].writingHead
	if currOffset+size > uint32(conf.DataFileMax) {
		ds.newHead++
		logger.Infof("rotate to %d, size %d, new rec size %d", ds.newHead, currOffset, size)
		currOffset = 0
		go ds.flush(ds.newHead-1, true)
	}
	pos.ChunkID = ds.newHead
	pos.Offset = currOffset
	wrec.pos = pos
	ds.chunks[ds.newHead].AppendRecord(wrec)
	ds.wbufSize += size
	if wrec.rec.Payload.Ver > 0 {
		cmem.DBRL.FlushData.AddSize(rec.Payload.AccountingSize)
	}
	if cmem.DBRL.FlushData.Size > int64(conf.FlushWake) {
		WakeupFlush()
	}
	ds.Unlock()
	return
}

func (ds *dataStore) flush(chunk int, force bool) error {
	if ds.wbufSize == 0 {
		return nil
	}
	ds.flushLock.Lock()
	defer ds.flushLock.Unlock()
	ds.Lock()
	if ds.wbufSize == 0 {
		ds.Unlock()
		return nil
	}
	if !force && (time.Since(ds.lastFlushTime) < time.Duration(conf.FlushInterval)*time.Second) && (ds.wbufSize < (1 << 20)) {
		ds.Unlock()
		return nil
	}

	if chunk < 0 {
		chunk = ds.newHead
	}
	ds.lastFlushTime = time.Now()
	ds.Unlock()
	// logger.Infof("flushing %d records to data %d", n, chunk)

	w, err := ds.GetStreamWriter(chunk, true)
	if err != nil {
		logger.Fatalf("fail to open data file to flush, stop! err: %v", err)
		return err
	}

	filessize := ds.chunks[chunk].getDiskFileSize()
	if w.offset != filessize {
		logger.Fatalf("wrong data file size, exp %d, got %d, %s", filessize, w.offset, ds.genPath(chunk))
	}
	nflushed, err := ds.chunks[chunk].flush(w, false)
	ds.wbufSize -= nflushed
	w.Close()

	return nil
}

func (ds *dataStore) GetRecordByPos(pos Position) (res *Record, inbuffer bool, err error) {
	return ds.chunks[pos.ChunkID].GetRecordByOffset(pos.Offset)
}

func (ds *dataStore) ListFiles() (max int, err error) {
	max = -1
	for i := 0; i < MAX_NUM_CHUNK; i++ {
		path := genDataPath(ds.home, i)
		st, e := os.Stat(path)
		if e != nil {
			pe := e.(*os.PathError)
			if "no such file or directory" == pe.Err.Error() {
				ds.chunks[i].size = 0
			} else {
				logger.Errorf(pe.Err.Error())
				err = pe
				return
			}
		} else {
			sz := uint32(st.Size())
			if (sz & 0xff) != 0 {
				err = fmt.Errorf("file not 256 aligned, size 0x%x: %s ", sz, path)
				return
			}
			ds.chunks[i].size = sz
			max = i
		}
	}
	ds.newHead = max + 1
	return
}

func (ds *dataStore) GetStreamReader(chunk int) (*DataStreamReader, error) {
	path := ds.genPath(chunk)
	return newDataStreamReader(path, 1<<20)
}

func GetStreamWriter(path string, isappend bool) (*DataStreamWriter, error) {
	offset := uint32(0)

	var fd *os.File
	if stat, err := os.Stat(path); err == nil { // TODO: avoid stat
		fd, err = os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			logger.Infof(err.Error())
			return nil, err
		}
		if isappend {
			offset = uint32(stat.Size())
			offset, err := fd.Seek(0, os.SEEK_END)
			if err != nil {
				logger.Infof(err.Error())
				return nil, err
			} else if offset%PADDING != 0 {
				logger.Fatalf("%s not 256 aligned : %d", path, offset)
			}
		}
	} else {
		logger.Infof("create data file: %s", path)
		fd, err = os.Create(path)
		if err != nil {
			logger.Fatalf(err.Error())
			return nil, err
		}
	}
	wbuf := bufio.NewWriterSize(fd, 1<<20)
	w := &DataStreamWriter{path: path, fd: fd, wbuf: wbuf, offset: offset}
	return w, nil
}

func (ds *dataStore) GetStreamWriter(chunk int, isappend bool) (*DataStreamWriter, error) {
	path := ds.genPath(chunk)
	return GetStreamWriter(path, isappend)

}
