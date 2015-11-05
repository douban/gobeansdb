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

	filesizes     [MAX_NUM_CHUNK]uint32
	wbufs         [MAX_NUM_CHUNK][]*WriteRecord
	wbufSize      uint32
	lastFlushTime time.Time
}

func NewdataStore(bucketID int, home string) *dataStore {
	ds := new(dataStore)
	ds.bucketID = bucketID
	ds.home = home

	return ds
}

func genPath(home string, chunkID int) string {
	if chunkID > config.maxOldChunk {
		return fmt.Sprintf("%s/%03d.data", home, chunkID)
	}
	// chunkID -= dataConfig.maxOldChunk
	return fmt.Sprintf("%s/%03d.data", home, chunkID)
}

func (ds *dataStore) genPath(chunkID int) string {

	return genPath(ds.home, chunkID)
}

func (ds *dataStore) nextChunkID(chunkID int) int {
	if chunkID < 255 {
		return chunkID + 1
	} else {
		return config.maxOldChunk
	}
}

func (ds *dataStore) AppendRecord(rec *Record) (pos Position, err error) {
	// TODO: check err of last flush

	// must  CalcValueHash before compress
	rec.Compress()
	wrec := wrapRecord(rec)
	ds.Lock()
	size := rec.Payload.RecSize
	currOffset := ds.filesizes[ds.newHead]
	if currOffset+size > uint32(dataConfig.MaxFileSize) {
		ds.newHead++
		logger.Infof("rotate to %d, size %d, new rec size %d", ds.newHead, currOffset, size)
		currOffset = 0
		go ds.flush(ds.newHead-1, true)
	}
	pos.ChunkID = ds.newHead
	pos.Offset = currOffset
	wrec.pos = pos
	ds.wbufs[ds.newHead] = append(ds.wbufs[ds.newHead], wrec)
	ds.filesizes[ds.newHead] += size
	ds.wbufSize += size
	if rec.Payload.Ver > 0 {
		cmem.Sub(cmem.TagSetData, int(wrec.vsz))
	}
	cmem.Add(cmem.TagFlushData, int(size))
	if cmem.AllocedSize[cmem.TagFlushData] > int64(dataConfig.FlushSize) {
		select {
		case cmem.Chans[cmem.TagFlushData] <- 1:
		default:
		}
	}
	ds.Unlock()
	return
}

// this is for test, use HStore.flusher instead,
func (ds *dataStore) flusher() {
	for {
		ds.flush(-1, false)
		time.Sleep(1 * time.Minute)
	}
}

func (ds *dataStore) flush(chunk int, force bool) error {
	if ds.wbufSize == 0 {
		return nil
	}
	ds.flushLock.Lock()
	defer ds.flushLock.Unlock()
	ds.Lock()
	if !force && (time.Since(ds.lastFlushTime) < time.Duration(dataConfig.DataFlushSec)*time.Second) && (ds.wbufSize < (1 << 20)) {
		ds.Unlock()
		return nil
	}

	if chunk < 0 {
		chunk = ds.newHead
	}
	ds.lastFlushTime = time.Now()
	n := len(ds.wbufs[chunk])
	ds.Unlock()
	// logger.Infof("flushing %d records to data %d", n, chunk)

	w, err := ds.getStreamWriter(chunk, true)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		ds.Lock()
		wrec := ds.wbufs[chunk][i]
		ds.Unlock()
		w.append(wrec)
		size := wrec.rec.Payload.RecSize
		ds.wbufSize -= size
		cmem.Sub(cmem.TagFlushData, int(size))
	}
	ds.Lock()
	ds.wbufs[chunk] = ds.wbufs[chunk][n:]
	ds.Unlock()

	return w.Close()
}

func (ds *dataStore) GetRecordByPos(pos Position) (res *Record, err error) {
	res = nil
	ds.Lock()
	wbuf := ds.wbufs[pos.ChunkID]
	n := len(wbuf)
	if n > 0 && pos.Offset >= wbuf[0].pos.Offset {
		for _, wrec := range wbuf {
			if wrec.pos.Offset == pos.Offset {
				// TODO: otherwise may need ref count to release []byte?
				res = wrec.rec.Copy()
			}
		}
	}
	ds.Unlock()
	if res != nil {
		return
	}
	wrec, e := readRecordAtPath(ds.genPath(pos.ChunkID), pos.Offset)
	if e != nil {
		return nil, e
	}
	wrec.rec.Payload.Decompress()
	return wrec.rec, nil
}

func (ds *dataStore) Truncate(chunk int, size uint32) error {
	path := ds.genPath(chunk)
	st, err := os.Stat(path)
	if err != nil {
		logger.Infof(err.Error())
		return err
	}
	logger.Infof("truncate %s %d to %d", path, st.Size(), size)
	if size == 0 {
		return os.Remove(path)
	}
	return os.Truncate(path, int64(size))
}

func (ds *dataStore) ListFiles() (max int, err error) {
	max = -1
	for i := 0; i < MAX_NUM_CHUNK; i++ {
		path := genPath(ds.home, i)
		st, e := os.Stat(path)
		if e != nil {
			pe := e.(*os.PathError)
			if "no such file or directory" == pe.Err.Error() {
				ds.filesizes[i] = 0
			} else {
				logger.Errorf(pe.Err.Error())
				err = pe
				return
			}
		} else {
			ds.filesizes[i] = uint32(st.Size())
			max = i
		}
	}
	ds.newHead = max + 1
	return
}

func (ds *dataStore) GetCurrPos() Position {
	return Position{ds.newHead, ds.filesizes[ds.newHead]}
}

func (ds *dataStore) DeleteFile(chunkID int) error {
	path := ds.genPath(chunkID)
	logger.Infof("remove data %s", path)
	return os.Remove(path)
}

func (ds *dataStore) GetStreamReader(chunk int) (*DataStreamReader, error) {
	path := ds.genPath(chunk)
	return newDataStreamReader(path, 1<<20)
}

func (ds *dataStore) GetStreamWriter(chunk int, isappend bool) (*DataStreamWriter, error) {
	return ds.getStreamWriter(chunk, isappend)
}

func (ds *dataStore) getStreamWriter(chunk int, isappend bool) (*DataStreamWriter, error) {
	offset := uint32(0)

	path := ds.genPath(chunk)
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
				logger.Infof("%s not 256 aligned : %d", path, offset)
			}
		}
	} else {
		logger.Infof("create data file: %s", path)
		fd, err = os.Create(path)
		if err != nil {
			logger.Infof(err.Error())
			return nil, err
		}
	}
	wbuf := bufio.NewWriterSize(fd, 1<<20)
	w := &DataStreamWriter{ds: ds, fd: fd, wbuf: wbuf, offset: offset}
	return w, nil
}
