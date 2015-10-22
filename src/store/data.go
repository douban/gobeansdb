package store

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	MAX_CHUNK = 256
)

type dataStore struct {
	home string

	sync.Mutex

	oldHead int // old tail == 0
	newHead int
	newTail int

	currOffset uint32

	filesizes [MAX_CHUNK]uint32
	wbufs     [MAX_CHUNK][]*WriteRecord
	wbufSize  uint32
}

func NewdataStore(home string) *dataStore {
	ds := new(dataStore)
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
	_, size := rec.Sizes()
	ds.Lock()
	if ds.currOffset+size > dataConfig.MaxFileSize {
		ds.newHead++
		ds.currOffset = 0
		logger.Infof("rotate to %d (%d %d %#v)", ds.newHead, ds.currOffset, size, config)
		go ds.flush(ds.newHead - 1)
	}
	pos.ChunkID = ds.newHead
	pos.Offset = ds.currOffset
	wrec.pos = pos
	ds.wbufs[ds.newHead] = append(ds.wbufs[ds.newHead], wrec)
	ds.currOffset += size
	ds.Unlock()
	return
}

func (ds *dataStore) flusher() {
	for {
		ds.flush(-1)
		time.Sleep(1 * time.Minute)
	}
}

func (ds *dataStore) flush(chunk int) error {
	// TODO: need a flush lock
	ds.Lock()
	if chunk < 0 {
		chunk = ds.newHead
	}
	n := len(ds.wbufs[chunk])
	ds.Unlock()
	// logger.Infof("flushing %d records to data %d", n, chunk)

	w, err := ds.getStreamWriter(chunk, true)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		ds.Lock()
		r := ds.wbufs[chunk][i]
		ds.Unlock()
		w.append(r)
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
	return os.Truncate(ds.genPath(chunk), int64(size))
}

func (ds *dataStore) ListFiles() ([]Position, error) {
	res := make([]Position, 0, 256)
	for i := 0; i < 256; i++ {
		path := genPath(ds.home, i)
		st, err := os.Stat(path)
		if err != nil {
			logger.Infof(err.Error())
			return nil, err // TODO: need another way to check
		} else {
			res = append(res, Position{i, uint32(st.Size())})
		}
	}
	return res, nil
}

func (ds *dataStore) GetCurrPos() Position {
	return Position{ds.newHead, ds.currOffset}
}

func (ds *dataStore) DeleteFile(chunkID int) error {
	return os.Remove(ds.genPath(chunkID))
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
			} else if offset%256 != 0 {
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
