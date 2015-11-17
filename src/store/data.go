package store

import (
	"bufio"
	"cmem"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
	"utils"
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
	return fmt.Sprintf("%s/%03d.data", home, chunkID)
}

func (ds *dataStore) genPath(chunkID int) string {

	return genPath(ds.home, chunkID)
}

func (ds *dataStore) nextChunkID(chunkID int) int {
	return chunkID + 1
}

func WakeupFlush() {
	select {
	case cmem.DBRL.FlushData.Chan <- 1:
	default:
	}
}

func (ds *dataStore) AppendRecord(rec *Record) (pos Position, err error) {
	// TODO: check err of last flush

	// must  CalcValueHash before compress
	rec.TryCompress()

	wrec := wrapRecord(rec)
	ds.Lock()
	size := rec.Payload.RecSize
	currOffset := ds.filesizes[ds.newHead]
	if currOffset+size > uint32(conf.DataFileMax) {
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
		cmem.DBRL.FlushData.AddSize(rec.Payload.AccountingSize)
	}
	if cmem.DBRL.FlushData.Size > int64(conf.FlushWake) {
		WakeupFlush()
	}
	ds.Unlock()
	return
}

func (ds *dataStore) getDiskFileSize(chunkID int) uint32 {
	if len(ds.wbufs[chunkID]) > 0 {
		return ds.wbufs[chunkID][0].pos.Offset
	}
	return ds.filesizes[chunkID]
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

	w, err := ds.getStreamWriter(chunk, true)
	if err != nil {
		return err
	}
	filessize := ds.getDiskFileSize(chunk)
	if w.offset != filessize {
		logger.Fatalf("wrong data file size, exp %d, got %d, %s", filessize, w.offset, ds.genPath(chunk))
	}
	ds.Lock()
	n := len(ds.wbufs[chunk])
	ds.Unlock()
	for i := 0; i < n; i++ {
		ds.Lock() // because append may change the slice
		wrec := ds.wbufs[chunk][i]
		ds.Unlock()
		w.append(wrec)
		size := wrec.rec.Payload.RecSize
		ds.wbufSize -= size
		if wrec.rec.Payload.Ver > 0 {
			cmem.DBRL.FlushData.SubSize(wrec.rec.Payload.AccountingSize)
			// NOTE: not freed yet, make it a little diff with AllocRL, which may provide more insight
		}
	}
	if err = w.Close(); err != nil {
		logger.Fatalf("write data fail, stop! err: %s", err.Error())
		return err
	}

	ds.Lock()
	tofree := ds.wbufs[chunk][:n]
	ds.wbufs[chunk] = ds.wbufs[chunk][n:]
	ds.Unlock()
	for _, wrec := range tofree {
		wrec.rec.Payload.Free()
	}
	return nil
}

func (ds *dataStore) GetRecordByPosInBuffer(pos Position) (res *Record, err error) {
	ds.Lock()
	defer ds.Unlock()

	wbuf := ds.wbufs[pos.ChunkID]
	n := len(wbuf)
	if n == 0 || wbuf[0].pos.Offset > pos.Offset {
		return
	}

	idx := sort.Search(n, func(i int) bool { return wbuf[i].pos.Offset >= pos.Offset })
	wrec := wbuf[idx]
	if wrec.pos.Offset == pos.Offset {
		cmem.DBRL.GetData.AddSize(wrec.rec.Payload.AccountingSize)
		res = wrec.rec.Copy()
		return
	} else {
		err = fmt.Errorf("rec should in buffer, but not, pos = %#v", pos)
		return
	}

	return
}

func (ds *dataStore) GetRecordByPos(pos Position) (res *Record, err error) {
	res, err = ds.GetRecordByPosInBuffer(pos)
	if err != nil {
		return
	}
	if res != nil {
		res.Payload.Decompress()
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
		return utils.Remove(path)
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
			sz := uint32(st.Size())
			if (sz & 0xff) != 0 {
				err = fmt.Errorf("file not 256 aligned, size 0x%x: %s ", sz, path)
				return
			}
			ds.filesizes[i] = sz
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
	return utils.Remove(path)
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
	w := &DataStreamWriter{ds: ds, fd: fd, wbuf: wbuf, offset: offset}
	return w, nil
}
