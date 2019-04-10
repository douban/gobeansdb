package store

import (
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/douban/gobeansdb/cmem"
	"github.com/douban/gobeansdb/utils"
)

type dataChunk struct {
	sync.Mutex

	chunkid int
	path    string
	size    uint32

	writingHead uint32
	wbuf        []*WriteRecord

	rewriting bool
	gcbufsize uint32
	gcWriter  *DataStreamWriter
}

func (dc *dataChunk) GoString() string {
	return fmt.Sprintf("(lock %v size %d writingHead %d rewriting %v len(wbuf) %d wbuf[0].pos %v wbuf[-1].pos %v)",
		dc.Mutex, dc.size, dc.writingHead, dc.rewriting, len(dc.wbuf),
		dc.wbuf[0].pos, dc.wbuf[len(dc.wbuf)-1].pos)
}

func (dc *dataChunk) Clear() error {
	dc.wbuf = nil
	dc.size = 0
	dc.rewriting = false
	dc.gcWriter = nil
	dc.gcbufsize = 0
	dc.writingHead = 0

	return utils.Remove(dc.path)
}

func (dc *dataChunk) AppendRecord(wrec *WriteRecord) {
	dc.Lock()
	dc.wbuf = append(dc.wbuf, wrec)

	size := wrec.rec.Payload.RecSize

	dc.writingHead += size
	dc.size = dc.writingHead
	dc.Unlock()
}

func (dc *dataChunk) AppendRecordGC(wrec *WriteRecord) (offset uint32, err error) {
	dc.Lock()
	wrec.pos.ChunkID = dc.chunkid
	offset = dc.writingHead
	wrec.pos.Offset = offset
	size := wrec.rec.Payload.RecSize

	dc.writingHead += size
	if dc.writingHead >= dc.size {
		dc.size = dc.writingHead
	}
	dc.Unlock()

	_, err = dc.gcWriter.append(wrec)
	if err != nil {
		logger.Fatalf("fail to append, stop! err: %v", err)
	}

	if err = dc.gcWriter.wbuf.Flush(); err != nil {
		logger.Fatalf("write data fail, stop! err: %v", err)
		return 0, err
	}
	return
}

func (dc *dataChunk) getDiskFileSize() uint32 {
	if len(dc.wbuf) > 0 {
		return dc.wbuf[0].pos.Offset
	}
	return dc.size
}

func (dc *dataChunk) flush(w *DataStreamWriter, gc bool) (flushed uint32, err error) {
	dc.Lock()
	n := len(dc.wbuf)
	dc.Unlock()
	for i := 0; i < n; i++ {
		dc.Lock() // because append may change the slice
		wrec := dc.wbuf[i]
		dc.Unlock()
		_, err := w.append(wrec)
		if err != nil {
			logger.Fatalf("fail to append, stop! err: %v", err)
		}
		size := wrec.rec.Payload.RecSize
		flushed += size
		if !gc && wrec.rec.Payload.Ver > 0 {
			cmem.DBRL.FlushData.SubSizeAndCount(wrec.rec.Payload.CArray.Cap)
			// NOTE: not freed yet, make it a little diff with AllocRL, which may provide more insight
		}
	}
	if err = w.wbuf.Flush(); err != nil {
		logger.Fatalf("write data fail, stop! err: %v", err)
		return 0, err
	}

	dc.Lock()
	tofree := dc.wbuf[:n]
	dc.wbuf = dc.wbuf[n:]
	dc.Unlock()
	for _, wrec := range tofree {
		wrec.rec.Payload.Free()
	}
	return
}

func (dc *dataChunk) GetRecordByOffsetInBuffer(offset uint32) (res *Record, err error) {
	dc.Lock()
	defer dc.Unlock()

	wbuf := dc.wbuf
	n := len(wbuf)
	if n == 0 || offset < wbuf[0].pos.Offset || offset >= dc.writingHead {
		return
	}

	idx := sort.Search(n, func(i int) bool { return wbuf[i].pos.Offset >= offset })
	if idx >= n {
		err = fmt.Errorf("%d %d %d %d %d", n, idx, dc.size, dc.writingHead, offset)
		logger.Errorf("%v", err)
		return
	}
	wrec := wbuf[idx]
	if wrec.pos.Offset == offset {
		res = wrec.rec.Copy()
		cmem.DBRL.GetData.AddSizeAndCount(res.Payload.CArray.Cap)
		return
	} else {
		err = fmt.Errorf("rec should in buffer, but not, pos = %#v", Position{dc.chunkid, offset})
		return
	}
	return
}

func (dc *dataChunk) GetRecordByOffset(offset uint32) (res *Record, inbuffer bool, err error) {
	res, err = dc.GetRecordByOffsetInBuffer(offset)
	if err != nil {
		inbuffer = true
		return
	}
	if res != nil {
		inbuffer = true
		cmem.DBRL.GetData.AddSize(res.Payload.DiffSizeAfterDecompressed())
		res.Payload.Decompress()
		return
	}
	wrec, e := readRecordAtPath(dc.path, offset)
	if e != nil {
		return nil, false, e
	}
	cmem.DBRL.GetData.AddSize(wrec.rec.Payload.DiffSizeAfterDecompressed())
	wrec.rec.Payload.Decompress()
	return wrec.rec, false, nil
}

func (dc *dataChunk) Truncate(size uint32) error {
	path := dc.path
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

func (dc *dataChunk) beginGCWriting(srcChunk int) (err error) {
	logger.Infof("BeginGCWriting chunk %d from %d rewrite %v size %d wsize %d ", dc.chunkid, srcChunk, dc.rewriting, dc.size, dc.writingHead)
	if dc.chunkid == srcChunk {
		dc.rewriting = true
		dc.writingHead = 0
		logger.Infof("rewrite %s", dc.path)
	} else {
		dc.writingHead = dc.size
	}
	dc.gcWriter, err = GetStreamWriter(dc.path, !dc.rewriting)
	if err != nil {
		dc.gcWriter = nil
	}
	return
}

func (dc *dataChunk) endGCWriting() (err error) {
	logger.Infof("endGCWriting chunk %d rewrite %v size %d wsize%d ", dc.chunkid, dc.rewriting, dc.size, dc.writingHead)
	if dc.gcWriter != nil {
		err = dc.gcWriter.wbuf.Flush()
		dc.gcWriter.Close()
		dc.gcWriter = nil
	}
	if dc.rewriting && dc.writingHead < dc.size {
		dc.Truncate(dc.writingHead)
		dc.size = dc.writingHead
	}
	dc.rewriting = false
	return
}

func (dc *dataChunk) getFirstRecTs() (ts int64, err error) {
	f, err := os.Open(dc.path)
	if err != nil {
		logger.Errorf("%v", err)
		return
	}
	var n int
	wrec := newWriteRecord()
	if n, err = f.ReadAt(wrec.header[:], 0); err != nil {
		err = fmt.Errorf("fail to read head %s, err = %v, n = %d", dc.path, err, n)
		logger.Errorf(err.Error())
		return
	}
	wrec.decodeHeader()
	ts = int64(wrec.rec.Payload.TS)
	return
}
