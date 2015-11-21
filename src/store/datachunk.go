package store

import (
	"cmem"
	"fmt"
	"os"
	"sort"
	"sync"
	"utils"
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

func (dc *dataChunk) AppendRecord(wrec *WriteRecord) {
	dc.wbuf = append(dc.wbuf, wrec)
	size := wrec.rec.Payload.RecSize

	dc.writingHead += size
	dc.size = dc.writingHead
}

func (dc *dataChunk) AppendRecordGC(wrec *WriteRecord) (offset uint32, err error) {
	offset = dc.writingHead
	dc.wbuf = append(dc.wbuf, wrec)
	size := wrec.rec.Payload.RecSize

	dc.writingHead += size
	if dc.writingHead >= dc.size {
		dc.size = dc.writingHead
	}

	dc.gcbufsize += size
	if dc.gcbufsize > (1 << 20) {
		_, err = dc.flush(dc.gcWriter)
		dc.gcbufsize = 0
	}
	return
}

func (dc *dataChunk) getDiskFileSize() uint32 {
	if len(dc.wbuf) > 0 {
		return dc.wbuf[0].pos.Offset
	}
	return dc.size
}

func (dc *dataChunk) flush(w *DataStreamWriter) (flushed uint32, err error) {
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
		if wrec.rec.Payload.Ver > 0 {
			cmem.DBRL.FlushData.SubSize(wrec.rec.Payload.AccountingSize)
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
	if n == 0 || offset < wbuf[0].pos.Offset || offset > dc.writingHead {
		return
	}

	idx := sort.Search(n, func(i int) bool { return wbuf[i].pos.Offset >= offset })
	wrec := wbuf[idx]
	if wrec.pos.Offset == offset {
		cmem.DBRL.GetData.AddSize(wrec.rec.Payload.AccountingSize)
		res = wrec.rec.Copy()
		return
	} else {
		err = fmt.Errorf("rec should in buffer, but not, pos = %#v", Position{dc.chunkid, offset})
		return
	}
	return
}

func (dc *dataChunk) GetRecordByOffset(offset uint32) (res *Record, err error) {
	res, err = dc.GetRecordByOffsetInBuffer(offset)
	if err != nil {
		return
	}
	if res != nil {
		res.Payload.Decompress()
		return
	}
	wrec, e := readRecordAtPath(dc.path, offset)
	if e != nil {
		return nil, e
	}
	wrec.rec.Payload.Decompress()
	return wrec.rec, nil
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
	if dc.chunkid == srcChunk {
		dc.rewriting = true
		dc.writingHead = 0
	}
	dc.gcWriter, err = GetStreamWriter(dc.path, !dc.rewriting)
	if err != nil {
		dc.gcWriter = nil
	}
	return
}

func (dc *dataChunk) endGCWriting() (err error) {
	if dc.gcWriter != nil {
		_, err = dc.flush(dc.gcWriter)
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
