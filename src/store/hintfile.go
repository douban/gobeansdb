package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	HINTFILE_HEAD_SIZE = 16
	HINTITEM_HEAD_SIZE = 19
	HINTINDEX_ROW_SIZE = 4096
)

type hintFileMeta struct {
	numKey      int
	indexOffset int64
	datasize    uint32
}

func (fm *hintFileMeta) Dumps(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], uint64(fm.indexOffset))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(fm.numKey))
	binary.LittleEndian.PutUint32(buf[12:16], fm.datasize)
}

func (fm *hintFileMeta) Loads(buf []byte) {
	fm.indexOffset = int64(binary.LittleEndian.Uint64(buf[:8]))
	fm.numKey = int(binary.LittleEndian.Uint32(buf[8:12]))
	fm.datasize = uint32(binary.LittleEndian.Uint32(buf[12:16]))
}

// used for 1. HTree scan 2. hint merge
type hintFileReader struct {
	hintFileMeta
	path    string
	bufsize int
	chunkID int
	size    int64

	fd     *os.File
	rbuf   *bufio.Reader
	offset int64
	buf    [256]byte
}

func newHintFileReader(path string, chunkID, bufsize int) (reader *hintFileReader) {
	return &hintFileReader{
		path:    path,
		chunkID: chunkID,
		bufsize: bufsize,
	}
}

func (reader *hintFileReader) open() (err error) {
	reader.fd, err = os.Open(reader.path)
	if err != nil {
		logger.Errorf(err.Error())
		return err
	}
	reader.rbuf = bufio.NewReaderSize(reader.fd, reader.bufsize)
	h := reader.buf[:HINTFILE_HEAD_SIZE]
	var readn int
	readn, err = io.ReadFull(reader.rbuf, h)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	if readn < HINTFILE_HEAD_SIZE {
		err = fmt.Errorf("bad hint file %s readn %d", reader.path, readn)
		logger.Errorf(err.Error())
		return
	}
	reader.hintFileMeta.Loads(h)
	//logger.Infof("open hint for read %#v, %s", reader.hintFileMeta, reader.path)
	fileInfo, _ := reader.fd.Stat()
	reader.size = fileInfo.Size()
	reader.offset = HINTFILE_HEAD_SIZE
	if reader.indexOffset == 0 {
		logger.Errorf("%s has no index", reader.path)
		reader.indexOffset = reader.size
	}

	return nil
}

func (reader *hintFileReader) next() (item *HintItem, err error) {
	if reader.offset >= reader.indexOffset {
		return nil, nil
	}
	h := reader.buf[:HINTITEM_HEAD_SIZE]
	item = new(HintItem)
	var readn int
	readn, err = io.ReadFull(reader.rbuf, h)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	if readn < HINTITEM_HEAD_SIZE {
		err = fmt.Errorf("bad hint file %s readn %d", reader.path, readn)
		return
	}
	item.Keyhash = binary.LittleEndian.Uint64(h[:8])
	item.Pos = binary.LittleEndian.Uint32(h[8:12])
	item.Ver = int32(binary.LittleEndian.Uint32(h[12:16]))
	item.Vhash = binary.LittleEndian.Uint16(h[16:18])
	ksz := int(h[18])
	key := reader.buf[:ksz]
	readn, err = io.ReadFull(reader.rbuf, key)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	if readn < ksz {
		err = fmt.Errorf("bad hint file %s readn %d", reader.path, readn)
		return
	}
	item.Key = string(key[:ksz])
	reader.offset += HINTITEM_HEAD_SIZE + int64(ksz)
	return item, nil
}

func (reader *hintFileReader) close() {
	reader.fd.Close()
}

// used for 1. dump hint 2. hint merge
type hintFileWriter struct {
	index *hintFileIndexBuffer
	hintFileMeta
	path string

	fd     *os.File
	wbuf   *bufio.Writer
	offset int64
	buf    [256]byte
}

func newHintFileWriter(path string, maxOffset uint32, bufsize int) (w *hintFileWriter, err error) {
	var fd *os.File
	tmp := path + ".tmp"
	logger.Infof("create hint file: %s", tmp)
	fd, err = os.Create(tmp)
	if err != nil {
		logger.Errorf(err.Error())
		return nil, err
	}
	wbuf := bufio.NewWriterSize(fd, bufsize)
	w = &hintFileWriter{
		fd:           fd,
		wbuf:         wbuf,
		offset:       HINTFILE_HEAD_SIZE,
		path:         path,
		hintFileMeta: hintFileMeta{datasize: maxOffset}}
	w.wbuf.Write(w.buf[:HINTFILE_HEAD_SIZE])
	w.index = newHintFileIndex()
	return
}

func (w *hintFileWriter) writeItem(item *HintItem) error {
	h := w.buf[:HINTITEM_HEAD_SIZE]
	binary.LittleEndian.PutUint64(h[0:8], item.Keyhash)
	binary.LittleEndian.PutUint32(h[8:12], item.Pos)
	binary.LittleEndian.PutUint32(h[12:16], uint32(item.Ver))
	binary.LittleEndian.PutUint16(h[16:18], item.Vhash)
	h[18] = byte(len(item.Key))
	w.wbuf.Write(h)
	w.wbuf.WriteString(item.Key)
	// TODO: refactor
	if (w.offset - w.index.lastoffset) > int64(conf.IndexIntervalSize-HINTITEM_HEAD_SIZE-256) {
		w.index.append(item.Keyhash, w.offset)
	}
	w.offset += HINTITEM_HEAD_SIZE + int64(len(item.Key))
	w.numKey += 1
	return nil
}

func (w *hintFileWriter) close() error {
	w.indexOffset = w.offset
	index := w.index
	var buf [16]byte
	for r := 0; r <= index.currRow; r++ {
		col := HINTINDEX_ROW_SIZE
		if r == index.currRow {
			col = index.currCol
		}
		for c := 0; c < col; c++ {
			it := index.index[r][c]
			binary.LittleEndian.PutUint64(buf[0:8], it.keyhash)
			binary.LittleEndian.PutUint64(buf[8:16], uint64(it.offset))
			w.wbuf.Write(buf[:])
		}
	}
	w.wbuf.Flush()
	w.fd.Seek(0, 0)
	w.hintFileMeta.Dumps(buf[:])
	w.fd.Write(buf[:])
	w.fd.Close()
	tmp := w.path + ".tmp"
	err := os.Rename(tmp, w.path)
	if err != nil {
		return err
	} else {
		logger.Infof("moved %s -> %s", tmp, w.path)
	}

	return nil
}
