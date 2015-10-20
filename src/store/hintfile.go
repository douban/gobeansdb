package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
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
	size        int64
}

// used for 1. HTree scan 2. hint merge
type hintFileReader struct {
	hintFileMeta
	path    string
	bufsize int
	chunkID int

	fd     *os.File
	rbuf   *bufio.Reader
	offset int64
	buf    [256]byte
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
	readn, err = reader.fd.Read(h)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	if readn < HINTFILE_HEAD_SIZE {
		err = fmt.Errorf("bad hint file %s readn %d", reader.path, readn)
		logger.Errorf(err.Error())
		return
	}
	reader.indexOffset = int64(binary.LittleEndian.Uint64(h[:8]))
	reader.numKey = int(binary.LittleEndian.Uint32(h[8:12]))
	logger.Debugf("%#v", reader.hintFileMeta)
	fileInfo, _ := reader.fd.Stat()
	reader.size = fileInfo.Size()
	reader.offset = HINTFILE_HEAD_SIZE
	if reader.indexOffset == 0 {
		logger.Errorf("%s has no index", reader.path)
		reader.indexOffset = reader.size
	}

	return nil
}

func (reader *hintFileReader) next() (item *hintItem, err error) {
	if reader.offset >= reader.indexOffset {
		return nil, nil
	}
	h := reader.buf[:HINTITEM_HEAD_SIZE]
	item = new(hintItem)
	var readn int
	readn, err = reader.fd.Read(h)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	if readn < HINTITEM_HEAD_SIZE {
		err = fmt.Errorf("bad hint file %s readn %d", reader.path, readn)
		return
	}
	item.keyhash = binary.LittleEndian.Uint64(h[:8])
	item.pos = binary.LittleEndian.Uint32(h[8:12])
	item.ver = int32(binary.LittleEndian.Uint32(h[12:16]))
	item.vhash = binary.LittleEndian.Uint16(h[16:18])
	ksz := int(h[18])
	key := reader.buf[:ksz]
	readn, err = reader.fd.Read(key)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	if readn < ksz {
		err = fmt.Errorf("bad hint file %s readn %d", reader.path, readn)
		return
	}
	item.key = string(key[:ksz])
	reader.offset += HINTITEM_HEAD_SIZE + int64(ksz)
	return item, nil
}

func (reader *hintFileReader) close() {
	reader.fd.Close()
}

func newHintFileWriter(path string, bufsize int) (w *hintFileWriter, err error) {
	var fd *os.File
	logger.Infof("create hint file: %s", path)
	fd, err = os.Create(path)
	if err != nil {
		logger.Errorf(err.Error())
		return nil, err
	}
	wbuf := bufio.NewWriterSize(fd, bufsize)
	w = &hintFileWriter{fd: fd, wbuf: wbuf, offset: HINTFILE_HEAD_SIZE}
	w.wbuf.Write(w.buf[:HINTFILE_HEAD_SIZE])
	w.index = newHintFileIndex()
	return
}

func (w *hintFileWriter) writeItem(item *hintItem) error {
	h := w.buf[:HINTITEM_HEAD_SIZE]
	binary.LittleEndian.PutUint64(h[0:8], item.keyhash)
	binary.LittleEndian.PutUint32(h[8:12], item.pos)
	binary.LittleEndian.PutUint32(h[12:16], uint32(item.ver))
	binary.LittleEndian.PutUint16(h[16:18], item.vhash)
	h[18] = byte(len(item.key))
	w.wbuf.Write(h)
	w.wbuf.WriteString(item.key)
	// TODO: refactor
	if (w.offset - w.index.lastoffset) > int64(hintConfig.IndexIntervalSize-HINTITEM_HEAD_SIZE-256) {
		w.index.append(item.keyhash, w.offset)
	}
	w.offset += HINTITEM_HEAD_SIZE + int64(len(item.key))
	w.numKey += 1
	return nil
}

func (w *hintFileWriter) close() error {
	w.indexOffset = w.offset
	index := w.index
	var buf [16]byte
	for r := 0; r <= index.currRow; r++ {
		for c := 0; c < index.currCol; c++ {
			it := index.index[r][c]
			binary.LittleEndian.PutUint64(buf[0:8], it.keyhash)
			binary.LittleEndian.PutUint64(buf[8:16], uint64(it.offset))
			w.wbuf.Write(buf[:])
		}
	}
	w.wbuf.Flush()
	w.fd.Seek(0, 0)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(w.indexOffset))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(w.numKey))
	w.fd.Write(buf[:])
	w.fd.Close()
	return nil
}
