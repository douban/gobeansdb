package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/douban/gobeansdb/cmem"
	"github.com/douban/gobeansdb/config"
)

const (
	recHeaderSize = 24
)

var (
	padding [256]byte
)

type WriteRecord struct {
	rec    *Record
	crc    uint32
	ksz    uint32
	vsz    uint32
	pos    Position
	header [recHeaderSize]byte
}

func newWriteRecord() *WriteRecord {
	wrec := new(WriteRecord)
	wrec.rec = new(Record)
	wrec.rec.Payload = new(Payload)
	return wrec
}
func wrapRecord(rec *Record) *WriteRecord {
	_, rec.Payload.RecSize = rec.Sizes()
	return &WriteRecord{
		rec: rec,
		ksz: uint32(len(rec.Key)),
		vsz: uint32(len(rec.Payload.Body)),
	}
}

func (wrec *WriteRecord) String() string {
	return fmt.Sprintf("{ts: %v,flag 0x%x, ver %d, ksz: %d, vsz: %d}",
		time.Unix(int64(wrec.rec.Payload.TS), 0),
		wrec.rec.Payload.Flag, wrec.rec.Payload.Ver, wrec.ksz, wrec.vsz)
}

func (wrec *WriteRecord) decode(data []byte) (err error) {
	wrec.decode(data)
	decodeHeader(wrec, data)
	wrec.rec.Key = data[recHeaderSize : recHeaderSize+wrec.ksz]
	wrec.rec.Payload.Body = data[recHeaderSize+wrec.ksz : recHeaderSize+wrec.ksz+wrec.vsz]
	if wrec.crc != wrec.getCRC() {
		err = fmt.Errorf("crc check fail")
		logger.Infof(err.Error())
		return
	}
	return nil
}

func (wrec *WriteRecord) getCRC() uint32 {
	hasher := newCrc32()
	hasher.write(wrec.header[4:])
	if len(wrec.rec.Key) > 0 {
		hasher.write(wrec.rec.Key)
	}
	if len(wrec.rec.Payload.Body) > 0 {
		hasher.write(wrec.rec.Payload.Body)
	}
	return hasher.get()
}

func (wrec *WriteRecord) encodeHeader() {
	h := wrec.header[:]
	binary.LittleEndian.PutUint32(h[4:8], wrec.rec.Payload.TS)
	binary.LittleEndian.PutUint32(h[8:12], wrec.rec.Payload.Flag)
	binary.LittleEndian.PutUint32(h[12:16], uint32(wrec.rec.Payload.Ver))
	binary.LittleEndian.PutUint32(h[16:20], wrec.ksz)
	binary.LittleEndian.PutUint32(h[20:24], wrec.vsz)
	crc := wrec.getCRC()
	binary.LittleEndian.PutUint32(h[:4], crc)
	return
}

func (wrec *WriteRecord) decodeHeader() (err error) {
	return decodeHeader(wrec, wrec.header[:])
}

func decodeHeader(wrec *WriteRecord, h []byte) (err error) {
	wrec.crc = binary.LittleEndian.Uint32(h[:4])
	wrec.rec.Payload.TS = binary.LittleEndian.Uint32(h[4:8])
	wrec.rec.Payload.Flag = binary.LittleEndian.Uint32(h[8:12])
	wrec.rec.Payload.Ver = int32(binary.LittleEndian.Uint32(h[12:16]))
	wrec.ksz = binary.LittleEndian.Uint32(h[16:20])
	wrec.vsz = binary.LittleEndian.Uint32(h[20:24])
	return
}

func readRecordAtPath(path string, offset uint32) (*WriteRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		logger.Errorf("fail to open: %s: %v", path, err)
		return nil, err
	}
	defer f.Close()
	return readRecordAt(path, f, offset)
}

func readRecordAt(path string, f *os.File, offset uint32) (wrec *WriteRecord, err error) {
	wrec = newWriteRecord()
	defer func() {
		if err != nil {
			wrec.rec.Payload.Free() // must not return (nil, err)
			wrec = nil
		}
	}()
	var n int
	if n, err = f.ReadAt(wrec.header[:], int64(offset)); err != nil {
		err = fmt.Errorf("fail to read head %s:%d, err = %s, n = %d", path, offset, err.Error(), n)
		logger.Errorf(err.Error())
		return
	}
	wrec.decodeHeader()
	if !config.IsValidKeySize(wrec.ksz) {
		err = fmt.Errorf("bad key size %s:%d, wrec %v", path, offset, wrec)
		logger.Errorf(err.Error())
		return
	} else if !config.IsValidValueSize(wrec.vsz) {
		err = fmt.Errorf("bad value size %s:%d, wrec %v", path, offset, wrec)
		logger.Errorf(err.Error())
		return
	}
	kvSize := int(wrec.ksz + wrec.vsz)
	var kv cmem.CArray
	if !kv.Alloc(int(kvSize)) {
		err = fmt.Errorf("fail to alloc for read %s:%d, wrec %v ", path, offset, wrec)
		logger.Errorf(err.Error())
		return
	}
	cmem.DBRL.GetData.AddSizeAndCount(kv.Cap)

	wrec.rec.Key = kv.Body[:wrec.ksz]

	if n, err = f.ReadAt(kv.Body, int64(offset)+recHeaderSize); err != nil {
		err = fmt.Errorf("fail to  read %s:%d, rec %v; return err = %s, n = %d",
			path, offset, wrec, err.Error(), n)
		logger.Errorf(err.Error())
		cmem.DBRL.GetData.SubSizeAndCount(kv.Cap)
		return
	}
	wrec.rec.Key = make([]byte, wrec.ksz)
	copy(wrec.rec.Key, kv.Body[:wrec.ksz])
	wrec.rec.Payload.CArray = kv
	wrec.rec.Payload.Body = kv.Body[wrec.ksz:]
	wrec.rec.Payload.RecSize = wrec.vsz
	crc := wrec.getCRC()
	if wrec.crc != crc {
		err = fmt.Errorf("crc check fail %s:%d, rec %v; %d != %d",
			path, offset, wrec, wrec.crc, crc)
		logger.Errorf(err.Error())
		cmem.DBRL.GetData.SubSizeAndCount(kv.Cap)
		return
	}
	return wrec, nil
}

type DataStreamReader struct {
	path string
	ds   *dataStore

	fd   *os.File
	rbuf *bufio.Reader

	chunk  int
	offset uint32
}

func newDataStreamReader(path string, bufsz int) (*DataStreamReader, error) {
	fd, err := os.Open(path)
	if err != nil {
		logger.Infof(err.Error())
		return nil, err
	}
	rbuf := bufio.NewReaderSize(fd, bufsz)
	return &DataStreamReader{path: path, fd: fd, rbuf: rbuf, offset: 0}, nil
}

func (stream *DataStreamReader) seek(offset uint32) {
	stream.fd.Seek(int64(offset), os.SEEK_SET)
	stream.offset = offset
}

// TODO: slow
func (stream *DataStreamReader) nextValid() (rec *Record, offset uint32, sizeBroken uint32, err error) {
	offset2 := stream.offset
	offset2 = offset2 & (^uint32(0xff))
	fd, _ := os.Open(stream.fd.Name())
	defer fd.Close()
	st, _ := fd.Stat()
	for int64(offset2) < st.Size() {
		wrec, err2 := readRecordAt(stream.path, fd, offset2)
		if err2 == nil {
			logger.Infof("crc fail end offset 0x%x, sizeBroken 0x%x", offset2, sizeBroken)
			_, rsize := wrec.rec.Sizes()
			offset3 := offset2 + rsize
			stream.fd.Seek(int64(offset3), 0)
			stream.rbuf.Reset(stream.fd)
			stream.offset = offset2 + rsize
			return wrec.rec, offset2, sizeBroken, nil
		}
		sizeBroken += 256
		offset2 += 256
		stream.offset = offset2
	}

	logger.Infof("crc fail until file end, sizeBroken 0x%x", sizeBroken)
	return nil, offset2, sizeBroken, nil
}

func (stream *DataStreamReader) Next() (res *Record, offset uint32, sizeBroken uint32, err error) {
	wrec := newWriteRecord()
	if _, err = io.ReadFull(stream.rbuf, wrec.header[:]); err != nil {
		if err != io.EOF {
			logger.Errorf("%s:0x%x %s", stream.path, stream.offset, err.Error())
		} else {
			err = nil
		}
		return
	}
	wrec.decodeHeader()
	if wrec.ksz > 250 || wrec.ksz <= 0 { // TODO
		logger.Errorf("bad key len %s %d %d %d", stream.fd.Name(), stream.offset, wrec.ksz, wrec.vsz)
		return stream.nextValid()
	}

	wrec.rec.Key = make([]byte, wrec.ksz)
	if _, err = io.ReadFull(stream.rbuf, wrec.rec.Key); err != nil {
		logger.Errorf(err.Error())
		return
	}
	wrec.rec.Payload.Body = make([]byte, wrec.vsz)
	if _, err = io.ReadFull(stream.rbuf, wrec.rec.Payload.Body); err != nil {
		logger.Errorf(err.Error())
		return
	}
	recsizereal, recsize := wrec.rec.Sizes()
	tail := recsizereal & 0xff
	if tail != 0 {
		stream.rbuf.Discard(int(PADDING - tail))
	}

	crc := wrec.getCRC()
	if wrec.crc != crc {
		err := fmt.Errorf("crc fail begin offset %x", stream.offset)
		logger.Errorf(err.Error())
		sizeBroken += 1
		return stream.nextValid()
	}
	res = wrec.rec
	offset = stream.offset
	stream.offset += recsize
	res.Payload.RecSize = recsize
	return
}

func (stream *DataStreamReader) Offset() uint32 {
	return stream.offset
}

func (stream *DataStreamReader) Close() error {
	return stream.fd.Close()
}

type DataStreamWriter struct {
	path string
	fd   *os.File
	wbuf *bufio.Writer

	chunk  int
	offset uint32
}

func (stream *DataStreamWriter) Append(rec *Record) (offset uint32, err error) {
	return stream.append(wrapRecord(rec))
}

func (stream *DataStreamWriter) append(wrec *WriteRecord) (offset uint32, err error) {
	offset = stream.offset
	err = wrec.append(stream.wbuf, true)
	stream.offset += wrec.rec.Payload.RecSize
	return
}

func (wrec *WriteRecord) append(wbuf io.Writer, dopadding bool) error {
	wrec.encodeHeader()
	size, sizeall := wrec.rec.Sizes()
	if n, err := wbuf.Write(wrec.header[:]); err != nil {
		logger.Errorf("%v %d", err, n)
		return err
	}
	if n, err := wbuf.Write(wrec.rec.Key); err != nil {
		logger.Errorf("%v %d", err, n)
		return err
	}
	if n, err := wbuf.Write(wrec.rec.Payload.Body); err != nil {
		logger.Errorf("%v %d", err, n)
		return err
	}
	npad := sizeall - size
	if dopadding && npad != 0 {
		if n, err := wbuf.Write(padding[:npad]); err != nil {
			logger.Errorf("%v %d", err, n)
			return err
		}
	}
	return nil
}

func (stream *DataStreamWriter) Offset() uint32 {
	return stream.offset
}

func (stream *DataStreamWriter) Close() error {
	if err := stream.wbuf.Flush(); err != nil {
		st, _ := stream.fd.Stat()
		logger.Errorf("flush err: %s %v %s", stream.fd.Name(), err, st.Mode())
		return err
	}
	return stream.fd.Close()
}
