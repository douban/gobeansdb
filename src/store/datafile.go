package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
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
	rec := new(WriteRecord)
	rec.rec = new(Record)
	rec.rec.Payload = new(Payload)
	return rec
}
func wrapRecord(rec *Record) *WriteRecord {
	return &WriteRecord{
		rec: rec,
		ksz: uint32(len(rec.Key)),
		vsz: uint32(len(rec.Payload.Value)),
	}
}

func (rec *WriteRecord) decode(data []byte) (err error) {
	rec.decode(data)
	decodeHeader(rec, data)
	rec.rec.Key = data[recHeaderSize : recHeaderSize+rec.ksz]
	rec.rec.Payload.Value = data[recHeaderSize+rec.ksz : recHeaderSize+rec.ksz+rec.vsz]
	if rec.crc != rec.getHash() {
		err = fmt.Errorf("crc check fail")
		logger.Infof(err.Error())
		return
	}
	return nil
}

func (rec *WriteRecord) getHash() uint32 {
	hasher := newCrc32()
	hasher.write(rec.header[4:])
	hasher.write(rec.rec.Key)
	if len(rec.rec.Payload.Value) > 0 {
		hasher.write(rec.rec.Payload.Value)
	}
	return hasher.get()
}

func (rec *WriteRecord) encodeHeader() {
	h := rec.header[:]
	binary.LittleEndian.PutUint32(h[4:8], rec.rec.Payload.TS)
	binary.LittleEndian.PutUint32(h[8:12], rec.rec.Payload.Flag)
	binary.LittleEndian.PutUint32(h[12:16], uint32(rec.rec.Payload.Ver))
	binary.LittleEndian.PutUint32(h[16:20], rec.ksz)
	binary.LittleEndian.PutUint32(h[20:24], rec.vsz)
	crc := rec.getHash()
	binary.LittleEndian.PutUint32(h[:4], crc)
	return
}

func (rec *WriteRecord) decodeHeader() (err error) {
	return decodeHeader(rec, rec.header[:])
}
func decodeHeader(rec *WriteRecord, h []byte) (err error) {
	rec.crc = binary.LittleEndian.Uint32(h[:4])
	rec.rec.Payload.TS = binary.LittleEndian.Uint32(h[4:8])
	rec.rec.Payload.Flag = binary.LittleEndian.Uint32(h[8:12])
	rec.rec.Payload.Ver = int32(binary.LittleEndian.Uint32(h[12:16]))
	rec.ksz = binary.LittleEndian.Uint32(h[16:20])
	rec.vsz = binary.LittleEndian.Uint32(h[20:24])
	return
}

func readRecordAtPath(path string, offset uint32) (*WriteRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		logger.Infof(err.Error())
		return nil, err
	}
	defer f.Close()
	return readRecordAt(f, offset)
}

func readRecordAt(f *os.File, offset uint32) (*WriteRecord, error) {
	rec := newWriteRecord()
	if n, err := f.ReadAt(rec.header[:], int64(offset)); err != nil {
		logger.Infof(err.Error(), n)
		return nil, err
	}
	rec.decodeHeader()
	kv := make([]byte, rec.ksz+rec.vsz)

	rec.rec.Key = kv[:rec.ksz]
	rec.rec.Payload.Value = kv[rec.ksz:]
	if n, err := f.ReadAt(kv, int64(offset)+recHeaderSize); err != nil {
		logger.Infof(err.Error(), n)
		return nil, err
	}
	crc := rec.getHash()
	if rec.crc != crc {
		err := fmt.Errorf("crc check fail")
		logger.Infof(err.Error())
		return nil, err
	}
	return rec, nil
}

type DataStreamReader struct {
	ds *dataStore

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
	return &DataStreamReader{fd: fd, rbuf: rbuf, offset: 0}, nil
}

// TODO: slow
func (stream *DataStreamReader) nextValid() (r *Record, offset uint32, sizeBroken uint32, err error) {
	offset2 := stream.offset
	offset2 = offset2 & (^uint32(255))
	fd2, _ := os.Open(stream.fd.Name())
	defer fd2.Close()
	st, _ := fd2.Stat()
	for int64(offset) < st.Size() {
		rec2, err2 := readRecordAt(fd2, offset2)
		if err2 == nil {
			logger.Infof("crc fail end %x, sizeBroken %d", offset2, sizeBroken)
			_, rsize2 := rec2.rec.Sizes()
			offset3 := offset2 + rsize2
			stream.fd.Seek(int64(offset3), 0)
			stream.rbuf.Reset(stream.fd)
			return rec2.rec, offset2, sizeBroken, nil
		}
		sizeBroken += 1
		offset2 += 256
	}
	logger.Infof("crc fail until file end, sizeBroken %d", sizeBroken)
	return nil, offset2, sizeBroken, nil
}

func (stream *DataStreamReader) Next() (res *Record, offset uint32, sizeBroken uint32, err error) {
	rec := newWriteRecord()
	if _, err = io.ReadFull(stream.rbuf, rec.header[:]); err != nil {
		if err != io.EOF {
			logger.Infof(err.Error(), err)
		} else {
			err = nil
		}
		return
	}
	rec.decodeHeader()
	if rec.ksz > 250 || rec.ksz <= 0 { // TODO
		logger.Fatalf("bad key len %s %d %d %d", stream.fd.Name(), stream.offset, rec.ksz, rec.vsz)
		return stream.nextValid()
	}

	rec.rec.Key = make([]byte, rec.ksz)
	if _, err = io.ReadFull(stream.rbuf, rec.rec.Key); err != nil {
		logger.Infof(err.Error())
		return
	}
	rec.rec.Payload.Value = make([]byte, rec.vsz)
	if _, err = io.ReadFull(stream.rbuf, rec.rec.Payload.Value); err != nil {
		logger.Infof(err.Error())
		return
	}
	recsize, _ := rec.rec.Sizes()
	tail := recsize & 0xff
	if tail != 0 {
		stream.rbuf.Discard(int(256 - tail))
	}

	crc := rec.getHash()
	if rec.crc != crc {
		err := fmt.Errorf("crc fail begin offset %x", stream.offset)
		logger.Infof(err.Error())
		sizeBroken += 1
		return stream.nextValid()
	}
	res = rec.rec
	return
}

func (stream *DataStreamReader) Offset() uint32 {
	return stream.offset
}

func (stream *DataStreamReader) Close() error {
	return stream.fd.Close()
}

type DataStreamWriter struct {
	ds   *dataStore
	fd   *os.File
	wbuf *bufio.Writer

	chunk  int
	offset uint32
}

func (stream *DataStreamWriter) Append(r *Record) (offset uint32, err error) {
	return stream.offset, stream.append(wrapRecord(r))
}

func (stream *DataStreamWriter) append(r *WriteRecord) error {
	wbuf := stream.wbuf
	r.encodeHeader()
	size, sizeall := r.rec.Sizes()
	stream.offset += sizeall
	if n, err := wbuf.Write(r.header[:]); err != nil {
		logger.Infof(err.Error(), n)
		return err
	}
	if n, err := wbuf.Write(r.rec.Key); err != nil {
		logger.Infof(err.Error(), n)
		return err
	}
	if n, err := wbuf.Write(r.rec.Payload.Value); err != nil {
		logger.Infof(err.Error(), n)
		return err
	}
	npad := sizeall - size
	if npad != 0 {
		if n, err := wbuf.Write(padding[:npad]); err != nil {
			logger.Infof(err.Error(), n)
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
		logger.Fatalf("%s %s %s", stream.fd.Name, err.Error(), st.Mode())
	}
	return stream.fd.Close()
}
