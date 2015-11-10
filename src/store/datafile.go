package store

import (
	"bufio"
	"cmem"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"quicklz"
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
		vsz: uint32(len(rec.Payload.Value)),
	}
}

func (wrec *WriteRecord) decode(data []byte) (err error) {
	wrec.decode(data)
	decodeHeader(wrec, data)
	wrec.rec.Key = data[recHeaderSize : recHeaderSize+wrec.ksz]
	wrec.rec.Payload.Value = data[recHeaderSize+wrec.ksz : recHeaderSize+wrec.ksz+wrec.vsz]
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
	hasher.write(wrec.rec.Key)
	if len(wrec.rec.Payload.Value) > 0 {
		hasher.write(wrec.rec.Payload.Value)
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

func (rec *WriteRecord) decodeHeader() (err error) {
	return decodeHeader(rec, rec.header[:])
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
		logger.Infof(err.Error())
		return nil, err
	}
	defer f.Close()
	return readRecordAt(f, offset)
}

func readRecordAt(f *os.File, offset uint32) (*WriteRecord, error) {
	wrec := newWriteRecord()
	if n, err := f.ReadAt(wrec.header[:], int64(offset)); err != nil {
		logger.Infof("%s %d", err.Error(), n)
		return nil, err
	}
	wrec.decodeHeader()
	kvSize := int64(wrec.ksz + wrec.vsz)
	kv := make([]byte, kvSize)

	cmem.DBRL.GetData.AddSize(kvSize)
	wrec.rec.Key = kv[:wrec.ksz]
	wrec.rec.Payload.Value = kv[wrec.ksz:]
	if n, err := f.ReadAt(kv, int64(offset)+recHeaderSize); err != nil {
		logger.Infof(err.Error(), n)
		cmem.DBRL.GetData.SubSize(int64(kvSize))
		return nil, err
	}

	crc := wrec.getCRC()
	if wrec.crc != crc {
		err := fmt.Errorf("crc check fail")
		logger.Infof(err.Error())
		cmem.DBRL.GetData.SubSize(int64(kvSize))
		return nil, err
	}
	wrec.rec.Payload.AccountingSize = kvSize
	if wrec.rec.Payload.IsCompressed() {
		diff := int64(quicklz.SizeDecompressed(wrec.rec.Payload.Value) - int(wrec.vsz))
		cmem.DBRL.GetData.AddSize(diff)
		wrec.rec.Payload.AccountingSize += diff
	}
	return wrec, nil
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

func (stream *DataStreamReader) seek(offset uint32) {
	stream.fd.Seek(int64(offset), os.SEEK_SET)
	stream.offset = offset
}

// TODO: slow
func (stream *DataStreamReader) nextValid() (rec *Record, offset uint32, sizeBroken uint32, err error) {
	offset2 := stream.offset
	offset2 = offset2 & (^uint32(255))
	fd, _ := os.Open(stream.fd.Name())
	defer fd.Close()
	st, _ := fd.Stat()
	for int64(offset) < st.Size() {
		wrec, err2 := readRecordAt(fd, offset2)
		if err2 == nil {
			logger.Infof("crc fail end %x, sizeBroken %d", offset2, sizeBroken)
			_, rsize := wrec.rec.Sizes()
			offset3 := offset2 + rsize
			stream.fd.Seek(int64(offset3), 0)
			stream.rbuf.Reset(stream.fd)
			stream.offset = offset2 + rsize
			cmem.DBRL.GetData.SubSize(wrec.rec.Payload.AccountingSize)
			return wrec.rec, offset2, sizeBroken, nil
		}
		sizeBroken += 1
		offset2 += 256
		stream.offset = offset2
	}

	logger.Infof("crc fail until file end, sizeBroken %d", sizeBroken)
	return nil, offset2, sizeBroken, nil
}

func (stream *DataStreamReader) Next() (res *Record, offset uint32, sizeBroken uint32, err error) {
	wrec := newWriteRecord()
	if _, err = io.ReadFull(stream.rbuf, wrec.header[:]); err != nil {
		if err != io.EOF {
			logger.Infof(err.Error(), err)
		} else {
			err = nil
		}
		return
	}
	wrec.decodeHeader()
	if wrec.ksz > 250 || wrec.ksz <= 0 { // TODO
		logger.Fatalf("bad key len %s %d %d %d", stream.fd.Name(), stream.offset, wrec.ksz, wrec.vsz)
		return stream.nextValid()
	}

	wrec.rec.Key = make([]byte, wrec.ksz)
	if _, err = io.ReadFull(stream.rbuf, wrec.rec.Key); err != nil {
		logger.Infof(err.Error())
		return
	}
	wrec.rec.Payload.Value = make([]byte, wrec.vsz)
	if _, err = io.ReadFull(stream.rbuf, wrec.rec.Payload.Value); err != nil {
		logger.Infof(err.Error())
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
		logger.Infof(err.Error())
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
	ds   *dataStore
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
		logger.Infof(err.Error(), n)
		return err
	}
	if n, err := wbuf.Write(wrec.rec.Key); err != nil {
		logger.Infof(err.Error(), n)
		return err
	}
	if n, err := wbuf.Write(wrec.rec.Payload.Value); err != nil {
		logger.Infof(err.Error(), n)
		return err
	}
	npad := sizeall - size
	if dopadding && npad != 0 {
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
