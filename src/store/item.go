package store

import (
	"bytes"
	"cmem"
	"fmt"
	"quicklz"
)

const (
	FLAG_INCR            = 0x00000204
	FLAG_COMPRESS        = 0x00010000
	FLAG_CLIENT_COMPRESS = 0x00000010
	COMPRESS_RATIO_LIMIT = 0.7
	PADDING              = 256
)

type Meta struct {
	TS   uint32
	Flag uint32
	Ver  int32
	// computed once
	ValueHash uint16
	RecSize   uint32
}

type HTreeReq struct {
	ki *KeyInfo
	Meta
	Position
	item HTreeItem
}

func (req *HTreeReq) encode() {
	req.item = HTreeItem{req.ki.KeyHash, req.Position.encode(), req.Ver, req.ValueHash}
}

type HTreeItem struct {
	keyhash uint64
	pos     uint32
	ver     int32
	vhash   uint16
}

type HintItem struct {
	Keyhash uint64
	Pos     uint32
	Ver     int32
	Vhash   uint16
	Key     string
}

func newHintItem(khash uint64, ver int32, vhash uint16, pos Position, key string) *HintItem {
	return &HintItem{khash, pos.encode(), ver, vhash, key}
}

type Payload struct {
	Meta
	Value []byte
}

func (p *Payload) Copy() *Payload {
	p2 := new(Payload)
	p2.Meta = p.Meta
	p2.Value = make([]byte, len(p.Value))
	copy(p2.Value, p.Value)
	return p2
}

func (p *Payload) IsCompressed() bool {
	return (p.Flag & FLAG_COMPRESS) != 0
}

func Getvhash(value []byte) uint16 {
	l := len(value)
	hash := uint32(l) * 97
	if l <= 1024 {
		hash += Fnv1a(value)
	} else {
		hash += Fnv1a(value[:512])
		hash *= 97
		hash += Fnv1a(value[l-512 : l])
	}
	return uint16(hash)
}

func (p *Payload) CalcValueHash() {
	p.ValueHash = Getvhash(p.Value)
}

func (p *Payload) RawValueSize() int {
	if !p.IsCompressed() {
		return len(p.Value)
	} else {
		return quicklz.SizeCompressed(p.Value)
	}
}

func (rec *Record) Compress() {
	if rec.Payload.Ver < 0 {
		return
	}
	p := rec.Payload
	if p.Flag&FLAG_CLIENT_COMPRESS != 0 || p.Flag&FLAG_COMPRESS != 0 {
		return
	}
	size := rec.Size()
	if size < 256 {
		return
	}
	v := quicklz.CCompress(rec.Payload.Value)
	if float32(len(v))/float32(len(p.Value)) < COMPRESS_RATIO_LIMIT {
		cmem.Sub(cmem.TagSetData, len(p.Value)-len(v))
		p.Value = v
		p.Flag += FLAG_COMPRESS
	}
}

func (p *Payload) Decompress() (err error) {
	if p.Flag&FLAG_COMPRESS == 0 {
		return
	}

	v, err := quicklz.CDecompressSafe(p.Value)
	if err != nil {
		return
	}
	cmem.Sub(cmem.TagGetData, len(p.Value)-len(v))
	p.Value = v
	p.Flag -= FLAG_COMPRESS
	return
}

type Position struct {
	ChunkID int
	Offset  uint32
}

func (pos *Position) encode() uint32 {
	return uint32(pos.ChunkID) + pos.Offset
}

func decodePos(pos uint32) Position {
	return Position{ChunkID: int(pos & 0xff), Offset: pos & 0xffffff00}
}

type Record struct {
	Key     []byte
	Payload *Payload
}

func (rec *Record) LogString() string {
	return fmt.Sprintf("ksz %d, vsz %d %d, meta %#v [%s] ",
		len(rec.Key),
		len(rec.Payload.Value),
		rec.Payload.Meta,
		string(rec.Key),
	)
}

func (rec *Record) Copy() *Record {
	return &Record{rec.Key, rec.Payload.Copy()}
}

// must be compressed
func (rec *Record) Sizes() (uint32, uint32) {
	recSize := uint32(24 + len(rec.Key) + len(rec.Payload.Value))
	return recSize, ((recSize + 255) >> 8) << 8
}

func (rec *Record) Size() uint32 {
	_, size := rec.Sizes()
	return size
}

func (rec *Record) Dumps() []byte {
	var buf bytes.Buffer
	wrec := wrapRecord(rec)
	wrec.append(&buf, false)
	return buf.Bytes()
}
