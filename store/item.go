package store

import (
	"bytes"
	"fmt"

	"github.intra.douban.com/coresys/gobeansdb/cmem"
	"github.intra.douban.com/coresys/gobeansdb/quicklz"
)

const (
	FLAG_INCR            = 0x00000204
	FLAG_COMPRESS        = 0x00010000
	FLAG_CLIENT_COMPRESS = 0x00000010
	COMPRESS_RATIO_LIMIT = 0.7
	TRY_COMPRESS_SIZE    = 1024 * 10
	PADDING              = 256
)

type Meta struct {
	TS   uint32
	Flag uint32
	Ver  int32
	// computed once
	ValueHash uint16
	RecSize   uint32
	// not change, make accounting easier
	// = ksz + vsz
	AccountingSize int64
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
type HintItemMeta struct {
	Keyhash uint64
	Pos     uint32
	Ver     int32
	Vhash   uint16
}
type HintItem struct {
	HintItemMeta
	Key string
}

func newHintItem(khash uint64, ver int32, vhash uint16, pos Position, key string) *HintItem {
	return &HintItem{HintItemMeta{khash, pos.encode(), ver, vhash}, key}
}

type Payload struct {
	Meta
	cmem.CArray
}

func (p *Payload) Copy() *Payload {
	p2 := new(Payload)
	p2.Meta = p.Meta
	var ok bool
	p2.CArray, ok = p.CArray.Copy()
	if !ok {
		return nil
	}
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
	p.ValueHash = Getvhash(p.Body)
}

func (p *Payload) RawValueSize() int {
	if !p.IsCompressed() {
		return len(p.Body)
	} else {
		return quicklz.SizeCompressed(p.Body)
	}
}

func (rec *Record) TryCompress() {
	if rec.Payload.Ver < 0 {
		return
	}
	p := rec.Payload
	if p.Flag&FLAG_CLIENT_COMPRESS != 0 || p.Flag&FLAG_COMPRESS != 0 {
		return
	}

	if rec.Size() <= 256 {
		return
	}
	body := rec.Payload.Body
	try := body
	if len(body) > TRY_COMPRESS_SIZE {
		try = try[:TRY_COMPRESS_SIZE]
	}
	compressed, ok := quicklz.CCompress(try)
	if !ok {
		// because oom, just not compress it
		return
	}
	if float32(len(compressed.Body))/float32(len(try)) > COMPRESS_RATIO_LIMIT {
		compressed.Free()
		return
	}
	if len(body) > len(try) {
		compressed.Free()
		compressed, ok = quicklz.CCompress(body)
		if !ok {
			// because oom, just not compress it
			return
		}
	}
	p.CArray.Free()
	p.CArray = compressed
	p.Flag += FLAG_COMPRESS
	return
}

func (p *Payload) Decompress() (err error) {
	if p.Flag&FLAG_COMPRESS == 0 {
		return
	}
	arr, err := quicklz.CDecompressSafe(p.Body)
	if err != nil {
		logger.Errorf("decompress fail %s", err.Error())
		return
	}
	p.CArray.Free()
	p.CArray = arr
	p.Flag -= FLAG_COMPRESS
	return
}

func (p *Payload) Getvhash() uint16 {
	if p.Ver < 0 {
		return 0
	}
	if p.Flag&FLAG_COMPRESS == 0 {
		return Getvhash(p.Body)
	}
	arr, _ := quicklz.CDecompressSafe(p.Body)
	vhash := Getvhash(arr.Body)
	arr.Free()
	return vhash
}

type Position struct {
	ChunkID int
	Offset  uint32
}

func (pos *Position) encode() uint32 {
	return uint32(pos.ChunkID) | pos.Offset
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
		len(rec.Payload.Body),
		rec.Payload.Meta,
		string(rec.Key),
	)
}

func (rec *Record) Copy() *Record {
	return &Record{rec.Key, rec.Payload.Copy()}
}

// must be compressed
func (rec *Record) Sizes() (uint32, uint32) {
	recSize := uint32(24 + len(rec.Key) + len(rec.Payload.Body))
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

func isValidKVSzie(ksz, vsz uint32) bool {
	return ksz != 0 && ksz <= uint32(conf.MaxKeyLen) && vsz <= uint32(conf.BodyMax)
}

func posForCompare(pos uint32) int64 {
	return (int64(pos&0xff) << 32) | int64(pos)
}
func comparePos(oldPos, newPos uint32) int64 {
	return posForCompare(oldPos) - posForCompare(newPos)
}
