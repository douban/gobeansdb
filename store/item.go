package store

import (
	"bytes"
	"fmt"

	"net/http"

	"github.com/douban/gobeansdb/cmem"
	"github.com/douban/gobeansdb/quicklz"
	"github.com/douban/gobeansdb/utils"
)

const (
	FLAG_INCR            = 0x00000204
	FLAG_COMPRESS        = 0x00010000
	FLAG_CLIENT_COMPRESS = 0x00000010
	COMPRESS_RATIO_LIMIT = 0.7
	TRY_COMPRESS_SIZE    = 1024 * 10
	PADDING              = 256
	HEADER_SIZE          = 512
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
	req.item = HTreeItem{req.ki.KeyHash, req.Position, req.Ver, req.ValueHash}
}

type HintItemMeta struct {
	Keyhash uint64
	Pos     Position
	Ver     int32
	Vhash   uint16
}

type HTreeItem HintItemMeta

type HintItem struct {
	HintItemMeta
	Key string
}

func newHintItem(khash uint64, ver int32, vhash uint16, pos Position, key string) *HintItem {
	return &HintItem{HintItemMeta{khash, pos, ver, vhash}, key}
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

func (p *Payload) DiffSizeAfterDecompressed() int {
	if p.IsCompressed() {
		return quicklz.SizeDecompressed(p.CArray.Body) - p.CArray.Cap
	}
	return 0
}

func Getvhash(value []byte) uint16 {
	l := len(value)
	hash := uint32(l) * 97
	if l <= 1024 {
		hash += utils.Fnv1a(value)
	} else {
		hash += utils.Fnv1a(value[:512])
		hash *= 97
		hash += utils.Fnv1a(value[l-512 : l])
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

func NeedCompress(header []byte) bool {
	typeValue := http.DetectContentType(header)
	_, ok := Conf.NotCompress[typeValue]
	return !ok
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
	if !NeedCompress(try) {
		return
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

func (pos *Position) CmpKey() int64 {
	return (int64(pos.ChunkID) << 32) + int64(pos.Offset)
}

type Record struct {
	Key     []byte
	Payload *Payload
}

func (rec *Record) LogString() string {
	return fmt.Sprintf("ksz %d, vsz %d, meta %#v [%s] ",
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

func posForCompare(pos uint32) int64 {
	return (int64(pos&0xff) << 32) | int64(pos)
}
func comparePos(oldPos, newPos uint32) int64 {
	return posForCompare(oldPos) - posForCompare(newPos)
}
