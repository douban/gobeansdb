package store

import (
	"cmem"
	"fmt"
	"quicklz"
	"strconv"

	"github.com/spaolacci/murmur3"
)

const (
	MAX_KEY_LEN          = 250
	FLAG_COMPRESS        = 0x00010000
	FLAG_CLIENT_COMPRESS = 0x00000010
	COMPRESS_RATIO_LIMIT = 0.7
	PADDING              = 256
)

func murmur(data []byte) (h uint32) {
	hasher := murmur3.New32()
	hasher.Write(data)
	return hasher.Sum32()
}

func fnv1a(data []byte) (h uint32) {
	PRIME := uint32(0x01000193)
	h = 0x811c9dc5
	for _, b := range data {
		h ^= uint32(int8(b))
		h = (h * PRIME)
	}
	return h
}

func getKeyHash(key []byte) uint64 {
	return (uint64(fnv1a(key)) << 32) | uint64(murmur(key))
}

type Meta struct {
	TS        uint32
	Flag      uint32
	Ver       int32
	ValueHash uint16 // mem only, computed only when needed
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

type hintItem struct {
	keyhash uint64
	pos     uint32
	ver     int32
	vhash   uint16
	key     string
}

func newHintItem(khash uint64, ver int32, vhash uint16, pos Position, key string) *hintItem {
	return &hintItem{khash, pos.encode(), ver, vhash, key}
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
		hash += Fnv1a(value[512:1024])
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

func (r *Record) Dumps() []byte {
	return nil
}

// computed once, before being routed to a bucket
type KeyPos struct {
	KeyPathBuf [16]int
	KeyPath    []int

	// need depth
	BucketID        int
	KeyPathInBucket []int
}

type KeyInfo struct {
	KeyHash   uint64
	KeyIsPath bool
	Key       []byte
	StringKey string
	KeyPos
}

func getBucketFromKey(key string) int {
	return 0
}

func ParsePathUint64(khash uint64, buf []int) []int {
	for i := 0; i < 16; i++ {
		shift := uint32(4 * (15 - i))
		idx := int((khash >> shift)) & 0xf
		buf[i] = int(idx)
	}
	return buf
}

func ParsePathString(pathStr string, buf []int) []int {
	path := buf[:len(pathStr)]
	for i := 0; i < len(pathStr); i++ {
		idx, err := strconv.ParseInt(pathStr[i:i+1], 16, 0)
		if err != nil {
			return nil
		}
		path[i] = int(idx)
	}
	return path
}

func checkKey(key []byte) error {
	if len(key) > MAX_KEY_LEN {
		return fmt.Errorf("key too long: %s", string(key))
	}
	return nil
}

func NewKeyInfoFromBytes(key []byte, keyhash uint64, keyIsPath bool) (ki *KeyInfo) {
	ki = &KeyInfo{
		KeyIsPath: keyIsPath,
		Key:       key,
		StringKey: string(key),
		KeyHash:   keyhash,
	}
	ki.Prepare()
	return
}

func (ki *KeyInfo) getKeyHash() {
	v := ki.KeyPath
	shift := uint(60)
	ki.KeyHash = 0
	for i := 0; i < len(v); i++ {
		ki.KeyHash |= (uint64(v[i]) << shift)
		shift -= 4
	}
}

func (ki *KeyInfo) Prepare() {
	if ki.KeyIsPath {
		ki.KeyPath = ParsePathString(ki.StringKey, ki.KeyPathBuf[:16])
		ki.getKeyHash()
		if len(ki.KeyPath) < config.TreeDepth {
			ki.BucketID = -1
			return
		}
	} else {

		ki.KeyPath = ParsePathUint64(ki.KeyHash, ki.KeyPathBuf[:16])
	}
	for _, v := range ki.KeyPath[:config.TreeDepth] {
		ki.BucketID <<= 4
		ki.BucketID += v
	}

	return
}

func isSamePath(x, y []int, n int) bool {
	for i := 0; i < n; i++ {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}
