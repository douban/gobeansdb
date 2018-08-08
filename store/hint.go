package store

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/douban/gobeansdb/utils"
)

const (
	SecsBeforeDumpDefault = int64(5)
	HintStateIdle         = 0
)

const (
	HintStateDump = 1 << iota
	HintStateMerge
	HintStateGC
)

var (
	SecsBeforeDump = SecsBeforeDumpDefault // may change in test
)

func idToStr(id int) string {
	if id < 0 {
		return "*"
	}
	return fmt.Sprintf("%03d", id)
}

func parseChunkIDFromName(name string) (int, error) {
	return strconv.Atoi(name[:3])
}

func parseSplitIDFromName(name string) (int, error) {
	return strconv.Atoi(name[4:7])
}

func parseIDFromPath(path string) (id HintID, ok bool) {
	return parseIDFromName(filepath.Base(path))
}

func parseIDFromName(name string) (id HintID, ok bool) {
	ck, err1 := parseChunkIDFromName(name)
	sp, err2 := parseSplitIDFromName(name)
	if err1 == nil && err2 == nil && ck < MAX_NUM_CHUNK && sp < MAX_NUM_SPLIT {
		return HintID{ck, sp}, true
	}
	return
}

func getIndexPath(home string, chunkID, splitID int, suffix string) string {
	return fmt.Sprintf("%s/%s.%s.idx.%s", home, idToStr(chunkID), idToStr(splitID), suffix)
}

func (h *hintMgr) getPath(chunkID, splitID int, merged bool) (path string) {
	suffix := "s"
	if merged {
		suffix = "m"
	}
	return getIndexPath(h.home, chunkID, splitID, suffix)
}

type HintID struct {
	Chunk int
	Split int
}

func (id *HintID) isLarger(ck, sp int) bool {
	return (ck > id.Chunk) || (ck == id.Chunk && sp >= id.Split)
}

func (id *HintID) setIfLarger(ck, sp int) (larger bool) {
	larger = id.isLarger(ck, sp)
	if larger {
		id.Chunk = ck
		id.Split = sp
	}
	return
}

type HintStatus struct {
	NumRead int
	MaxRead int
	MaxTime time.Duration
}

type HintBuffer struct {
	maxoffset  uint32
	index      map[uint64]int
	collisions map[uint64]map[string]int
	items      []*HintItem
	num        int
}

type hintSplit struct {
	buf  *HintBuffer
	file *hintFileIndex
}

func NewHintBuffer() *HintBuffer {
	buf := &HintBuffer{}
	buf.index = make(map[uint64]int)
	buf.collisions = make(map[uint64]map[string]int)
	return buf
}

func newhintSplit() *hintSplit {
	return &hintSplit{NewHintBuffer(), nil}
}

func (h *HintBuffer) SetMaxOffset(offset uint32) {
	h.maxoffset = offset
}

func (h *HintBuffer) Set(it *HintItem, recSize uint32) bool {
	if len(h.index) == 0 {
		h.items = make([]*HintItem, Conf.SplitCap)
	}

	idx, found := h.index[it.Keyhash]
	iscollision := false
	if found && it.Key != h.items[idx].Key {
		iscollision = true
		var keys map[string]int
		keys, found = h.collisions[it.Keyhash]
		if found {
			idx, found = keys[it.Key]
		} else {
			keys = make(map[string]int)
			keys[h.items[idx].Key] = idx
			h.collisions[it.Keyhash] = keys
		}
	}
	if !found {
		idx = h.num
		if idx >= len(h.items) {
			if it.Pos.Offset > h.maxoffset {
				h.maxoffset = it.Pos.Offset
			}
			return false
		}
		h.num += 1
	}
	h.items[idx] = it
	h.index[it.Keyhash] = idx
	if iscollision {
		h.collisions[it.Keyhash][it.Key] = idx
	}

	end := it.Pos.Offset + recSize
	if end > h.maxoffset {
		h.maxoffset = end
	}
	return true
}

func (h *HintBuffer) Get(keyhash uint64, key string) (it *HintItem, iscollision bool) {
	idx, found := h.index[keyhash]
	if found {
		if key != h.items[idx].Key {
			iscollision = true
			var keys map[string]int
			keys, found = h.collisions[keyhash]
			if found {
				idx, found = keys[key]
			}
		}
	}
	if found {
		it = h.items[idx]
	}
	return
}

func (h *HintBuffer) Dump(path string) (index *hintFileIndex, err error) {
	n := h.num
	arr := make([]int, n)
	for i := 0; i < n; i++ {
		arr[i] = i
	}
	sort.Sort(&byKeyHash{arr, h.items})
	w, err := newHintFileWriter(path, h.maxoffset, 1<<20)
	if err != nil {
		return
	}
	for _, idx := range arr {
		err = w.writeItem(h.items[idx])
		if err != nil {
			return
		}
	}
	w.close()
	index = &hintFileIndex{
		w.index.toIndex(),
		path,
		hintFileMeta{
			datasize: h.maxoffset,
			numKey:   n,
		},
	}
	return
}

func (h *hintSplit) needDump() bool {
	return h.file == nil && h.buf.num > 0
}

type hintChunk struct {
	sync.Mutex
	id       int
	fileLock sync.RWMutex
	splits   []*hintSplit

	// set to 0 : 1. loaded 2. before merge
	lastTS int64
}

func newHintChunk(id int) *hintChunk {
	ck := &hintChunk{id: id}
	ck.rotate()
	return ck
}

func (chunk *hintChunk) rotate() *hintSplit {
	sp := newhintSplit()
	n := len(chunk.splits)
	chunk.splits = append(chunk.splits, sp)
	if n > 0 {
		logger.Infof("hint rotate split to %d, chunk %d", n, chunk.id)
	}

	return sp
}

func (chunk *hintChunk) setItem(it *HintItem, recSize uint32) (rotated bool) {
	chunk.Lock()
	l := len(chunk.splits)
	sp := chunk.splits[l-1]
	if !sp.buf.Set(it, recSize) {
		chunk.rotate().buf.Set(it, recSize)
		rotated = true
	}
	chunk.lastTS = time.Now().Unix()
	chunk.Unlock()
	return
}

func (chunk *hintChunk) getMemOnly(keyhash uint64, key string) (it *HintItem, sp int) {
	chunk.Lock()
	defer chunk.Unlock()
	for sp = len(chunk.splits) - 1; sp >= 0; sp-- {
		split := chunk.splits[sp]
		if split.buf != nil {
			if it, _ = split.buf.Get(keyhash, key); it != nil {
				return
			}
		} else {
			return
		}
	}
	return
}

func (chunk *hintChunk) get(keyhash uint64, key string, memOnly bool) (it *HintItem, err error) {
	it, split := chunk.getMemOnly(keyhash, key)
	if it != nil {
		logger.Infof("hint get %016x %s, hit buffer (%d, %d)", keyhash, key, chunk.id, split)
		return
	}
	if memOnly {
		return
	}
	for i := split; i >= 0; i-- {
		split := chunk.splits[i]
		file := split.file
		if file == nil {
			logger.Errorf("lose hint split (%d, %d)", chunk.id, i)
			continue
		}
		it, err = split.file.get(keyhash, key)
		if err != nil {
			logger.Warnf("%v", err.Error())
			return
		} else if it != nil {
			logger.Infof("hint get %016x %s, hit file (%d, %d)", keyhash, key, chunk.id, i)
			return
		}
	}
	return
}

// only check it for the last split of each chunk
func (chunk *hintChunk) silenceTime() int64 {
	return chunk.lastTS + SecsBeforeDump - time.Now().Unix()
}

type hintMgr struct {
	bucketID int
	home     string

	sync.Mutex // protect maxChunkID
	maxChunkID int

	chunks [MAX_NUM_CHUNK]*hintChunk

	maxDumpedHintID HintID

	dumpLock           sync.Mutex
	mergeLock          sync.Mutex
	maxDumpableChunkID int
	merged             *hintFileIndex
	state              int

	collisions *CollisionTable
}

func newHintMgr(bucketID int, home string) *hintMgr {
	hm := &hintMgr{bucketID: bucketID, home: home}
	for i := 0; i < MAX_NUM_CHUNK; i++ {
		hm.chunks[i] = newHintChunk(i)
	}
	hm.maxDumpableChunkID = MAX_NUM_CHUNK - 1

	hm.collisions = newCollisionTable()
	return hm
}

type byKeyHash struct {
	idx  []int
	data []*HintItem
}

func (by byKeyHash) Len() int      { return len(by.idx) }
func (by byKeyHash) Swap(i, j int) { by.idx[i], by.idx[j] = by.idx[j], by.idx[i] }
func (by byKeyHash) Less(i, j int) bool {
	a := by.data[by.idx[i]]
	b := by.data[by.idx[j]]
	if a.Keyhash < b.Keyhash {
		return true
	} else if a.Keyhash > b.Keyhash {
		return false
	} else {
		return a.Key < b.Key
	}
	return false
}

func (h *hintMgr) dump(chunkID, splitID int) (err error) {
	ck := h.chunks[chunkID]
	sp := ck.splits[splitID]

	ck.Unlock()
	defer ck.Lock()

	path := h.getPath(chunkID, splitID, false)
	logger.Infof("dump %s", path)
	sp.file, err = sp.buf.Dump(path)
	if err == nil {
		h.maxDumpedHintID.setIfLarger(chunkID, splitID)
	}
	sp.buf = nil
	return nil
}

func (h *hintMgr) trydump(chunkID int, dumplast bool) (silence int64) {
	ck := h.chunks[chunkID]
	ck.Lock()
	defer ck.Unlock()
	splits := ck.splits
	l := len(splits)

	// dump old splits
	j := 0
	for ; j < l-1; j++ {
		if splits[j].needDump() {
			logger.Infof("dump old (%d, %d, %d)", h.bucketID, chunkID, j)
			h.dump(chunkID, j)
		}
	}

	if !dumplast && chunkID == h.maxChunkID {
		return
	}

	if ck.lastTS == 0 {
		return
	}
	s := ck.silenceTime()
	if dumplast || s <= 0 {
		if splits[j].needDump() {
			ck.rotate()
			ck.lastTS = 0
			h.dump(chunkID, j)
			silence = 0
		}
	} else {
		silence = s
	}
	return
}

func (h *hintMgr) close() {
	for i := 0; i <= h.maxChunkID; i++ {
		h.trydump(i, true)
	}
}

func (h *hintMgr) dumpAndMerge(forGC bool) (maxSilence int64) {
	h.state |= HintStateDump
	h.dumpLock.Lock()
	defer func() {
		h.state &= (^HintStateDump)
		h.dumpLock.Unlock()
		if e := recover(); e != nil {
			logger.Errorf("dumpAndMerge panic(%#v), stack: %s", e, utils.GetStack(2000))
		}
	}()

	maxDumpableChunkID := h.maxDumpableChunkID
	if forGC {
		maxDumpableChunkID = MAX_NUM_CHUNK - 1
	}

	for i := 0; i <= maxDumpableChunkID; i++ {
		silence := h.trydump(i, false)
		if silence > maxSilence {
			maxSilence = silence
		}
	}

	if h.state&HintStateDump != 0 { // gcing
		return
	}
	if !(h.state&HintStateMerge != 0) && !forGC && (h.maxChunkID-h.collisions.Chunk > Conf.MergeInterval) {
		logger.Infof("start merge goroutine")
		go h.Merge(false)
	}
	return
}

func (h *hintMgr) RemoveMerged() {
	paths, _ := filepath.Glob(h.getPath(-1, -1, true))
	for _, path := range paths {
		utils.Remove(path)
	}
	h.merged = nil
}

func (h *hintMgr) Merge(forGC bool) (err error) {
	defer func() {
		if e := recover(); e != nil {
			logger.Errorf("merge panic(%#v), stack: %s", e, utils.GetStack(1000))
		}
	}()

	oldchunk := h.collisions.Chunk
	h.mergeLock.Lock()
	h.state |= HintStateMerge
	st := time.Now()
	defer func() {
		h.state &= ^HintStateMerge
		h.mergeLock.Unlock()
		logger.Infof("merged done, %#v, %s", h.collisions.HintID, time.Since(st))
	}()
	if oldchunk != h.collisions.Chunk {
		return
	}
	h.RemoveMerged()

	// TODO: check hint with datas!
	pattern := h.getPath(-1, -1, false)
	paths, err := filepath.Glob(pattern)
	sort.Sort(sort.StringSlice(paths))

	readers := make([]*hintFileReader, 0, len(paths))
	var maxid HintID
	names := make([]string, 0, len(paths))
	for _, path := range paths {
		name := filepath.Base(path)
		hid, ok := parseIDFromName(name)
		if !ok {
			logger.Errorf("bad chunk index %s", path)
		}

		names = append(names, name)
		maxid.setIfLarger(hid.Chunk, hid.Split)
		r := newHintFileReader(path, hid.Chunk, 4096)
		readers = append(readers, r)
	}
	dst := h.getPath(maxid.Chunk, maxid.Split, true)
	// since we DO NOT check collision in hint bufffer, merger even if only one idx.s
	logger.Infof("to merge %s forGC=%v from %v", dst, forGC, names)
	index, err := merge(readers, dst, h.collisions, &h.state, forGC)
	if err != nil {
		logger.Errorf("merge to %s forGC=%v fail: %v", dst, forGC, err)
		return
	}
	h.merged = index
	h.collisions.HintID = maxid
	h.dumpCollisions()
	return
}

func (h *hintMgr) set(ki *KeyInfo, meta *Meta, pos Position, recSize uint32, reason string) (rotated bool) {
	it := newHintItem(ki.KeyHash, meta.Ver, meta.ValueHash, Position{0, pos.Offset}, ki.StringKey)
	_, ok := h.collisions.get(ki.KeyHash, ki.StringKey)
	if ok {
		it2 := *it
		it2.Pos.ChunkID = pos.ChunkID
		h.collisions.compareAndSet(&it2, reason)
	}
	return h.setItem(it, pos.ChunkID, recSize)
}

func (h *hintMgr) setItem(it *HintItem, chunkID int, recSize uint32) (rotated bool) {
	rotated = h.chunks[chunkID].setItem(it, recSize)
	if rotated {
		if mergeChan != nil && chunkID >= h.maxChunkID {
			select {
			case mergeChan <- 1:
			default:
			}
		} else {
			h.trydump(chunkID, false)
		}
	}
	if chunkID > h.maxChunkID {
		h.Lock()
		if chunkID > h.maxChunkID {
			logger.Infof("hint rotate chunk %d -> %d, bucket %d", h.maxChunkID, chunkID, h.bucketID)
			h.maxChunkID = chunkID
			// chunkRotate  = true
		}
		h.Unlock()
	}
	return
}

func (h *hintMgr) forceRotateSplit() {
	h.Lock()
	ck := h.chunks[h.maxChunkID]
	ck.Lock()
	ck.rotate()
	ck.Unlock()
	h.Unlock()
}

func (h *hintMgr) get(keyhash uint64, key string) (meta Meta, pos Position, err error) {
	var it *HintItem
	it, pos.ChunkID, err = h.getItem(keyhash, key, false)
	if err != nil {
		return
	}
	if it == nil {
		err = fmt.Errorf("not found")
		return
	}
	meta.ValueHash = it.Vhash
	meta.Ver = it.Ver
	pos.Offset = it.Pos.Offset
	return
}

func (h *hintMgr) getItem(keyhash uint64, key string, memOnly bool) (it *HintItem, chunkID int, err error) {
	h.mergeLock.Lock()
	merged := h.merged
	h.mergeLock.Unlock()
	for i := h.maxChunkID; i >= 0; i-- {
		if !memOnly && merged != nil && (i <= h.collisions.Chunk) {
			it, err = merged.get(keyhash, key)
			if err != nil {
				return
			} else if it != nil {
				logger.Infof("hint get hit merged %#v", it)
				chunkID = it.Pos.ChunkID
				it.Pos.ChunkID = 0
			}
			return
		}

		it, err = h.chunks[i].get(keyhash, key, memOnly)
		if err != nil {
			return
		} else if it != nil {
			chunkID = i
			return
		}
	}
	return
}

func (chunk *hintChunk) getItemCollision(keyhash uint64, key string) (it *HintItem, ChunkID int, collision, stop bool) {
	chunk.Lock()
	defer chunk.Unlock()
	for sp := len(chunk.splits) - 1; sp >= 0; sp-- {
		split := chunk.splits[sp]
		if split.buf == nil {
			stop = true
			return
		} else {
			if it, collision = split.buf.Get(keyhash, key); it != nil {
				ChunkID = chunk.id
				return
			}
		}
	}
	return
}

func (h *hintMgr) getItemCollision(keyhash uint64, key string) (it *HintItem, ChunkID int, collision bool) {
	for ck := h.maxChunkID; ck >= 0; ck-- {
		var stop bool
		it, ChunkID, collision, stop = h.chunks[ck].getItemCollision(keyhash, key)
		if collision || stop {
			return
		}
	}
	return
}

func (h *hintMgr) RemoveHintfilesByChunk(chunkID int) {
	pattern := h.getPath(chunkID, -1, false)
	paths, _ := filepath.Glob(pattern)
	for _, p := range paths {
		utils.Remove(p)
	}
}

func (hm *hintMgr) findValidPaths(chunkID int) (hints []string) {
	pattern := hm.getPath(chunkID, -1, false)
	paths, _ := filepath.Glob(pattern)
	if len(paths) == 0 {
		return
	}

	sort.Sort(sort.StringSlice(paths))
	n := 0
	for _, path := range paths {
		name := filepath.Base(path)
		sid, err := strconv.Atoi(name[4:7])
		if err != nil {
			logger.Errorf("find bad hint: hint_path=%s", path)
			utils.Remove(path)
		} else if sid != n {
			logger.Errorf("find bad hint: hint_path=%s, expect_split_id=%d, got_split_id=%d", paths, n, sid)
			utils.Remove(path)
		} else {
			hints = append(hints, path)
			n++
		}
	}
	return
}

func (hm *hintMgr) loadHintsByChunk(chunkID int) (datasize uint32) {
	paths0 := hm.findValidPaths(chunkID)
	l := len(paths0)
	if l == 0 {
		return
	}
	ck := hm.chunks[chunkID]
	var err error
	for _, p := range paths0 {
		if err != nil {
			logger.Errorf("a failure of loading hint happend before: curr_hint_path=%s", p)
			utils.Remove(p)
			continue
		}
		sp := &hintSplit{}
		sp.file, err = loadHintIndex(p)
		if err != nil {
			logger.Errorf("fail to load hint: hintpath=%s", p)
			utils.Remove(p)
		} else {
			logger.Infof("load hint %s datasize = %d", p, sp.file.datasize)
			if sp.file.datasize < datasize {
				logger.Errorf("later hint has smaller datasize %s %d < %d", p, sp.file.datasize, datasize)
			} else {
				datasize = sp.file.datasize
			}
			l := len(ck.splits)

			bufsp := ck.splits[l-1]
			ck.splits[l-1] = sp
			ck.splits = append(ck.splits, bufsp)
		}
	}
	if len(ck.splits) < 2 {
		return 0
	}
	return
}

func (h *hintMgr) ClearChunk(chunkID int) {
	h.chunks[chunkID] = newHintChunk(chunkID)
	h.RemoveHintfilesByChunk(chunkID)
}

// e.g. get A
// merged | hint buffer
// A B    |              =>  it, true
// A      | B            => nil, true
//        | A B          =>  it, true
//        | A            =>  it, false
// A/B    |              => nil, false
//        | B            => nil, true // should not happen when used in gc
func (h *hintMgr) getCollisionGC(ki *KeyInfo) (it *HintItem, ChunkID int, collision bool) {
	it, collision = h.collisions.get(ki.KeyHash, ki.StringKey)
	if !collision {
		// only in mem, in new hints buffers after gc begin
		it, ChunkID, collision = h.getItemCollision(ki.KeyHash, ki.StringKey)
	} else {
		ChunkID = it.Pos.ChunkID
	}
	return
}

func (h *hintMgr) getCollisionPath() string {
	return fmt.Sprintf("%s/collision.yaml", h.home)
}

func (h *hintMgr) dumpCollisions() {
	h.collisions.dump(h.getCollisionPath())
}

func (h *hintMgr) loadCollisions() {
	h.collisions.load(h.getCollisionPath())
}
