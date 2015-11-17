package store

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
	"utils"
)

const (
	SecsBeforeDumpDefault = int64(10)
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
	if err1 == nil && err2 == nil && ck < 256 && sp < 256 {
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

type hintBuffer struct {
	maxoffset uint32
	expsize   int
	keys      map[string]int
	keyhashs  map[uint64]bool
	array     []*HintItem
}

type hintSplit struct {
	buf  *hintBuffer
	file *hintFileIndex
}

func newHintBuffer() *hintBuffer {
	buf := &hintBuffer{}
	buf.keys = make(map[string]int)
	buf.keyhashs = make(map[uint64]bool)
	return buf
}

func newHintSplit() *hintSplit {
	return &hintSplit{newHintBuffer(), nil}
}

func (h *hintBuffer) set(it *HintItem, recSize uint32) bool {
	if len(h.keys) == 0 {
		h.array = make([]*HintItem, conf.SplitCap)
	}

	idx, found := h.keys[it.Key]
	if !found {
		h.expsize += len(it.Key) + 8*4 // meta and at least 4 pointers
		idx = len(h.keys)
		if idx >= len(h.array) {
			if it.Pos > h.maxoffset {
				h.maxoffset = it.Pos
			}
			return false
		}
		h.keys[it.Key] = idx
	}
	h.array[idx] = it
	end := it.Pos + recSize
	if end > h.maxoffset {
		h.maxoffset = end
	}
	return true
}

func (h *hintBuffer) get(key string) *HintItem {
	idx, found := h.keys[key]
	if found {
		return h.array[idx]
	}
	return nil
}

func (h *hintBuffer) dump(path string) (index *hintFileIndex, err error) {
	n := len(h.keys)
	arr := make([]int, n)
	for i := 0; i < n; i++ {
		arr[i] = i
	}
	sort.Sort(&byKeyHash{arr, h.array})
	w, err := newHintFileWriter(path, h.maxoffset, 1<<20)
	if err != nil {
		return
	}
	for _, idx := range arr {
		err = w.writeItem(h.array[idx])
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
	return h.file == nil && h.buf.keys != nil && len(h.buf.keys) > 0
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
	sp := newHintSplit()
	n := len(chunk.splits)
	chunk.splits = append(chunk.splits, sp)
	if n > 1 {
		logger.Infof("hint rotate split to %d, chunk %d", n, chunk.id)

	}

	return sp
}

func (chunk *hintChunk) set(it *HintItem, recSize uint32) (rotated bool) {
	chunk.Lock()
	l := len(chunk.splits)
	sp := chunk.splits[l-1]
	if !sp.buf.set(it, recSize) {
		chunk.rotate().buf.set(it, recSize)
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
			if it = split.buf.get(key); it != nil {
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
	dumpAndMergeState  int
	mergeing           bool

	collisions *CollisionTable
}

func newHintMgr(bucketID int, home string) *hintMgr {
	hm := &hintMgr{bucketID: bucketID, home: home}
	for i := 0; i < MAX_NUM_CHUNK; i++ {
		hm.chunks[i] = newHintChunk(i)
	}
	hm.maxDumpableChunkID = MAX_CHUNK_ID

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
	return by.data[by.idx[i]].Keyhash < by.data[by.idx[j]].Keyhash
}

func (h *hintMgr) dump(chunkID, splitID int) (err error) {
	ck := h.chunks[chunkID]
	sp := ck.splits[splitID]

	ck.Unlock()
	defer ck.Lock()

	path := h.getPath(chunkID, splitID, false)
	logger.Warnf("dump %s", path)
	sp.file, err = sp.buf.dump(path)
	if err == nil {
		h.maxDumpedHintID.setIfLarger(chunkID, splitID)
	}
	sp.buf = nil
	return nil
}

func (h *hintMgr) trydump(chunkID int, force bool) (silence int64) {
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

	if !force && chunkID == h.maxChunkID {
		return
	}

	if ck.lastTS == 0 {
		return
	}
	s := ck.silenceTime()
	if force || s <= 0 {
		if splits[j].needDump() {
			ck.rotate()
			ck.lastTS = 0
		}
		h.dump(chunkID, j)
		silence = 0
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

func (h *hintMgr) dumpAndMerge(force bool) (maxSilence int64) {
	defer func() {
		if e := recover(); e != nil {
			logger.Errorf("dumpAndMerge panic(%#v), stack: %s", e, utils.GetStack(1000))
		}
	}()

	h.dumpAndMergeState = HintStatetWorking
	h.dumpLock.Lock()
	defer func() {
		h.dumpAndMergeState = HintStateIdle
		h.dumpLock.Unlock()
	}()

	maxDumpableChunkID := h.maxDumpableChunkID
	if force {
		maxDumpableChunkID = MAX_CHUNK_ID
	}

	for i := 0; i <= maxDumpableChunkID; i++ {
		silence := h.trydump(i, false)
		if silence > maxSilence {
			maxSilence = silence
		}
	}

	if h.maxDumpableChunkID < MAX_CHUNK_ID { // gcing
		return
	}
	if !h.mergeing && (h.maxChunkID-h.collisions.Chunk > conf.MergeInterval) {
		logger.Infof("start merge goroutine")
		go h.Merge()
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

func (h *hintMgr) Merge() (err error) {
	defer func() {
		if e := recover(); e != nil {
			logger.Errorf("merge panic(%#v), stack: %s", e, utils.GetStack(1000))
		}
	}()

	oldchunk := h.collisions.Chunk
	h.mergeLock.Lock()
	h.mergeing = true
	st := time.Now()
	defer func() {
		h.mergeing = false
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
	logger.Infof("to merge %s from %v", dst, names)
	index, err := merge(readers, dst, h.collisions, &h.dumpAndMergeState)
	if err != nil {
		logger.Errorf("merge to %s fail: %s", dst, err.Error())
		return
	}
	h.merged = index
	h.collisions.HintID = maxid
	return
}

func (h *hintMgr) set(ki *KeyInfo, meta *Meta, pos Position, recSize uint32) {
	it := newHintItem(ki.KeyHash, meta.Ver, meta.ValueHash, Position{0, pos.Offset}, ki.StringKey)
	_, ok := h.collisions.get(ki.KeyHash, ki.StringKey)
	if ok {
		it2 := *it
		it2.Pos |= uint32(pos.ChunkID)
		h.collisions.compareAndSet(&it2)
	}
	h.setItem(it, pos.ChunkID, recSize)
}

func (h *hintMgr) setItem(it *HintItem, chunkID int, recSize uint32) {
	rotated := h.chunks[chunkID].set(it, recSize)
	if rotated {
		if mergeChan != nil {
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
	pos.Offset = it.Pos
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
				chunkID = int(it.Pos & 0xff)
				it.Pos -= uint32(chunkID)
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

func (chunk *hintChunk) getItemCollision(keyhash uint64, key string) (it *HintItem, collision, stop bool) {
	chunk.Lock()
	defer chunk.Unlock()
	for sp := len(chunk.splits) - 1; sp >= 0; sp-- {
		split := chunk.splits[sp]
		if split.buf == nil {
			stop = true
			return
		} else {
			if it = split.buf.get(key); it != nil {
				collision = true
				it.Pos |= uint32(chunk.id)
				return
			} else if !collision {
				_, collision = split.buf.keyhashs[keyhash]
			}
		}
	}
	return
}

func (h *hintMgr) getItemCollision(keyhash uint64, key string) (it *HintItem, collision bool) {
	for ck := h.maxChunkID; ck >= 0; ck-- {
		var stop bool
		it, collision, stop = h.chunks[ck].getItemCollision(keyhash, key)
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
			l := len(ck.splits)
			bufsp := ck.splits[l-1]
			ck.splits[l-1] = sp
			ck.splits = append(ck.splits, bufsp)
		}
	}
	return ck.splits[len(ck.splits)-2].file.datasize
}
func (h *hintMgr) ClearChunks(min, max int) {
	for i := min; i <= max; i++ {
		h.chunks[i] = newHintChunk(i)
		h.RemoveHintfilesByChunk(i)
	}
}
