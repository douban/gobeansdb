package store

import (
	"fmt"
	"loghub"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const ()

type HintID struct {
	Chunk int
	Split int
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
	array     []*HintItem
}

type hintSplit struct {
	buf  *hintBuffer
	file *hintFileIndex
}

func newHintBuffer() *hintBuffer {
	buf := &hintBuffer{}
	buf.keys = make(map[string]int)
	buf.array = make([]*HintItem, hintConfig.SplitCount)
	return buf
}

func newHintSplit() *hintSplit {
	return &hintSplit{newHintBuffer(), nil}
}

func (h *hintBuffer) set(it *HintItem) bool {
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
	if it.Pos > h.maxoffset {
		h.maxoffset = it.Pos
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
	return &hintFileIndex{w.index.toIndex(), path}, nil
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
	chunk.splits = append(chunk.splits, sp)
	return sp
}

func (chunk *hintChunk) set(it *HintItem) (rotated bool) {
	chunk.Lock()
	l := len(chunk.splits)
	sp := chunk.splits[l-1]
	if !sp.buf.set(it) {
		chunk.rotate().buf.set(it)
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
		logger.Debugf("hint get %016x %s, hit buffer (%d, %d)", keyhash, key, chunk.id, split)
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
			logger.Debugf("hint get %016x %s, hit file (%d, %d)", keyhash, key, chunk.id, i)
			return
		}
	}
	return
}

// only check it for the last split of each chunk
func (chunk *hintChunk) silenceTime() int64 {
	return chunk.lastTS + hintConfig.SecondsBeforeDump - time.Now().Unix()
}

type hintMgr struct {
	home string

	sync.Mutex // protect maxChunkID
	maxChunkID int

	chunks [MAX_NUM_CHUNK]*hintChunk

	filesizes []uint32 // ref toto bucket.datas.filesizes

	maxDumpedHintID HintID

	mergeLock          sync.Mutex
	maxDumpableChunkID int
	merged             *hintFileIndex
	mergedHintID       HintID // merged file may not exist
	dumpAndMergeState  int
}

func newHintMgr(home string) *hintMgr {
	hm := &hintMgr{home: home}
	for i := 0; i < MAX_NUM_CHUNK; i++ {
		hm.chunks[i] = newHintChunk(i)
	}
	hm.filesizes = make([]uint32, MAX_NUM_CHUNK)
	hm.maxDumpableChunkID = MAX_CHUNK_ID
	return hm
}

func (hm *hintMgr) findChunk(chunkID int, remove bool) (hints []string) {
	pattern := hm.getPath(chunkID, -1, false)
	paths, _ := filepath.Glob(pattern)
	if len(paths) == 0 {
		return
	}

	if remove {
		for _, p := range paths {
			os.Remove(p)
		}
		return
	}
	sort.Sort(sort.StringSlice(paths))
	n := 0
	for _, path := range paths {
		name := filepath.Base(path)
		sid, err := strconv.Atoi(name[11:])
		if err != nil {
			logger.Errorf("bad hint path %s", path)
		} else if sid != n {
			logger.Errorf("bad hints %s", paths)
			os.Remove(path)
		} else {
			hints = append(hints, path)
			n++
		}
	}
	return
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
			logger.Infof("dump old %d %d", chunkID, j)
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
		h.chunks[chunkID].splits[j].buf.maxoffset = h.filesizes[chunkID]
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
	h.mergeLock.Lock()
	h.dumpAndMergeState = HintStatetWorking
	defer func() {
		h.dumpAndMergeState = HintStateIdle
		h.mergeLock.Unlock()
		if err := recover(); err != nil {
			logger.Errorf("Merge Error: %#v, stack: %s", err, loghub.GetStack(1000))
		}
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

	if h.maxDumpableChunkID < MAX_CHUNK_ID {
		return
	}
	if h.merged != nil {
		if h.maxChunkID-h.mergedHintID.Chunk > 1 {
			paths, _ := filepath.Glob(h.getPath(-1, -1, true))
			for _, path := range paths {
				os.Remove(path)
			}
			h.merged = nil
			h.mergedHintID.Chunk = -1
		}
	}
	if h.maxChunkID > 0 && h.merged == nil {
		h.Merge()
	}
	return
}

func (h *hintMgr) Merge() (err error) {
	// TODO: check hint with datas!
	pattern := h.getPath(-1, -1, false)
	paths, err := filepath.Glob(pattern)
	sort.Sort(sort.StringSlice(paths))

	readers := make([]*hintFileReader, 0, len(paths))
	maxChunkID := 0
	maxSplitID := 0

	names := make([]string, 0, len(paths))
	for _, path := range paths {
		name := filepath.Base(path)
		chunkID, err := strconv.Atoi(name[:3])
		if err != nil {
			logger.Errorf("bad chunk index %s", path)
			continue
		}
		splitID, err := strconv.Atoi(name[4:7])
		if err != nil {
			logger.Errorf("bad chunk index %s", path)
			continue
		}
		names = append(names, name)
		if chunkID > maxChunkID {
			maxChunkID = chunkID
		} else if chunkID == maxChunkID {
			maxSplitID = splitID
		}
		r := newHintFileReader(path, chunkID, 4096)
		readers = append(readers, r)
	}

	dst := h.getPath(maxChunkID, maxSplitID, true)
	logger.Infof("to merge %s from %v", dst, names)
	index, err := merge(readers, dst, &h.dumpAndMergeState)
	if err != nil {
		logger.Errorf("merge to %s fail: %s", dst, err.Error())
		return
	}
	h.merged = index
	h.mergedHintID.Chunk = maxChunkID
	h.mergedHintID.Split = maxChunkID
	return
}

func idToStr(id int) string {
	if id < 0 {
		return "*"
	}
	return fmt.Sprintf("%03d", id)
}

func (h *hintMgr) getPath(chunkID, splitID int, big bool) (path string) {
	suffix := "s"
	if big {
		suffix = "m"
	}
	return fmt.Sprintf("%s/%s.%s.idx.%s", h.home, idToStr(chunkID), idToStr(splitID), suffix)
}

func (h *hintMgr) set(ki *KeyInfo, meta *Meta, pos Position) {
	it := newHintItem(ki.KeyHash, meta.Ver, meta.ValueHash, Position{0, pos.Offset}, ki.StringKey)
	h.setItem(it, pos.ChunkID)
}

func (h *hintMgr) setItem(it *HintItem, chunkID int) {
	h.chunks[chunkID].set(it)
	if chunkID > h.maxChunkID {
		h.Lock()
		if chunkID > h.maxChunkID {
			logger.Infof("hint rotate %d -> %d", h.maxChunkID, chunkID)
			h.maxChunkID = chunkID
			// chunkRotate  = true
		}
		h.Unlock()
	}
}

func (h *hintMgr) forceRotateSplit() {
	h.Lock()
	h.chunks[h.maxChunkID].Lock()
	h.chunks[h.maxChunkID].rotate()
	h.chunks[h.maxChunkID].Unlock()
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
		if !memOnly && merged != nil && (i <= h.mergedHintID.Chunk) {
			it, err = merged.get(keyhash, key)
			if err != nil {
				return
			} else if it != nil {
				logger.Debugf("hint get hit merged %#v", it)
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

func (h *hintMgr) ClearChunks(min, max int) {
	for i := min; i <= max; i++ {
		h.chunks[i] = newHintChunk(i)
		pattern := h.getPath(i, -1, false)
		paths, _ := filepath.Glob(pattern)
		for _, p := range paths {
			os.Remove(p)
		}
	}
}
