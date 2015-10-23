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

type HintStatus struct {
	NumRead int
	MaxRead int
	MaxTime time.Duration
}

type hintBuffer struct {
	maxoffset uint32
	expsize   int
	keys      map[string]int
	array     []*hintItem
}

type hintSplit struct {
	buf  *hintBuffer
	file *hintFileIndex
}

func newHintBuffer() *hintBuffer {
	buf := &hintBuffer{}
	buf.keys = make(map[string]int)
	buf.array = make([]*hintItem, hintConfig.SplitCount)
	return buf
}

func newHintSplit() *hintSplit {
	return &hintSplit{newHintBuffer(), nil}
}

func (h *hintBuffer) set(it *hintItem) bool {
	idx, found := h.keys[it.key]
	if !found {
		h.expsize += len(it.key) + 8*4 // meta and at least 4 pointers
		idx = len(h.keys)
		if idx >= len(h.array) {
			if it.pos > h.maxoffset {
				h.maxoffset = it.pos
			}
			return false
		}
		h.keys[it.key] = idx
	}
	h.array[idx] = it
	if it.pos > h.maxoffset {
		h.maxoffset = it.pos
	}
	return true
}

func (h *hintBuffer) get(key string) *hintItem {
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
	fileLock sync.RWMutex
	splits   []*hintSplit

	// set to 0 : 1. loaded 2. before merge
	lastTS int64
}

func newHintChunk() *hintChunk {
	ck := &hintChunk{}
	ck.rotate()
	return ck
}

func (chunk *hintChunk) rotate() *hintSplit {
	sp := newHintSplit()
	chunk.splits = append(chunk.splits, sp)
	return sp
}

func (chunk *hintChunk) set(it *hintItem) (rotated bool) {
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

func (chunk *hintChunk) getFiles() (paths []string) {
	for i := 0; i < len(chunk.splits); i++ {
		f := chunk.splits[i].file
		if f != nil {
			paths = append(paths, f.path)
		}
	}
	return
}

func (chunk *hintChunk) get(keyhash uint64, key string) (it *hintItem, err error) {
	chunk.fileLock.RLock()
	defer chunk.fileLock.RUnlock()
	chunk.Lock()
	// no err while locked
	for i := len(chunk.splits) - 1; i >= 0; i-- {
		split := chunk.splits[i]
		if split.buf != nil {
			if it = split.buf.get(key); it != nil {
				chunk.Unlock()
				return
			}
		}
		file := split.file
		chunk.Unlock()
		if file == nil {
			chunk.Lock()
			continue
		}

		it, err = split.file.get(keyhash, key)
		if err != nil {
			logger.Warnf("%v", err.Error())
			return
		}
		if it != nil {
			return
		}
		chunk.Lock()
	}
	chunk.Unlock()
	return
}

func (chunk *hintChunk) silenceTime() int64 {
	return chunk.lastTS + hintConfig.SecondsBeforeDump - time.Now().Unix()
}

type hintMgr struct {
	home string

	sync.Mutex
	maxChunkID int

	chunks    [256]*hintChunk
	filesizes []uint32 // ref toto bucket.datas.filesizes

	merging   bool
	merged    *hintFileIndex
	mergedID  int
	mergeChan chan bool
}

func newHintMgr(home string) *hintMgr {
	hm := &hintMgr{home: home}
	for i := 0; i < 256; i++ {
		hm.chunks[i] = newHintChunk()
	}
	hm.filesizes = make([]uint32, 256)
	hm.maxChunkID = 0
	hm.mergedID = -1
	hm.mergeChan = make(chan bool, 256)
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
		sid, err := strconv.Atoi(path[8:])
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
	data []*hintItem
}

func (by byKeyHash) Len() int      { return len(by.idx) }
func (by byKeyHash) Swap(i, j int) { by.idx[i], by.idx[j] = by.idx[j], by.idx[i] }
func (by byKeyHash) Less(i, j int) bool {
	return by.data[by.idx[i]].keyhash < by.data[by.idx[j]].keyhash
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

func (h *hintMgr) trydump(chunkID int) (needmerge, silence bool) {
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

	if chunkID == h.maxChunkID {
		return false, false
	}
	if ck.lastTS == 0 {
		needmerge = (j > 1)
		return
	}
	s := ck.silenceTime()
	if s <= 0 {
		if splits[j].needDump() {
			ck.rotate()
			logger.Infof("dump last %d %d", chunkID, j)
			ck.lastTS = 0
		}
		h.chunks[chunkID].splits[j].buf.maxoffset = h.filesizes[chunkID]
		h.dump(chunkID, j)
		if j > 0 {
			needmerge = true
		}
	} else {
		silence = true
	}
	return
}

func (h *hintMgr) smallMerge(chunkID int) (suc bool) {
	ck := h.chunks[chunkID]
	suc = true
	ck.Lock()
	if ck.silenceTime() > 0 {
		logger.Infof("new write when try small merge")
		suc = false
		ck.Unlock()
		return
	}
	ck.lastTS = 0
	splits := h.chunks[chunkID].splits
	l := len(splits) - 1
	splits = splits[:l]
	readers := make([]*hintFileReader, 0, 20)
	for _, sp := range splits {
		r := newHintFileReader(sp.file.path, 0, 4096)
		readers = append(readers, r)
	}

	dst := h.getPath(chunkID, 0, false)
	tmp := h.getPath(chunkID+500, 999, false)
	ck.Unlock()
	logger.Infof("small merge chunck %d, num %d", chunkID, len(readers))
	index, err := merge(readers, tmp)
	if err != nil {
		logger.Errorf("fail to merge %s", tmp)
		return
	}
	index.path = dst
	files := ck.getFiles()
	ck.Lock()
	newwrite := ck.splits[l:]
	ck.splits = nil
	ck.rotate()
	ck.splits[0].file = index
	ck.splits = append(ck.splits, newwrite...)
	ck.Unlock()
	ck.fileLock.Lock() // TODO: check order
	defer ck.fileLock.Unlock()
	for _, f := range files {
		os.Remove(f)
	}
	os.Rename(tmp, dst)
	return
}

func (h *hintMgr) close() {
	for i := 0; i <= h.maxChunkID; i++ {
		h.trydump(i)
	}
}

func (h *hintMgr) dumpAndMerge() {
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("Merge Error: %#v, stack: %s", err, loghub.GetStack(1000))
		}
	}()

	tosmallmerge := make(map[int]bool)
	trybigmerge := true
	for i := 0; i <= h.maxChunkID; i++ {
		needsmall, silence := h.trydump(i)
		// logger.Infof("%d %d %v %v", i, len(h.chunks[i].splits), needsmall, silence)
		if needsmall {
			tosmallmerge[i] = true
		}
		if silence {
			trybigmerge = false
		}
	}

	// small merge
	// merge hints with same data file id
	for chunkID, _ := range tosmallmerge {
		if !h.smallMerge(chunkID) {
			trybigmerge = false
		}
	}
	if !trybigmerge {
		// TODO:
		return
	}
	//large merge
	// merge hints with diff data file id to hint.m
	// TODO: limit size of hint.m, i.e. multi hint.m
	if h.mergedID < 0 || (h.maxChunkID >= 1 && h.maxChunkID-h.mergedID > 1) {
		readers := make([]*hintFileReader, 0, 256)
		i := 0
		if h.mergedID >= 0 {
			i = h.mergedID + 1
			path := h.getPath(h.mergedID, -1, true)
			r := newHintFileReader(path, 0, 1<<20)
			readers = append(readers, r)
		}
		newMergedID := h.mergedID
		for ; i < h.maxChunkID; i++ {
			ck := h.chunks[i]
			nsp := len(ck.splits)
			if nsp <= 1 {
				continue
			} else if nsp > 2 {
				logger.Errorf("big merge find bad chunk, id = %d, #split = %d", i, nsp)
				return
			} else {
				newMergedID = i
				path := ck.splits[0].file.path
				r := newHintFileReader(path, i, 4096)
				readers = append(readers, r)
			}
		}
		if len(readers) <= 1 {
			logger.Warnf("mid=%d, cid=%d, newcid=%d, #src=%d", h.mergedID, h.maxChunkID, i, len(readers))
			return
		}
		dst := h.getPath(newMergedID, -1, true)
		logger.Infof("large merge chunck %d, num %d", newMergedID, len(readers))
		index, err := merge(readers, dst)
		if err != nil {
			return
		}
		h.merged = index
		oldmerged := h.mergedID
		h.mergedID = newMergedID
		if oldmerged > 0 {
			os.Remove(h.getPath(oldmerged, -1, true))
		}
	}
}

func (h *hintMgr) getPath(chunkID, splitID int, big bool) (path string) {
	if big {
		path = fmt.Sprintf("%03d.hint.m", chunkID)
	} else {
		if splitID < 0 {
			path = fmt.Sprintf("%03d.hint.s.*", chunkID)
		} else {
			path = fmt.Sprintf("%03d.hint.s.%d", chunkID, splitID)
		}
	}
	path = filepath.Join(h.home, path)
	return
}

// start this after old hintfile indexe loaded
// do not merge at exit
func (h *hintMgr) merger(interval time.Duration) {
	for {
		select {
		case <-h.mergeChan:
		case <-time.After(interval):
		}
		h.dumpAndMerge()
	}
}

func (h *hintMgr) set(ki *KeyInfo, meta *Meta, pos Position) {
	it := newHintItem(ki.KeyHash, meta.Ver, meta.ValueHash, Position{0, pos.Offset}, ki.StringKey)
	h.setItem(it, pos.ChunkID)
}

func (h *hintMgr) setItem(it *hintItem, chunkID int) {
	splitRotate := h.chunks[chunkID].set(it)
	if chunkID > h.maxChunkID {
		h.Lock()
		if chunkID > h.maxChunkID {
			logger.Infof("hint rotate %d -> %d", h.maxChunkID, chunkID)
			h.maxChunkID = chunkID
			// chunkRotate  = true
		}
		h.Unlock()
	}
	if splitRotate {
		h.mergeChan <- true
	}
}

func (h *hintMgr) get(keyhash uint64, key string) (meta Meta, pos Position, err error) {
	var it *hintItem
	it, pos.ChunkID, err = h.getItem(keyhash, key)
	if err != nil {
		return
	}
	if it == nil {
		err = fmt.Errorf("not found")
		return
	}
	meta.ValueHash = it.vhash
	meta.Ver = it.ver
	pos.Offset = it.pos
	return
}

func (h *hintMgr) getItem(keyhash uint64, key string) (it *hintItem, chunkID int, err error) {
	for i := h.maxChunkID; i >= 0; i-- {
		if i <= h.mergedID {
			it, err = h.merged.get(keyhash, key)
			if err == nil && it != nil {
				chunkID = int(it.pos & 0xff)
				it.pos -= uint32(chunkID)
			}
			return
		}
		if it, err = h.chunks[i].get(keyhash, key); it != nil || err != nil {
			chunkID = i
			return
		}
	}
	return
}
