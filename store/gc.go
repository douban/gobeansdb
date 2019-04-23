package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/douban/gobeansdb/config"
)

type GCCancelCtx struct {
	Cancel    context.CancelFunc
	ChunkChan chan int
}

type GCMgr struct {
	mu   sync.RWMutex
	stat map[int]*GCState // map[bucketID]*GCState
}

type GCState struct {
	BeginTS time.Time
	EndTS   time.Time

	// Begin and End are chunckIDs, they determine the range of GC.
	Begin int
	End   int

	// Src and Dst are chunkIDs, they are tmp variables used in gc process.
	Src int
	Dst int

	Err error
	// sum
	GCFileState
}

type GCFileState struct {
	NumBefore          int64
	NumReleased        int64
	NumReleasedDeleted int64
	SizeBefore         int64
	SizeReleased       int64
	SizeDeleted        int64
	SizeBroken         int64
	NumNotInHtree      int64
}

func (s *GCFileState) add(s2 *GCFileState) {
	s.NumBefore += s2.NumBefore
	s.NumReleased += s2.NumReleased
	s.NumReleasedDeleted += s2.NumReleasedDeleted
	s.SizeBefore += s2.SizeBefore
	s.SizeBroken += s2.SizeBroken
	s.SizeDeleted += s2.SizeDeleted
	s.SizeReleased += s2.SizeReleased
	s.NumNotInHtree += s2.NumNotInHtree
}

func (s *GCFileState) addRecord(size uint32, isNewest, isDeleted bool, sizeBroken uint32) {
	if !isNewest {
		s.NumReleased += 1
		s.SizeReleased += int64(size)
		if isDeleted {
			s.NumReleasedDeleted += 1
			s.SizeDeleted += int64(size)
		}
	}
	s.SizeReleased += int64(sizeBroken)
	s.SizeBroken += int64(sizeBroken)
	s.SizeBefore += int64(size + sizeBroken)
	s.NumBefore += 1
}

func (s *GCFileState) String() string {
	return fmt.Sprintf("%#v", s)
}

func (mgr *GCMgr) UpdateCollision(bkt *Bucket, ki *KeyInfo, oldPos, newPos Position, rec *Record) {
	// not have to (leave it to get)

	// if in ctable: update pos
	// else: decompress, get vhash and set collisions
}

func (mgr *GCMgr) UpdateHtreePos(bkt *Bucket, ki *KeyInfo, oldPos, newPos Position) {
	// TODO: should be a api of htree to be atomic
	meta, _, ok := bkt.htree.get(ki)
	if !ok {
		logger.Warnf("old key removed when updating pos bucket %d %s %#v %#v",
			bkt.ID, ki.StringKey, meta, oldPos)
		return
	}
	bkt.htree.set(ki, meta, newPos)
}

func (mgr *GCMgr) BeforeBucket(bkt *Bucket, startChunkID, endChunkID int, merge bool) {
	bkt.hints.state |= HintStateGC // will about
	for bkt.hints.state&HintStateMerge != 0 {
		logger.Infof("gc wait for merge to stop")
		time.Sleep(5 * time.Millisecond)
	}

	// dump hint and do merge, and hold all new SETs in hint buffers
	// so collision will be find either during merge or in hint buffer
	// so will not wrongly GC a collision record. e.g.:
	//   key1 and key2 have the same keyhash, key1 is set before gc, and key2 after that.
	bkt.hints.maxDumpableChunkID = endChunkID - 1
	if merge {
		bkt.hints.forceRotateSplit()
		time.Sleep(time.Duration(SecsBeforeDump+1) * time.Second)
		bkt.hints.dumpAndMerge(true) // TODO: should not dump idx.m!
		bkt.hints.Merge(true)
	} else {
		bkt.hints.RemoveMerged()
	}

	// remove hints
	bkt.removeHtree()
}

func (mgr *GCMgr) AfterBucket(bkt *Bucket) {
	bkt.hints.state &= ^HintStateGC
	bkt.hints.maxDumpableChunkID = MAX_NUM_CHUNK - 1
}

func (bkt *Bucket) gcCheckEnd(start, endChunkID, noGCDays int) (end int, err error) {
	end = endChunkID
	if end < 0 || end >= bkt.datas.newHead-1 {
		end = bkt.datas.newHead - 1
	}

	if noGCDays < 0 {
		noGCDays = Conf.NoGCDays
	}
	for next := end + 1; next >= start+1; next-- {
		if bkt.datas.chunks[next].getDiskFileSize() <= 0 {
			continue
		}
		var ts int64
		ts, err = bkt.datas.chunks[next].getFirstRecTs()
		if err != nil {
			return
		}
		if time.Now().Unix()-ts > int64(noGCDays)*86400 {
			for end = next - 1; end >= start; end-- {
				if bkt.datas.chunks[end].size >= 0 {
					return
				}
			}
			return
		}
	}
	err = fmt.Errorf("no file to gc within %d days, start = %d", noGCDays, start)
	return
}

func (bkt *Bucket) gcCheckStart(startChunkID int) (start int, err error) {
	if startChunkID < 0 {
		start = bkt.NextGCChunk
	} else if startChunkID > bkt.datas.newHead {
		err = fmt.Errorf("startChunkID > bkt.datas.newHead ")
		return
	} else {

		start = startChunkID
	}
	for ; start < bkt.datas.newHead; start++ {
		if bkt.datas.chunks[start].size >= 0 {
			break
		}
	}
	return
}

func (bkt *Bucket) gcCheckRange(startChunkID, endChunkID, noGCDays int) (start, end int, err error) {
	if start, err = bkt.gcCheckStart(startChunkID); err != nil {
		return
	}
	if end, err = bkt.gcCheckEnd(start, endChunkID, noGCDays); err != nil {
		return
	}
	if end < start {
		err = fmt.Errorf("end %d < start %d, nothing to gc", end, start)
	}
	return
}

func (mgr *GCMgr) gc(ctx context.Context, ch chan<- int, bkt *Bucket, startChunkID, endChunkID int, merge bool) {

	logger.Infof("begin GC bucket %d chunk [%d, %d]", bkt.ID, startChunkID, endChunkID)

	bkt.GCHistory = append(bkt.GCHistory, GCState{})
	gc := &bkt.GCHistory[len(bkt.GCHistory)-1]
	// add gc to mgr's stat map
	mgr.mu.Lock()
	mgr.stat[bkt.ID] = gc
	mgr.mu.Unlock()
	gc.BeginTS = time.Now()
	defer func() {
		mgr.mu.Lock()
		delete(mgr.stat, bkt.ID)
		mgr.mu.Unlock()
		gcContextMap.rw.Lock()
		delete(gcContextMap.m, bkt.ID)
		gcContextMap.rw.Unlock()
		gc.EndTS = time.Now()
	}()
	gc.Begin = startChunkID
	gc.End = endChunkID

	var oldPos Position
	var newPos Position
	var rec *Record
	var r *DataStreamReader

	mgr.BeforeBucket(bkt, startChunkID, endChunkID, merge)
	defer mgr.AfterBucket(bkt)

	gc.Dst = startChunkID
	for i := 0; i < startChunkID; i++ {
		sz := bkt.datas.chunks[i].size
		if sz > 0 && (int64(sz) < Conf.DataFileMax-config.MCConf.BodyMax) {
			gc.Dst = i
		}
	}

	dstchunk := &bkt.datas.chunks[gc.Dst]
	err := dstchunk.beginGCWriting(gc.Begin)
	if err != nil {
		gc.Err = err
		return
	}
	newPos.ChunkID = gc.Dst
	defer func() {
		dstchunk.endGCWriting()
		bkt.hints.trydump(gc.Dst, true)
	}()

	for gc.Src = gc.Begin; gc.Src <= gc.End; gc.Src++ {
		if bkt.datas.chunks[gc.Src].size <= 0 {
			logger.Infof("skip empty chunk %d", gc.Src)
			continue
		}
		oldPos.ChunkID = gc.Src
		var fileState GCFileState
		// reader must have a larger buffer
		logger.Infof("begin GC bucket %d, file %d -> %d", bkt.ID, gc.Src, gc.Dst)
		bkt.hints.ClearChunk(gc.Src)
		if r, err = bkt.datas.GetStreamReader(gc.Src); err != nil {
			gc.Err = err
			logger.Errorf("gc failed: %s", err.Error())
			return
		}

		for {
			select {
			case <-ctx.Done():
				logger.Infof("cancel gc, src: %d, dst: %d", gc.Src, gc.Dst)
				gcContextMap.rw.Lock()
				delete(gcContextMap.m, bkt.ID)
				gcContextMap.rw.Unlock()
				ch <- gc.Src
				return
			default:
			}
			var sizeBroken uint32
			rec, oldPos.Offset, sizeBroken, err = r.Next()
			if err != nil {
				gc.Err = err
				logger.Errorf("gc failed: %s", err.Error())
				return
			}
			if rec == nil {
				break
			}

			var isNewest, isCoverdByCollision, isDeleted bool
			meta := rec.Payload.Meta
			ki := NewKeyInfoFromBytes(rec.Key, getKeyHash(rec.Key), false)
			treeMeta, treePos, found := bkt.htree.get(ki)
			if found {
				if oldPos == treePos { // easy
					meta.ValueHash = treeMeta.ValueHash
					isNewest = true
				} else {
					isDeleted = treeMeta.Ver < 0
					hintit, hintchunkid, isCoverdByCollision := bkt.hints.getCollisionGC(ki)
					if isCoverdByCollision {
						if hintit != nil {
							p := Position{hintchunkid, hintit.Pos.Offset}
							if p == oldPos {
								isNewest = true
								meta.ValueHash = hintit.Vhash
							}
						} else {
							isNewest = true // guess
							meta.ValueHash = rec.Payload.Getvhash()
						}
					}
				}
			} else {
				// when rebuiding the HTree, the deleted recs are removed from HTree
				// we are not sure whether the `set rec` is still in datafiles while `auto GC`, so we need to write a copy of `del rec` in datafile.
				// but we can remove the `del rec` while GC begin with 0
				fileState.NumNotInHtree++
				if gc.Begin > 0 && rec.Payload.Ver < 0 {
					isNewest = true
				}
			}

			wrec := wrapRecord(rec)
			recsize := wrec.rec.Payload.RecSize
			fileState.addRecord(recsize, isNewest, isDeleted, sizeBroken)
			//logger.Infof("key stat: %v %v %v %v", ki.StringKey, isNewest, isCoverdByCollision, isDeleted)
			if !isNewest {
				continue
			}

			if recsize+dstchunk.writingHead > uint32(Conf.DataFileMax) {
				dstchunk.endGCWriting()
				bkt.hints.trydump(gc.Dst, true)

				gc.Dst++
				newPos.ChunkID = gc.Dst
				logger.Infof("continue GC bucket %d, file %d -> %d", bkt.ID, gc.Src, gc.Dst)
				dstchunk = &bkt.datas.chunks[gc.Dst]
				err = dstchunk.beginGCWriting(gc.Src)
				if err != nil {
					gc.Err = err
					return
				}
			}
			if newPos.Offset, err = dstchunk.AppendRecordGC(wrec); err != nil {
				gc.Err = err
				logger.Errorf("gc failed: %s", err.Error())
				return
			}
			// logger.Infof("%s %v %v", ki.StringKey, newPos, meta)
			if found {
				if isCoverdByCollision {
					mgr.UpdateCollision(bkt, ki, oldPos, newPos, rec)
				}
				mgr.UpdateHtreePos(bkt, ki, oldPos, newPos)
			}

			rotated := bkt.hints.set(ki, &meta, newPos, recsize, "gc")
			if rotated {
				bkt.hints.trydump(gc.Dst, false)
			}
		}

		if gc.Src != gc.Dst {
			bkt.datas.chunks[gc.Src].Clear()
		}
		if gc.Src+1 >= bkt.NextGCChunk {
			bkt.NextGCChunk = gc.Src + 1
			bkt.dumpGCHistroy()
		}
		logger.Infof("end GC file %#v", fileState)
		gc.add(&fileState)
	}
	logger.Infof("end GC all %#v", gc)
}
