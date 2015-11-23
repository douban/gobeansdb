package store

import (
	"fmt"
	"time"
)

type GCMgr struct {
	bucketID int
	stat     *GCState // curr or last
	ki       KeyInfo
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

	Err     error
	Running bool

	// sum
	GCFileState
}

type GCFileState struct {
	Src                int
	NumBefore          int
	NumReleased        int
	NumReleasedDeleted int
	SizeBefore         uint32
	SizeReleased       uint32
	SizeDeleted        uint32
	SizeBroken         uint32
}

func (s *GCFileState) add(size uint32, isNewest, isDeleted bool, sizeBroken uint32) {
	if !isNewest {
		s.NumReleased += 1
		s.SizeReleased += size
		if isDeleted {
			s.NumReleasedDeleted += 1
			s.SizeDeleted += size
		}
	}
	s.SizeReleased += sizeBroken
	s.SizeBroken += sizeBroken
	s.SizeBefore += (size + sizeBroken)
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
	meta, pos, _ := bkt.htree.get(ki)
	if pos != oldPos {
		logger.Warnf("old key update when updating pos bucket %d %s %#v %#v",
			bkt.ID, ki.StringKey, meta, oldPos)
		return
	}
	bkt.htree.set(ki, meta, newPos)
}

func (mgr *GCMgr) BeforeBucket(bkt *Bucket, startChunkID, endChunkID int) {
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
	bkt.hints.forceRotateSplit()
	time.Sleep(time.Duration(SecsBeforeDump+1) * time.Second)
	bkt.hints.dumpAndMerge(true) // TODO: should not dump idx.m!
	bkt.hints.Merge(true)

	// remove hints
	bkt.removeHtree()
}

func (mgr *GCMgr) AfterBucket(bkt *Bucket) {
	bkt.hints.state &= ^HintStateGC
	bkt.hints.maxDumpableChunkID = MAX_CHUNK_ID
	bkt.dumpHtree()
}

func (bkt *Bucket) gcCheckEnd(start, endChunkID, noGCDays int) (end int, err error) {
	end = endChunkID
	if end < 0 || end >= bkt.datas.newHead-1 {
		end = bkt.datas.newHead - 1
	}

	if noGCDays < 0 {
		noGCDays = conf.NoGCDays
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
		if time.Now().Unix()-ts < int64(noGCDays)*86400 {
			for end = next - 1; end >= start; end-- {
				if bkt.datas.chunks[end].size >= 0 {
					return
				}
			}
			return
		}
	}
	return
}

func (bkt *Bucket) gcCheckStart(startChunkID int) (start int) {
	if startChunkID < 0 {
		start = bkt.NextGCChunk
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
	start = bkt.gcCheckStart(startChunkID)
	if end, err = bkt.gcCheckEnd(start, endChunkID, noGCDays); err != nil {
		return
	}
	if end <= start {
		err = fmt.Errorf("end %d <= start %d, nothing to gc", end, start)
	}
	return
}

func (mgr *GCMgr) gc(bkt *Bucket, startChunkID, endChunkID int) {

	logger.Infof("begin GC bucket %d chunk [%d, %d]", bkt.ID, startChunkID, endChunkID)

	bkt.GCHistory = append(bkt.GCHistory, GCState{})
	gc := &bkt.GCHistory[len(bkt.GCHistory)-1]
	mgr.stat = gc
	gc.Running = true
	defer func() {
		gc.Running = false
	}()
	gc.Begin = startChunkID
	gc.End = endChunkID

	var oldPos Position
	var newPos Position
	var rec *Record
	var r *DataStreamReader
	mfs := uint32(conf.DataFileMax)

	mgr.BeforeBucket(bkt, startChunkID, endChunkID)
	defer mgr.AfterBucket(bkt)

	gc.Dst = startChunkID
	for i := 0; i <= startChunkID; i++ {
		sz := bkt.datas.chunks[i].size
		if sz > 0 && (int64(sz) < conf.DataFileMax-conf.BodyMax) {
			gc.Dst = i
		}
	}
	dstchunk := bkt.datas.chunks[gc.Dst]
	err := dstchunk.beginGCWriting(gc.Begin)
	if err != nil {
		gc.Err = err
		return
	}
	defer dstchunk.endGCWriting()

	for gc.Src = gc.Begin; gc.Src <= gc.End; gc.Src++ {
		oldPos.ChunkID = gc.Src
		var fileState GCFileState
		// reader must have a larger buffer
		logger.Infof("begin GC bucket %d, file %d -> %d", bkt.ID, gc.Src, gc.Dst)
		bkt.hints.ClearChunks(gc.Src, gc.Src)
		if r, err = bkt.datas.GetStreamReader(gc.Src); err != nil {
			gc.Err = err
			logger.Errorf("gc failed: %s", err.Error())
			return
		}

		for {
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
					hintit, isCoverdByCollision := bkt.hints.getCollisionGC(ki)
					if isCoverdByCollision {
						if hintit != nil {
							if decodePos(hintit.Pos) == oldPos {
								isNewest = true
								meta.ValueHash = hintit.Vhash
							} else {

							}
						} else {
							isNewest = true // guess
							meta.ValueHash = rec.Payload.Getvhash()
						}
					}
				}
			} else { // should not happen
				logger.Errorf("gc old key not found in htree bucket %d %#v %#v %#v",
					bkt.ID, ki, meta, oldPos)
				isNewest = true
				meta.ValueHash = rec.Payload.Getvhash()
			}

			wrec := wrapRecord(rec)
			recsize := wrec.rec.Payload.RecSize
			fileState.add(recsize, isNewest, isDeleted, sizeBroken)
			// logger.Infof("%v %v %v %v", ki.StringKey, isNewest, isCollision, isDeleted)
			if !isNewest {
				continue
			}

			if recsize+dstchunk.writingHead > mfs {
				dstchunk.endGCWriting()

				gc.Dst++
				newPos.ChunkID = gc.Dst

				dstchunk = bkt.datas.chunks[gc.Dst]
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
			if isCoverdByCollision {
				mgr.UpdateCollision(bkt, ki, oldPos, newPos, rec)
			} else {
				mgr.UpdateHtreePos(bkt, ki, oldPos, newPos)
			}
			bkt.hints.set(ki, &meta, newPos, recsize)
		}
		if gc.Src != gc.Dst {
			bkt.datas.DeleteFile(gc.Src)
		}
		logger.Infof("end GC file %#v", fileState)
	}
}
