package store

import (
	"fmt"
	"time"
)

type GCMgr struct {
	bucketID int
	stat     *GCState // curr or laste
	ki       KeyInfo
}

type GCState struct {
	BeginTS time.Time
	EndTS   time.Time

	Begin int
	End   int

	// curr
	Src int
	Dst int

	StopReason error
	Running    bool

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

func (s *GCFileState) add(size uint32, isRetained, isDeleted bool, sizeBroken uint32) {
	if !isRetained {
		s.NumReleased += 1
		s.SizeReleased += size
		if isDeleted {
			s.NumReleasedDeleted += 1
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

func (mgr *GCMgr) ShouldRetainRecord(bkt *Bucket, rec *Record, oldPos Position) (retain, updateHtree bool) {
	ki := &mgr.ki
	ki.KeyHash = getKeyHash(rec.Key)
	ki.Key = rec.Key
	ki.StringKey = string(ki.Key)
	ki.KeyIsPath = false
	ki.Prepare()
	meta, pos, found := bkt.htree.get(ki)
	if !found {
		logger.Errorf("old key not found in htree bucket %d %#v %#v %#v",
			bkt.id, ki, meta, oldPos)
		return true, true
	} else if pos == oldPos {
		return true, true
	} else {
		it, collision := bkt.hints.collisions.get(ki.KeyHash, ki.StringKey)
		if !collision {
			it, collision = bkt.hints.getItemCollision(ki.KeyHash, ki.StringKey) // TODO
		}
		if collision {
			if it != nil {
				return decodePos(it.Pos) == oldPos, false
			} else {
				return true, false
			}
		}
	}
	return false, false
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
			bkt.id, ki.StringKey, meta, oldPos)
		return
	}
	bkt.htree.set(ki, meta, newPos)
}

func (mgr *GCMgr) BeforeBucket(bkt *Bucket, startChunkID, endChunkID int) {
	for bkt.hints.dumpAndMergeState != HintStateIdle {
		time.Sleep(10 * time.Millisecond)
	}
	bkt.hints.maxDumpableChunkID = endChunkID
	bkt.hints.forceRotateSplit()
	bkt.hints.dumpAndMerge(true)
	bkt.removeHtree()
	bkt.hints.ClearChunks(startChunkID, endChunkID)

}

func (mgr *GCMgr) AfterBucket(bkt *Bucket) {
	bkt.hints.dumpAndMerge(true)
	bkt.hints.maxDumpableChunkID = MAX_CHUNK_ID
	bkt.dumpHtree()
}

func (mgr *GCMgr) gc(bkt *Bucket, startChunkID, endChunkID int) (err error) {
	if endChunkID < 0 {
		endChunkID = bkt.datas.newHead
	}
	bkt.GCHistory = append(bkt.GCHistory, GCState{})
	gc := &bkt.GCHistory[len(bkt.GCHistory)-1]
	mgr.stat = gc

	gc.Begin = startChunkID
	gc.End = endChunkID
	gc.Dst = startChunkID
	gc.Running = true
	defer func() {
		gc.Running = false
	}()

	var oldPos Position
	var newPos Position
	var rec *Record
	var r *DataStreamReader
	var w *DataStreamWriter
	mfs := uint32(dataConfig.MaxFileSize)

	mgr.BeforeBucket(bkt, startChunkID, endChunkID)
	defer mgr.AfterBucket(bkt)
	for gc.Src = gc.Begin; gc.Src < gc.End; gc.Src++ {
		oldPos.ChunkID = gc.Src
		var fileState GCFileState
		// reader must have a larger buffer
		logger.Infof("begin GC file %d -> %d", gc.Src, gc.Dst)
		if r, err = bkt.datas.GetStreamReader(gc.Src); err != nil {
			logger.Errorf("gc failed: %s", err.Error())
			return
		}
		w, err = bkt.datas.GetStreamWriter(gc.Dst, gc.Dst != gc.Src)
		if err != nil {
			return
		}
		for {
			var sizeBroken uint32
			rec, oldPos.Offset, sizeBroken, err = r.Next()
			if err != nil {
				logger.Errorf("gc failed: %s", err.Error())
				return
			}
			if rec == nil {
				break
			}

			_, recsize := rec.Sizes()

			if recsize+w.Offset() > mfs {
				w.Close()
				gc.Dst++
				newPos.ChunkID = gc.Dst
				if w, err = bkt.datas.GetStreamWriter(gc.Dst, gc.Dst != gc.Src); err != nil {
					logger.Errorf("gc failed: %s", err.Error())
					return
				}
			}
			isRetained, updateHtree := mgr.ShouldRetainRecord(bkt, rec, oldPos)
			if isRetained {
				if newPos.Offset, err = w.Append(rec); err != nil {
					logger.Errorf("gc failed: %s", err.Error())
					return
				}
				keyinfo := NewKeyInfoFromBytes(rec.Key, getKeyHash(rec.Key), false)
				if updateHtree {
					mgr.UpdateHtreePos(bkt, keyinfo, oldPos, newPos)
				} else {
					mgr.UpdateCollision(bkt, keyinfo, oldPos, newPos, rec)
				}
			}
			fileState.add(recsize, isRetained, rec.Payload.Ver < 0, sizeBroken)
		}
		w.Close()
		size := w.Offset()
		bkt.datas.Truncate(gc.Dst, size)
		if gc.Src != gc.Dst {
			bkt.datas.DeleteFile(gc.Src)
		}
		logger.Infof("end GC file %#v", fileState)
	}
	return nil
}
