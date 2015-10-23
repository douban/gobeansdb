package store

import (
	"fmt"
	"time"
)

type gcMgr struct {
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

func (mgr *gcMgr) ShouldRetainRecord(bkt *Bucket, rec *Record, oldPos Position) bool {
	return true
}

func (mgr *gcMgr) UpdatePos(bkt *Bucket, ki *KeyInfo, oldPos, newPos Position) {
	// TODO
}

func (mgr *gcMgr) gc(bkt *Bucket, startChunkID, endChunkID int) (err error) {
	bkt.GCHistory = append(bkt.GCHistory, GCState{})
	gc := &bkt.GCHistory[len(bkt.GCHistory)-1]

	gc.Begin = startChunkID
	gc.End = endChunkID
	gc.Dst = startChunkID
	gc.Running = true

	var oldPos Position
	var newPos Position
	var rec *Record
	var r *DataStreamReader
	var w *DataStreamWriter
	mfs := dataConfig.MaxFileSize

	//bkt.Index.OnGCBegin(gc)
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
			isRetained := mgr.ShouldRetainRecord(bkt, rec, oldPos)
			if isRetained {
				if newPos.Offset, err = w.Append(rec); err != nil {
					logger.Errorf("gc failed: %s", err.Error())
					return
				}
				keyinfo := NewKeyInfoFromBytes(rec.Key, getKeyHash(rec.Key), false)
				mgr.UpdatePos(bkt, keyinfo, oldPos, newPos)
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
	//bkt.Index.OnGCEnd()
	gc.Running = false
	return nil
}
