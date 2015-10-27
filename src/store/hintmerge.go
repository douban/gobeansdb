package store

import "container/heap"

type mergeReader struct {
	r    *hintFileReader
	curr *HintItem
}

type mergeWriter struct {
	w   *hintFileWriter
	buf []*HintItem
	num int
}

func newMergeWriter(w *hintFileWriter) *mergeWriter {
	mw := new(mergeWriter)
	mw.w = w
	mw.buf = make([]*HintItem, 1000)
	return mw
}

func (mw *mergeWriter) write(it *HintItem) {
	if mw.num == 0 {
		mw.buf[0] = it
		mw.num = 1
		return
	}
	last := mw.buf[mw.num-1]
	if last.Keyhash != it.Keyhash {
		mw.flush()
		mw.num = 1
		mw.buf[0] = it
	} else {
		if last.Key != it.Key {
			mw.num += 1
			if mw.num > len(mw.buf) {
				newbuf := make([]*HintItem, len(mw.buf)*2)
				copy(newbuf, mw.buf)
				mw.buf = newbuf
			}
		}
		mw.buf[mw.num-1] = it
	}
}

func (mw *mergeWriter) flush() {
	for i := 0; i < mw.num; i++ {
		mw.w.writeItem(mw.buf[i])
	}
}

type mergeHeap []*mergeReader

func (h mergeHeap) Len() int      { return len(h) }
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h mergeHeap) Less(i, j int) bool {
	a := h[i].curr
	b := h[j].curr
	if a.Keyhash != b.Keyhash {
		return a.Keyhash < b.Keyhash
	} else {
		if a.Key != b.Key {
			return a.Key < b.Key
		}
	}
	return a.Pos < b.Pos
}

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(*mergeReader))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func merge(src []*hintFileReader, dst string) (idx *hintFileIndex, err error) {
	n := len(src)
	maxoffset := uint32(0)
	hp := make([]*mergeReader, n)
	for i := 0; i < n; i++ {
		err := src[i].open()
		if err != nil {
			logger.Errorf("%s", err.Error())
			return nil, err
		}
		hp[i] = &mergeReader{src[i], nil}
		hp[i].curr, err = src[i].next()
		if err != nil {
			logger.Errorf("%s", err.Error())
			return nil, err
		}
		if src[i].maxOffset > maxoffset {
			maxoffset = src[i].maxOffset
		}
	}
	w, err := newHintFileWriter(dst, maxoffset, 1<<20)
	if err != nil {
		logger.Errorf("%s", err.Error())
		return nil, err
	}
	mw := newMergeWriter(w)
	h := mergeHeap(hp)
	heap.Init(&h)

	for len(h) > 0 {
		mr := heap.Pop(&h).(*mergeReader)
		mw.write(mr.curr)
		mr.curr, err = mr.r.next()
		if err != nil {
			logger.Errorf("%s", err.Error())
			// TODO:
		}
		if mr.curr != nil {
			mr.curr.Pos += uint32(mr.r.chunkID)
			heap.Push(&h, mr)
		} else {
			// logger.Debugf("%s done", mr.r.path)
		}
	}
	for _, mr := range hp {
		mr.r.close()
	}
	mw.flush()
	mw.w.close()
	return &hintFileIndex{mw.w.index.toIndex(), dst}, nil
}
