package store

import "container/heap"

type mergeReader struct {
	r    *hintFileReader
	curr *hintItem
}

type mergeWriter struct {
	w   *hintFileWriter
	buf []*hintItem
	num int
}

func newMergeWriter(w *hintFileWriter) *mergeWriter {
	mw := new(mergeWriter)
	mw.w = w
	mw.buf = make([]*hintItem, 1000)
	return mw
}

func (mw *mergeWriter) write(it *hintItem) {
	if mw.num == 0 {
		mw.buf[0] = it
		mw.num = 1
		return
	}
	last := mw.buf[mw.num-1]
	if last.keyhash != it.keyhash {
		mw.flush()
		mw.num = 1
		mw.buf[0] = it
	} else {
		if last.key != it.key {
			mw.num += 1
			if mw.num > len(mw.buf) {
				newbuf := make([]*hintItem, len(mw.buf)*2)
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
	if a.keyhash != b.keyhash {
		return a.keyhash < b.keyhash
	} else {
		if a.key != b.key {
			return a.key < b.key
		}
	}
	return a.pos < b.pos
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
	}
	w, err := newHintFileWriter(dst, 1<<20)
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
			mr.curr.pos += uint32(mr.r.chunkID)
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
