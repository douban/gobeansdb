package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

type hintIndexItem struct {
	keyhash uint64
	offset  int64
}

type hintFileIndex struct {
	index []hintIndexItem
	path  string
}

func newHintFileIndex() (idx *hintFileIndexBuffer) {
	idx = new(hintFileIndexBuffer)
	idx.index = make([][]hintIndexItem, 1, 4096)
	idx.index[0] = make([]hintIndexItem, HINTINDEX_ROW_SIZE)
	return
}

func (idx *hintFileIndex) get(keyhash uint64, key string) (item *HintItem, err error) {
	//logger.Warnf("try get from hintfile %s: %016x, %s", idx.path, keyhash, key)
	arr := idx.index

	// first larger
	j := sort.Search(len(arr), func(i int) bool { return arr[i].keyhash >= keyhash })
	offset := int64(HINTFILE_HEAD_SIZE)
	if j > 1 {
		offset = arr[j-1].offset
	}
	reader := newHintFileReader(idx.path, 0, int(hintConfig.IndexIntervalSize))
	err = reader.open()
	if err != nil {
		return
	}
	reader.fd.Seek(offset, 0)
	reader.rbuf.Reset(reader.fd)
	defer reader.fd.Close()
	var it *HintItem
	for {
		it, err = reader.next()
		if err != nil {
			return
		}
		if it == nil {
			return
		}
		if it.Keyhash < keyhash {
			continue
		} else if it.Keyhash > keyhash {
			return
		} else {
			if it.Key == key {
				item = it
				return
			} else {
				continue
			}
		}
	}
	return
}

func loadHintIndex(path string) (index *hintFileIndex, err error) {
	fd, err := os.Open(path)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	fileInfo, _ := fd.Stat()
	size := fileInfo.Size()

	var head [HINTFILE_HEAD_SIZE]byte

	var readn int
	readn, err = fd.Read(head[:])
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	if readn < HINTFILE_HEAD_SIZE {
		err = fmt.Errorf("bad hint file %s readn %d", path, readn)
		logger.Errorf(err.Error())
		return
	}
	start := int64(binary.LittleEndian.Uint64(head[:8]))
	num := int(size - start)
	fd.Seek(start, 0)
	raw := make([]byte, num)
	readn, err = fd.Read(raw)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	if int64(readn) < size-start {
		err = fmt.Errorf("bad hint file %s readn %d", path, start)
		logger.Errorf(err.Error())
		return
	}
	fd.Close()
	arr := make([]hintIndexItem, num/16)
	for offset := 0; offset < num; offset += 16 {
		i := offset / 16
		arr[i].keyhash = binary.LittleEndian.Uint64(raw[offset : offset+8])
		arr[i].offset = int64(binary.LittleEndian.Uint64(raw[offset+8 : offset+16]))
	}
	return &hintFileIndex{arr, path}, nil
}

// used for 1. get
type hintFileIndexBuffer struct {
	index      [][]hintIndexItem
	currRow    int
	currCol    int
	lastoffset int64
}

func (b *hintFileIndexBuffer) toIndex() []hintIndexItem {
	n := HINTINDEX_ROW_SIZE*b.currRow + b.currCol
	arr := make([]hintIndexItem, n)
	size := 0
	for r := 0; r < b.currRow; r++ {
		copy(arr[size:], b.index[r])
		size += HINTINDEX_ROW_SIZE
	}
	copy(arr[size:], b.index[b.currRow][:b.currCol])
	return arr
}

func (idx *hintFileIndexBuffer) append(keyhash uint64, offset int64) {
	idx.index[idx.currRow][idx.currCol] = hintIndexItem{keyhash, offset}
	idx.lastoffset = offset
	if idx.currCol >= HINTINDEX_ROW_SIZE-1 {
		idx.currRow += 1
		idx.index[idx.currRow] = make([]hintIndexItem, HINTINDEX_ROW_SIZE)
		idx.currCol = 0
	} else {
		idx.currCol += 1
	}
}
