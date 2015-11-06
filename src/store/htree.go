package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	BUCKET_SIZE = 16
	MAX_DEPTH   = 8
)

var (
	KHASH_LENS = [8]int{8, 8, 7, 7, 6, 6, 5, 5}
)

type HTree struct {
	sync.Mutex
	// args
	depth int
	pos   int

	// runtime
	levels  [][]Node
	leafs   []bytesLeaf
	maxLeaf int

	// tmp, to avoid alloc
	ni NodeInfo
}

type Node struct {
	count uint32
	// size    uint32 //including deleted
	hash    uint16
	isValid bool
}

type NodeInfo struct {
	node   *Node
	level  int
	offset int
	path   []int
}

func newHTree(depth, pos, height int) *HTree {
	if depth+height > MAX_DEPTH {
		panic("HTree too high")
	}
	tree := new(HTree)
	tree.depth = depth
	tree.pos = pos
	tree.levels = make([][]Node, height)
	size := 1
	for i := 0; i < height; i++ {
		tree.levels[i] = make([]Node, size)
		size *= 16
	}
	size /= 16
	leafnodes := tree.levels[height-1]
	for i := 0; i < size; i++ {
		leafnodes[i].isValid = true
	}
	tree.leafs = make([]bytesLeaf, size)
	return tree
}

func (tree *HTree) load(path string) (err error) {
	f, err := os.Open(path)
	if err != nil {
		logger.Errorf("fail to load htree %s", err.Error())
		return
	}
	defer f.Close()
	logger.Infof("loading htree %s", path)
	reader := bufio.NewReader(f)
	buf := make([]byte, 6)
	leafnodes := tree.levels[htreeConfig.TreeHeight-1]
	size := len(leafnodes)
	for i := 0; i < size; i++ {
		if _, err = io.ReadFull(reader, buf); err != nil {
			return
		}
		leafnodes[i].count = binary.LittleEndian.Uint32(buf[0:4])
		leafnodes[i].hash = binary.LittleEndian.Uint16(buf[4:6])
	}
	for i := 0; i < size; i++ {
		if _, err = io.ReadFull(reader, buf[:4]); err != nil {
			return
		}
		l := int(binary.LittleEndian.Uint32(buf[:4]))
		tree.leafs[i] = tree.leafs[i].enlarge(int(l))
		if _, err = io.ReadFull(reader, tree.leafs[i]); err != nil {
			return
		}
	}
	tree.ListTop()
	return nil
}

func (tree *HTree) dump(path string) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logger.Errorf("fail to dump htree %s", err.Error())
		return
	}
	defer f.Close()
	tmp := path + ".tmp"
	logger.Infof("dumping htree %s", tmp)
	tree.ListTop()

	writer := bufio.NewWriter(f)
	buf := make([]byte, 6)
	leafnodes := tree.levels[htreeConfig.TreeHeight-1]
	size := len(leafnodes)
	for i := 0; i < size; i++ {
		binary.LittleEndian.PutUint32(buf[0:4], leafnodes[i].count)
		binary.LittleEndian.PutUint16(buf[4:6], leafnodes[i].hash)
		if _, err := writer.Write(buf); err != nil {
			logger.Errorf("write node fail %s %s", path, err.Error())
			return
		}
	}
	minleaf := 1 << 30
	maxleaf := 0
	for i := 0; i < size; i++ {
		leaf := tree.leafs[i]
		ll := len(leaf)
		if ll > maxleaf {
			maxleaf = ll
		} else if ll < minleaf {
			minleaf = ll
		}

		binary.LittleEndian.PutUint32(buf[0:4], uint32(ll))
		if _, err := writer.Write(buf[:4]); err != nil {
			logger.Errorf("write leafsize fail %s %s", path, err.Error())
			return
		}
		if _, err := writer.Write(leaf); err != nil {
			logger.Errorf("write leaf fail %s %s", path, err.Error())
			return
		}
	}
	if err := writer.Flush(); err != nil {
		logger.Errorf("flush htree fail %s %s", path, err.Error())
		return
	}
	os.Rename(tmp, path)
	logger.Debugf("dumped %s, min leaf %d, max leaf", path, minleaf, maxleaf)
}

func (tree *HTree) getHex(khash uint64, level int) int {
	depth := level + tree.depth
	shift := (16 - depth - 1) * 4
	return int(0xf & (khash >> uint32(shift)))
}

func (tree *HTree) setLeaf(req *HTreeReq, ni *NodeInfo) {
	node := ni.node
	oldm, exist, newleaf := tree.leafs[ni.offset].Set(req, ni)
	tree.leafs[ni.offset] = newleaf

	vhash := uint16(0)
	if req.item.ver > 0 {
		vhash += req.item.vhash
		node.count += 1
	}
	if exist && oldm.ver > 0 {
		vhash -= oldm.vhash
		node.count -= 1
	}
	node.hash += vhash * uint16(req.ki.KeyHash>>32)
}

func (tree *HTree) getLeaf(ki *KeyInfo, ni *NodeInfo) {
	ni.level = len(tree.levels) - 1
	ni.offset = 0
	path := ki.KeyPath[tree.depth:]
	for level := 1; level < len(tree.levels); level += 1 {
		ni.offset = ni.offset*16 + path[level-1]
	}
	ni.node = &tree.levels[ni.level][ni.offset]
	ni.path = ki.KeyPath[:tree.depth+ni.level]
	return
}

func (tree *HTree) getLeafAndInvalidNodes(ki *KeyInfo, ni *NodeInfo) {
	ni.level = len(tree.levels) - 1

	ni.offset = 0
	path := ki.KeyPath[tree.depth:]
	tree.levels[0][0].isValid = false
	for level := 1; level < len(tree.levels)-1; level += 1 {
		ni.offset = ni.offset*16 + path[level-1]
		tree.levels[level][ni.offset].isValid = false
	}
	ni.offset = ni.offset*16 + path[ni.level-1]
	ni.node = &tree.levels[ni.level][ni.offset]
	ni.path = ki.KeyPath[:tree.depth+ni.level]
	return
}

func (tree *HTree) getNode(ki *KeyInfo, ni *NodeInfo) {
	offset := 0

	l := tree.depth + len(tree.levels) - 1
	if len(ki.KeyPath) < l {
		l = len(ki.KeyPath)
	}

	h := tree.depth

	for ; h < l; h += 1 {
		offset = offset*16 + ki.KeyPath[h]
	}
	ni.level = l - tree.depth
	if ni.level > len(tree.levels)-1 {
		panic(fmt.Sprintf("Bug: level TOO LARGE %d l=%d", h, l))
	}
	ni.offset = offset
	ni.node = &tree.levels[ni.level][ni.offset]
	ni.path = ki.KeyPath[:tree.depth+ni.level]
	return
}

func (tree *HTree) set(ki *KeyInfo, meta *Meta, pos Position) {
	var req HTreeReq
	req.ki = ki
	req.Meta = *meta
	req.Position = pos
	req.item = HTreeItem{ki.KeyHash, pos.encode(), meta.Ver, meta.ValueHash}
	tree.setReq(&req)
}

func (tree *HTree) setReq(req *HTreeReq) {
	tree.Lock()
	defer tree.Unlock()

	tree.getLeafAndInvalidNodes(req.ki, &tree.ni)
	tree.setLeaf(req, &tree.ni)
	if int(tree.ni.node.count) > tree.maxLeaf {
		tree.maxLeaf = int(tree.ni.node.count)
	}
}

func (tree *HTree) get(ki *KeyInfo) (meta *Meta, pos Position, found bool) {
	var req HTreeReq
	req.ki = ki
	found = tree.getReq(&req)
	meta = &Meta{0, 0, req.item.ver, req.item.vhash, 0}
	pos = decodePos(req.item.pos)
	return
}

func (tree *HTree) getReq(req *HTreeReq) (found bool) {
	tree.Lock()
	defer tree.Unlock()
	ni := &tree.ni
	tree.getLeaf(req.ki, ni)

	found = tree.leafs[ni.offset].Get(req, ni)
	return
}

func (tree *HTree) Update() (node *Node) {
	tree.Lock()
	defer tree.Unlock()
	return tree.updateNodes(0, 0)
}

func (tree *HTree) updateNodes(level, offset int) (node *Node) {
	node = &tree.levels[level][offset]
	if node.isValid {
		return
	}
	node.count = 0
	var hashs [16]uint16
	for i := 0; i < 16; i++ {
		cnode := tree.updateNodes(level+1, offset*16+i)
		node.count += cnode.count
		hashs[i] = cnode.hash
	}
	node.hash = 0
	for i := 0; i < 16; i++ {
		if node.count > htreeConfig.ThresholdBigHash {
			node.hash *= 97
		}
		node.hash += hashs[i]
	}
	node.isValid = true
	return
}

func (tree *HTree) collectItems(ni *NodeInfo, items []HTreeItem, filterkeyhash, filtermask uint64) []HTreeItem {
	if ni.level >= len(tree.levels)-1 { // leaf
		f := func(h uint64, m *HTreeItem) {
			if (filtermask & h) == filterkeyhash {
				m.keyhash = h
				items = append(items, *m)
			}
		}
		tree.leafs[ni.offset].Iter(f, ni)
	} else {
		var c NodeInfo
		c.level = ni.level + 1
		var cpathBuf [8]int
		c.path = cpathBuf[:tree.depth+c.level]
		copy(c.path, ni.path)
		for i := 0; i < 16; i++ {
			c.offset = ni.offset*16 + i
			c.node = &tree.levels[c.level][c.offset]
			c.path[tree.depth+ni.level] = i
			items = tree.collectItems(&c, items, filterkeyhash, filtermask)
		}
	}
	return items
}

func (tree *HTree) listDir(ki *KeyInfo) (items []HTreeItem, nodes []*Node) {
	var ni NodeInfo
	if len(ki.KeyPath) == tree.depth {
		ni.node = &tree.levels[0][0]
		ni.path = ki.KeyPath
	} else {
		tree.getNode(ki, &ni)
	}
	node := ni.node
	tree.updateNodes(ni.level, ni.offset)
	if ni.level >= len(tree.levels)-1 || node.count < htreeConfig.ThresholdListKey {
		items = make([]HTreeItem, 0, node.count+64) // item deleted not counted
		var filtermask uint64 = 0xffffffffffffffff
		shift := uint(64 - len(ki.StringKey)*4)
		filtermask = (filtermask >> shift) << shift
		items = tree.collectItems(&ni, items, ki.KeyHash, filtermask)
		return
	} else {
		nodes = make([]*Node, 16)
		for i := 0; i < 16; i++ {
			nodes[i] = &tree.levels[ni.level+1][ni.offset*16+i]
		}
	}
	return
}

func (tree *HTree) ListDir(ki *KeyInfo) (ret []byte, err error) {
	if len(ki.Key) < tree.depth {
		return nil, fmt.Errorf("bad dir path to list: too short")
	}
	tree.Lock()
	defer tree.Unlock()

	items, nodes := tree.listDir(ki)
	var buffer bytes.Buffer
	if items != nil {

		for _, item := range items {
			s := fmt.Sprintf("%016x %d %d\n", item.keyhash, int(item.vhash), item.ver)
			buffer.WriteString(s)
		}
		return buffer.Bytes(), nil
	} else if nodes != nil {
		for i, n := range nodes {
			s := fmt.Sprintf("%x/ %d %d\n", i, n.hash, int(n.count))
			buffer.WriteString(s)
		}
		return buffer.Bytes(), nil
	}
	return nil, nil
}

func (tree *HTree) ListTop() {
	path := fmt.Sprintf("%x", tree.pos)
	ki := &KeyInfo{
		StringKey: path,
		Key:       []byte(path),
		KeyIsPath: true}
	ki.Prepare()
	data, _ := tree.ListDir(ki)
	logger.Infof("listing %s:\n%s", path, string(data))
	//items, nodes := tree.listDir(ki)
	//logger.Infof("%s %#v %#v", path, items, nodes)
}
