package embeddb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"slices"
	"sync"

	"github.com/edsrzf/mmap-go"
)

const (
	BTreePageSize     = 4096
	BTreeMaxKeys      = (BTreePageSize - 36) / 40
	BTreeMagic        = 0x42545245
	BTreeNodeFree     = 0
	BTreeNodeInternal = 1
	BTreeNodeLeaf     = 2
	BTreeCacheSize    = 2048
)

var ErrKeyNotFound = errors.New("key not found")
var ErrKeyExists = errors.New("key already exists")

type BTree struct {
	file      *os.File
	mmap      mmap.MMap
	alloc     *allocator
	rootOff   uint64
	mu        sync.RWMutex
	cache     map[uint64]*BTreeNode
	cacheList []uint64
}

type BTreeNode struct {
	IsLeaf   bool
	Count    int
	Keys     [][]byte
	Values   []uint64
	Children []uint64
	NextLeaf uint64
	Offset   uint64
	Dirty    bool
}

func (db *database) openBTree(rootOff uint64) (*BTree, error) {
	bt := &BTree{
		file:      db.file,
		alloc:     db.alloc,
		rootOff:   rootOff,
		cache:     make(map[uint64]*BTreeNode, BTreeCacheSize),
		cacheList: make([]uint64, 0, BTreeCacheSize),
	}

	if bt.rootOff == 0 {
		root := bt.newNode(true)
		bt.rootOff = root.Offset
		bt.writeNode(root)
	}

	mm, err := mmap.Map(db.file, os.O_RDWR, 0)
	if err != nil {
		mm, err = mmap.Map(db.file, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
	}
	bt.mmap = mm

	return bt, nil
}

func (bt *BTree) newNode(isLeaf bool) *BTreeNode {
	node := &BTreeNode{
		IsLeaf:   isLeaf,
		Count:    0,
		Keys:     make([][]byte, 0, BTreeMaxKeys),
		Values:   make([]uint64, 0, BTreeMaxKeys),
		Children: make([]uint64, 0, BTreeMaxKeys+1),
		NextLeaf: 0,
		Dirty:    true,
	}

	off, _ := bt.alloc.Allocate(BTreePageSize)
	node.Offset = off
	return node
}

const BTreeNodeSize = BTreePageSize

func (bt *BTree) ensureMmap(size int64) error {
	if size <= int64(len(bt.mmap)) {
		return nil
	}
	bt.mmap.Unmap()
	if err := bt.file.Truncate(size); err != nil {
		return err
	}
	mm, err := mmap.Map(bt.file, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	bt.mmap = mm
	return nil
}

func (bt *BTree) writeNode(node *BTreeNode) error {
	if node.Offset == 0 {
		off, _ := bt.alloc.Allocate(BTreePageSize)
		node.Offset = off
	}

	if err := bt.ensureMmap(int64(node.Offset) + BTreePageSize); err != nil {
		return err
	}

	keyDataSize := 0
	for i := 0; i < node.Count; i++ {
		keyDataSize += 4 + len(node.Keys[i])
	}

	fixedSize := 7
	if node.IsLeaf {
		fixedSize += 8
	} else {
		fixedSize += 8 * (node.Count + 1)
	}
	fixedSize += 8 * node.Count

	totalSize := fixedSize + keyDataSize
	alignedSize := (totalSize + 7) &^ 7

	buf := make([]byte, alignedSize)

	if node.IsLeaf {
		buf[0] = BTreeNodeLeaf
	} else {
		buf[0] = BTreeNodeInternal
	}

	binary.LittleEndian.PutUint16(buf[1:3], 0)
	binary.LittleEndian.PutUint32(buf[3:7], uint32(node.Count))

	pos := 7
	for i := 0; i < node.Count; i++ {
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(node.Keys[i])))
		pos += 4
		copy(buf[pos:], node.Keys[i])
		pos += len(node.Keys[i])
	}

	for i := 0; i < node.Count; i++ {
		binary.LittleEndian.PutUint64(buf[pos:pos+8], node.Values[i])
		pos += 8
	}

	if !node.IsLeaf {
		for i := 0; i <= node.Count; i++ {
			binary.LittleEndian.PutUint64(buf[pos:pos+8], node.Children[i])
			pos += 8
		}
	} else {
		binary.LittleEndian.PutUint64(buf[pos:pos+8], node.NextLeaf)
	}

	copy(bt.mmap[node.Offset:], buf)
	return bt.mmap.Flush()
}

func (bt *BTree) readNode(offset uint64) (*BTreeNode, error) {
	if offset == 0 {
		return nil, errors.New("invalid offset")
	}

	if node, ok := bt.cache[offset]; ok {
		return node, nil
	}

	if int64(offset)+BTreePageSize > int64(len(bt.mmap)) {
		return nil, errors.New("read beyond mmap bounds")
	}

	buf := bt.mmap[offset : offset+BTreePageSize]

	nodeType := buf[0]
	count := binary.LittleEndian.Uint32(buf[3:7])
	isLeaf := nodeType == BTreeNodeLeaf

	node := &BTreeNode{
		IsLeaf:   isLeaf,
		Count:    int(count),
		Keys:     make([][]byte, 0, count),
		Values:   make([]uint64, 0, count),
		Children: make([]uint64, 0, count+1),
		Offset:   offset,
		Dirty:    false,
	}

	pos := 7
	for i := 0; i < int(count); i++ {
		keyLen := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		pos += 4
		keyBuf := make([]byte, keyLen)
		copy(keyBuf, buf[pos:pos+keyLen])
		node.Keys = append(node.Keys, keyBuf)
		pos += keyLen
	}

	for i := 0; i < int(count); i++ {
		node.Values = append(node.Values, binary.LittleEndian.Uint64(buf[pos:pos+8]))
		pos += 8
	}

	if !isLeaf {
		for i := 0; i <= int(count); i++ {
			node.Children = append(node.Children, binary.LittleEndian.Uint64(buf[pos:pos+8]))
			pos += 8
		}
	} else {
		node.NextLeaf = binary.LittleEndian.Uint64(buf[pos : pos+8])
	}

	bt.cacheNode(node)

	return node, nil
}

func (bt *BTree) cacheNode(node *BTreeNode) {
	if len(bt.cacheList) >= BTreeCacheSize {
		old := bt.cacheList[0]
		delete(bt.cache, old)
		bt.cacheList = bt.cacheList[1:]
	}
	bt.cache[node.Offset] = node
	bt.cacheList = append(bt.cacheList, node.Offset)
}

func (bt *BTree) Insert(key []byte, value uint64) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return err
	}

	if root.Count >= BTreeMaxKeys {
		newRoot := bt.newNode(false)
		newRoot.Children = append(newRoot.Children, bt.rootOff)
		bt.splitChild(newRoot, 0, root)
		bt.rootOff = newRoot.Offset
		root = newRoot
	}

	bt.insertNonFull(root, key, value)

	return nil
}

func (bt *BTree) insertNonFull(node *BTreeNode, key []byte, value uint64) {
	if node.IsLeaf {
		i := 0
		for i < node.Count && bytes.Compare(node.Keys[i], key) < 0 {
			i++
		}
		if i < node.Count && bytes.Compare(node.Keys[i], key) == 0 {
			node.Values[i] = value
			bt.writeNode(node)
			return
		}
		node.Keys = append(node.Keys, nil)
		node.Values = append(node.Values, 0)
		copy(node.Keys[i+1:], node.Keys[i:])
		copy(node.Values[i+1:], node.Values[i:])
		node.Keys[i] = make([]byte, len(key))
		copy(node.Keys[i], key)
		node.Values[i] = value
		node.Count++
		bt.writeNode(node)
	} else {
		i := 0
		for i < node.Count && bytes.Compare(node.Keys[i], key) < 0 {
			i++
		}
		child, err := bt.readNode(node.Children[i])
		if err != nil {
			return
		}
		if child.Count >= BTreeMaxKeys {
			bt.splitChild(node, i, child)
			if bytes.Compare(key, node.Keys[i]) > 0 {
				i++
			}
		}
		child, _ = bt.readNode(node.Children[i])
		bt.insertNonFull(child, key, value)
		bt.writeNode(child)
	}
}

func (bt *BTree) splitChild(parent *BTreeNode, i int, child *BTreeNode) {
	mid := child.Count / 2

	newNode := bt.newNode(child.IsLeaf)
	newNode.Count = child.Count - mid - 1

	for j := 0; j < newNode.Count; j++ {
		newNode.Keys = append(newNode.Keys, child.Keys[mid+1+j])
		newNode.Values = append(newNode.Values, child.Values[mid+1+j])
	}

	if !child.IsLeaf {
		for j := 0; j <= newNode.Count; j++ {
			newNode.Children = append(newNode.Children, child.Children[mid+1+j])
		}
	} else {
		newNode.NextLeaf = child.NextLeaf
		child.NextLeaf = newNode.Offset
	}

	child.Count = mid

	parent.Keys = append(parent.Keys, nil)
	parent.Values = append(parent.Values, 0)
	parent.Children = append(parent.Children, 0)
	copy(parent.Keys[i+1:], parent.Keys[i:])
	copy(parent.Values[i+1:], parent.Values[i:])
	copy(parent.Children[i+1:], parent.Children[i:])
	parent.Keys[i] = child.Keys[mid]
	parent.Values[i] = child.Values[mid]
	parent.Children[i] = child.Offset
	parent.Children[i+1] = newNode.Offset
	parent.Count++

	bt.writeNode(child)
	bt.writeNode(newNode)
	bt.writeNode(parent)
}

func (bt *BTree) Get(key []byte) (uint64, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return 0, err
	}

	return bt.search(root, key)
}

func (bt *BTree) Put(key []byte, value uint64) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return err
	}

	bt.deleteKey(root, key)
	bt.insertNonFull(root, key, value)
	bt.writeNode(root)

	return nil
}

func (bt *BTree) deleteKey(node *BTreeNode, key []byte) {
	i := 0
	for i < node.Count && bytes.Compare(key, node.Keys[i]) > 0 {
		i++
	}

	if i < node.Count && bytes.Compare(key, node.Keys[i]) == 0 {
		if node.IsLeaf {
			copy(node.Keys[i:], node.Keys[i+1:])
			copy(node.Values[i:], node.Values[i+1:])
			node.Keys = node.Keys[:node.Count-1]
			node.Values = node.Values[:node.Count-1]
			node.Count--
			node.Dirty = true
			return
		}
	}

	if node.IsLeaf {
		return
	}

	child, _ := bt.readNode(node.Children[i])
	bt.deleteKey(child, key)
}

func (bt *BTree) search(node *BTreeNode, key []byte) (uint64, error) {
	i := 0
	for i < node.Count && bytes.Compare(key, node.Keys[i]) > 0 {
		i++
	}

	if i < node.Count && bytes.Compare(key, node.Keys[i]) == 0 {
		return node.Values[i], nil
	}

	if node.IsLeaf {
		return 0, ErrKeyNotFound
	}

	child, err := bt.readNode(node.Children[i])
	if err != nil {
		return 0, err
	}

	return bt.search(child, key)
}

func (bt *BTree) Delete(key []byte) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return err
	}

	bt.deleteFromNode(root, key)
	return nil
}

func (bt *BTree) deleteFromNode(node *BTreeNode, key []byte) {
	i := 0
	for i < node.Count && bytes.Compare(key, node.Keys[i]) > 0 {
		i++
	}

	if i < node.Count && bytes.Compare(key, node.Keys[i]) == 0 {
		if node.IsLeaf {
			copy(node.Keys[i:], node.Keys[i+1:])
			copy(node.Values[i:], node.Values[i+1:])
			node.Keys = node.Keys[:node.Count-1]
			node.Values = node.Values[:node.Count-1]
			node.Count--
			bt.writeNode(node)
			return
		}
	}

	if node.IsLeaf {
		return
	}

	child, err := bt.readNode(node.Children[i])
	if err != nil || child == nil {
		return
	}
	bt.deleteFromNode(child, key)
	bt.writeNode(child)
}

func (bt *BTree) Scan(fn func(key []byte, value uint64) bool) error {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return err
	}

	return bt.scanNode(root, fn)
}

func (bt *BTree) scanNode(node *BTreeNode, fn func([]byte, uint64) bool) error {
	type frame struct {
		node     *BTreeNode
		childIdx int
	}
	stack := make([]frame, 0, 64)
	stack = append(stack, frame{node: node, childIdx: 0})

	for len(stack) > 0 {
		f := &stack[len(stack)-1]
		if f.node.IsLeaf {
			for f.childIdx < f.node.Count {
				if !fn(f.node.Keys[f.childIdx], f.node.Values[f.childIdx]) {
					return nil
				}
				f.childIdx++
			}
			stack = stack[:len(stack)-1]
			continue
		}

		if f.childIdx <= f.node.Count {
			child, err := bt.readNode(f.node.Children[f.childIdx])
			if err != nil {
				return err
			}
			f.childIdx++
			stack = append(stack, frame{node: child, childIdx: 0})
		} else {
			stack = stack[:len(stack)-1]
		}
	}
	return nil
}

func (bt *BTree) Close() error {
	return nil
}

func (bt *BTree) Sync() error {
	return bt.file.Sync()
}

func (bt *BTree) RootOffset() uint64 {
	return bt.rootOff
}

func (bt *BTree) SetRootOffset(off uint64) {
	bt.rootOff = off
}

type btreeMapIndex struct {
	bt *BTree
}

func newBtreeMapIndex(db *database, rootOff uint64) (*btreeMapIndex, error) {
	bt, err := db.openBTree(rootOff)
	if err != nil {
		return nil, err
	}
	return &btreeMapIndex{bt: bt}, nil
}

func (b *btreeMapIndex) Set(key []byte, value []byte) {
	off := binary.BigEndian.Uint64(value)
	b.bt.Insert(key, off)
}

func (b *btreeMapIndex) Get(key []byte) ([]byte, bool) {
	off, err := b.bt.Get(key)
	if err != nil {
		return nil, false
	}
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, off)
	return val, true
}

func (b *btreeMapIndex) Delete(key []byte) {
	b.bt.Delete(key)
}

func (b *btreeMapIndex) Range(fn func(k []byte, v []byte) bool) {
	b.bt.Scan(func(key []byte, value uint64) bool {
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, value)
		return fn(key, val)
	})
}

func (b *btreeMapIndex) Size() int {
	count := 0
	b.bt.Scan(func(k []byte, v uint64) bool {
		count++
		return true
	})
	return count
}

func (b *btreeMapIndex) RootOffset() uint64 {
	return b.bt.RootOffset()
}

func (b *btreeMapIndex) SetRootOffset(off uint64) {
	b.bt.SetRootOffset(off)
}

type btreeUint32MapIndex struct {
	bt       *BTree
	keySize  int
	dataSize int
}

func newBtreeUint32MapIndex(db *database, rootOff uint64) (*btreeUint32MapIndex, error) {
	bt, err := db.openBTree(rootOff)
	if err != nil {
		return nil, err
	}
	return &btreeUint32MapIndex{
		bt:       bt,
		keySize:  16,
		dataSize: 256,
	}, nil
}

func serializeUint32Slice(values []uint32) []byte {
	if values == nil {
		values = []uint32{}
	}
	buf := make([]byte, 4+len(values)*4)
	binary.LittleEndian.PutUint32(buf, uint32(len(values)))
	for i, v := range values {
		binary.LittleEndian.PutUint32(buf[4+i*4:], v)
	}
	return buf
}

func deserializeUint32Slice(data []byte) []uint32 {
	if data == nil || len(data) < 4 {
		return []uint32{}
	}
	count := binary.LittleEndian.Uint32(data)
	if count == 0 || len(data) < 4+int(count)*4 {
		return []uint32{}
	}
	values := make([]uint32, count)
	for i := uint32(0); i < count; i++ {
		values[i] = binary.LittleEndian.Uint32(data[4+i*4:])
	}
	return values
}

func (b *btreeUint32MapIndex) Set(key string, value uint32) {
	k := []byte(key)

	var existing []uint32
	packedVal, err := b.bt.Get(k)
	if err == nil && packedVal != 0 {
		existing = packedValToSlice(packedVal)
	}

	existing = append(existing, value)
	b.bt.Put(k, sliceToPackedVal(existing))
}

func (b *btreeUint32MapIndex) Get(key string) (uint32, bool) {
	values, ok := b.GetAll(key)
	if !ok || len(values) == 0 {
		return 0, false
	}
	return values[0], true
}

func (b *btreeUint32MapIndex) GetAll(key string) ([]uint32, bool) {
	k := []byte(key)
	packedVal, err := b.bt.Get(k)
	if err != nil {
		return nil, false
	}
	return packedValToSlice(packedVal), true
}

func (b *btreeUint32MapIndex) Delete(key string) {
	b.bt.Delete([]byte(key))
}

func (b *btreeUint32MapIndex) Range(fn func(k string, v uint32) bool) {
	b.bt.Scan(func(key []byte, packedVal uint64) bool {
		values := packedValToSlice(packedVal)
		if len(values) > 0 {
			return fn(string(key), values[0])
		}
		return true
	})
}

func (b *btreeUint32MapIndex) SortedKeys() []string {
	keys := make([]string, 0)
	b.bt.Scan(func(key []byte, packedVal uint64) bool {
		keys = append(keys, string(key))
		return true
	})
	slices.Sort(keys)
	return keys
}

func (b *btreeUint32MapIndex) Size() int {
	count := 0
	b.bt.Scan(func(k []byte, v uint64) bool {
		count++
		return true
	})
	return count
}

func (b *btreeUint32MapIndex) RootOffset() uint64 {
	return b.bt.RootOffset()
}

func (b *btreeUint32MapIndex) SetRootOffset(off uint64) {
	b.bt.SetRootOffset(off)
}

func packedValToSlice(packed uint64) []uint32 {
	if packed == 0 {
		return []uint32{}
	}
	high := uint32(packed >> 32)
	low := uint32(packed & 0xFFFFFFFF)
	count := int(high)
	if count == 0 {
		return []uint32{}
	}
	if count > 10000 {
		count = 10000
	}
	values := make([]uint32, 0, count)
	for i := 0; i < count && i < 1000; i++ {
		values = append(values, low)
	}
	return values
}

func sliceToPackedVal(values []uint32) uint64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) == 1 {
		return uint64(values[0])
	}
	return uint64(len(values))<<32 | uint64(values[0])
}
