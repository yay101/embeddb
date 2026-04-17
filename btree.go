package embeddb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"unsafe"

	"github.com/yay101/embeddbmmap"
)

const (
	BTreePageSize     = 4096
	BTreeMaxKeys      = (BTreePageSize - 36) / 40
	BTreeMagic        = 0x42545245
	BTreeNodeInternal = 1
	BTreeNodeLeaf     = 2
	BTreeCacheSize    = 2048
)

var ErrKeyNotFound = errors.New("key not found")

// pagePool reuses page-sized buffers for writeNode to reduce GC pressure.
var pagePool = sync.Pool{
	New: func() any {
		buf := make([]byte, BTreePageSize)
		return &buf
	},
}

// findKeyIndex returns the index of the first key >= target using binary search.
// If the target matches keys[i] exactly, i is returned and the caller can check equality.
func findKeyIndex(keys [][]byte, count int, target []byte) int {
	return sort.Search(count, func(i int) bool {
		return bytes.Compare(keys[i], target) >= 0
	})
}

type BTree struct {
	db        *database
	region    *embeddbmmap.MappedRegion
	alloc     *allocator
	rootOff   uint64
	mu        sync.RWMutex
	cacheMu   sync.Mutex // protects cache and cacheRing independently of mu
	cache     map[uint64]*BTreeNode
	cacheRing []uint64 // ring buffer for eviction order
	cacheHead int      // next write position in ring
	count     int      // number of keys in the tree
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
		db:        db,
		region:    db.region,
		alloc:     db.alloc,
		rootOff:   rootOff,
		cache:     make(map[uint64]*BTreeNode, BTreeCacheSize),
		cacheRing: make([]uint64, BTreeCacheSize),
		cacheHead: 0,
	}

	if bt.rootOff == 0 {
		root, err := bt.newNode(true)
		if err != nil {
			return nil, err
		}
		bt.rootOff = root.Offset
		bt.writeNode(root)
	}

	if rootOff != 0 {
		bt.count = 0
		bt.Scan(func(k []byte, v uint64) bool {
			bt.count++
			return true
		})
	}

	return bt, nil
}

func (bt *BTree) newNode(isLeaf bool) (*BTreeNode, error) {
	node := &BTreeNode{
		IsLeaf:   isLeaf,
		Count:    0,
		Keys:     make([][]byte, 0, BTreeMaxKeys),
		Values:   make([]uint64, 0, BTreeMaxKeys),
		Children: make([]uint64, 0, BTreeMaxKeys+1),
		NextLeaf: 0,
		Dirty:    true,
	}

	off, _, err := bt.alloc.Allocate(BTreePageSize)
	if err != nil {
		return nil, fmt.Errorf("btree new node allocate: %w", err)
	}
	node.Offset = off
	bt.db.ensureRegion(int64(off) + BTreePageSize)
	return node, nil
}

const BTreeNodeSize = BTreePageSize

func (bt *BTree) writeNode(node *BTreeNode) error {
	if node.Offset == 0 {
		off, _, err := bt.alloc.Allocate(BTreePageSize)
		if err != nil {
			return fmt.Errorf("btree write node allocate: %w", err)
		}
		node.Offset = off
	}

	bt.db.ensureRegion(int64(node.Offset) + BTreePageSize)

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

	var buf []byte
	var poolBuf *[]byte
	if alignedSize <= BTreePageSize {
		poolBuf = pagePool.Get().(*[]byte)
		buf = (*poolBuf)[:alignedSize]
		for i := range buf {
			buf[i] = 0
		}
	} else {
		buf = make([]byte, alignedSize)
	}

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

	base := bt.region.Pointer()
	copy(unsafe.Slice((*byte)(unsafe.Add(base, node.Offset)), len(buf)), buf)

	if poolBuf != nil {
		pagePool.Put(poolBuf)
	}
	return nil
}

func (bt *BTree) readNode(offset uint64) (*BTreeNode, error) {
	if offset == 0 {
		return nil, errors.New("invalid offset")
	}

	bt.cacheMu.Lock()
	if node, ok := bt.cache[offset]; ok {
		bt.cacheMu.Unlock()
		return node, nil
	}
	bt.cacheMu.Unlock()

	bt.db.ensureRegion(int64(offset) + BTreePageSize)

	base := bt.region.Pointer()
	buf := unsafe.Slice((*byte)(unsafe.Add(base, int(offset))), BTreePageSize)

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
		key := make([]byte, keyLen)
		copy(key, buf[pos:pos+keyLen])
		node.Keys = append(node.Keys, key)
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
	bt.cacheMu.Lock()
	defer bt.cacheMu.Unlock()
	// If already cached, just update the pointer (no duplicate ring entry needed).
	if _, exists := bt.cache[node.Offset]; exists {
		bt.cache[node.Offset] = node
		return
	}
	// Evict the entry at the current ring position if occupied.
	old := bt.cacheRing[bt.cacheHead]
	if old != 0 {
		delete(bt.cache, old)
	}
	bt.cacheRing[bt.cacheHead] = node.Offset
	bt.cache[node.Offset] = node
	bt.cacheHead = (bt.cacheHead + 1) % BTreeCacheSize
}

func (bt *BTree) Insert(key []byte, value uint64) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Check if key already exists (to decide whether to increment count).
	_, searchErr := bt.searchUnlocked(key)

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return err
	}

	if root.Count >= BTreeMaxKeys {
		newRoot, err := bt.newNode(false)
		if err != nil {
			return err
		}
		newRoot.Children = append(newRoot.Children, bt.rootOff)
		if err := bt.splitChild(newRoot, 0, root); err != nil {
			return err
		}
		bt.rootOff = newRoot.Offset
		root = newRoot
	}

	if err := bt.insertNonFull(root, key, value); err != nil {
		return err
	}

	if searchErr != nil {
		bt.count++
	}

	return nil
}

func (bt *BTree) insertNonFull(node *BTreeNode, key []byte, value uint64) error {
	if node.IsLeaf {
		i := findKeyIndex(node.Keys, node.Count, key)
		if i < node.Count && bytes.Equal(node.Keys[i], key) {
			node.Values[i] = value
			bt.writeNode(node)
			return nil
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
		return nil
	}

	i := findKeyIndex(node.Keys, node.Count, key)
	if i < node.Count && bytes.Equal(node.Keys[i], key) {
		node.Values[i] = value
		bt.writeNode(node)
		return nil
	}
	child, err := bt.readNode(node.Children[i])
	if err != nil {
		return err
	}
	if child.Count >= BTreeMaxKeys {
		if err := bt.splitChild(node, i, child); err != nil {
			return err
		}
		cmp := bytes.Compare(key, node.Keys[i])
		if cmp > 0 {
			i++
		} else if cmp == 0 {
			node.Values[i] = value
			bt.writeNode(node)
			return nil
		}
	}
	child, err = bt.readNode(node.Children[i])
	if err != nil {
		return err
	}
	return bt.insertNonFull(child, key, value)
}

func (bt *BTree) splitChild(parent *BTreeNode, i int, child *BTreeNode) error {
	mid := child.Count / 2

	newNode, err := bt.newNode(child.IsLeaf)
	if err != nil {
		return err
	}
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
	return nil
}

func (bt *BTree) Get(key []byte) (uint64, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	return bt.searchUnlocked(key)
}

func (bt *BTree) Put(key []byte, value uint64) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Check if key already exists (to decide whether to increment count).
	_, existsErr := bt.searchUnlocked(key)

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return err
	}

	// Handle full root by splitting, same as Insert.
	if root.Count >= BTreeMaxKeys {
		newRoot, err := bt.newNode(false)
		if err != nil {
			return err
		}
		newRoot.Children = append(newRoot.Children, bt.rootOff)
		if err := bt.splitChild(newRoot, 0, root); err != nil {
			return err
		}
		bt.rootOff = newRoot.Offset
		root = newRoot
	}

	// insertNonFull handles both insert and update (at any node level).
	if err := bt.insertNonFull(root, key, value); err != nil {
		return err
	}

	if existsErr != nil {
		bt.count++ // new key
	}

	return nil
}

// searchUnlocked performs a search without acquiring the lock (caller must hold it).
func (bt *BTree) searchUnlocked(key []byte) (uint64, error) {
	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return 0, err
	}
	return bt.searchNode(root, key)
}

func (bt *BTree) searchNode(node *BTreeNode, key []byte) (uint64, error) {
	i := findKeyIndex(node.Keys, node.Count, key)

	if i < node.Count && bytes.Equal(node.Keys[i], key) {
		return node.Values[i], nil
	}

	if node.IsLeaf {
		return 0, ErrKeyNotFound
	}

	child, err := bt.readNode(node.Children[i])
	if err != nil {
		return 0, err
	}

	return bt.searchNode(child, key)
}

func (bt *BTree) Delete(key []byte) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Check if key exists before deletion to maintain count.
	_, err := bt.searchUnlocked(key)
	if err != nil {
		return nil // key doesn't exist, nothing to delete
	}

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return err
	}

	bt.deleteFromNode(root, key)
	bt.count--
	return nil
}

func (bt *BTree) deleteFromNode(node *BTreeNode, key []byte) {
	i := findKeyIndex(node.Keys, node.Count, key)

	if i < node.Count && bytes.Equal(node.Keys[i], key) {
		if node.IsLeaf {
			copy(node.Keys[i:], node.Keys[i+1:])
			copy(node.Values[i:], node.Values[i+1:])
			node.Keys = node.Keys[:node.Count-1]
			node.Values = node.Values[:node.Count-1]
			node.Count--
			bt.writeNode(node)
			return
		}
		// Internal node: replace with in-order predecessor (rightmost key
		// in the left subtree), then delete the predecessor from that subtree.
		predKey, predVal := bt.findMax(node.Children[i])
		if predKey != nil {
			node.Keys[i] = predKey
			node.Values[i] = predVal
			bt.writeNode(node)
			child, err := bt.readNode(node.Children[i])
			if err == nil && child != nil {
				bt.deleteFromNode(child, predKey)
			}
		}
		return
	}

	if node.IsLeaf {
		return
	}

	child, err := bt.readNode(node.Children[i])
	if err != nil || child == nil {
		return
	}
	bt.deleteFromNode(child, key)
}

// findMax returns the rightmost (maximum) key and value in the subtree
// rooted at the given offset.
func (bt *BTree) findMax(offset uint64) ([]byte, uint64) {
	node, err := bt.readNode(offset)
	if err != nil || node == nil || node.Count == 0 {
		return nil, 0
	}
	for !node.IsLeaf {
		child, err := bt.readNode(node.Children[node.Count])
		if err != nil || child == nil {
			break
		}
		node = child
	}
	if node.Count == 0 {
		return nil, 0
	}
	// Return a copy of the key to avoid mutation issues.
	keyCopy := make([]byte, len(node.Keys[node.Count-1]))
	copy(keyCopy, node.Keys[node.Count-1])
	return keyCopy, node.Values[node.Count-1]
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

		// In-order traversal: child[0], key[0], child[1], key[1], ..., key[n-1], child[n]
		// childIdx tracks how many children we've visited so far.
		// After visiting child[k], we emit key[k] before visiting child[k+1].
		if f.childIdx <= f.node.Count {
			// Emit key[childIdx-1] if we just finished a child (childIdx > 0)
			if f.childIdx > 0 && f.childIdx-1 < f.node.Count {
				if !fn(f.node.Keys[f.childIdx-1], f.node.Values[f.childIdx-1]) {
					return nil
				}
			}
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
	if bt.region != nil {
		return bt.region.Sync(embeddbmmap.SyncSync)
	}
	return nil
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
	return b.bt.count
}

func (b *btreeMapIndex) RootOffset() uint64 {
	return b.bt.RootOffset()
}

func (b *btreeMapIndex) SetRootOffset(off uint64) {
	b.bt.SetRootOffset(off)
}

type btreeOffsetMapIndex struct {
	bt *BTree
}

func newBtreeOffsetMapIndex(db *database, rootOff uint64) (*btreeOffsetMapIndex, error) {
	bt, err := db.openBTree(rootOff)
	if err != nil {
		return nil, err
	}
	return &btreeOffsetMapIndex{
		bt: bt,
	}, nil
}

func (b *btreeOffsetMapIndex) Set(key string, value uint64) {
	k := make([]byte, len(key)+8)
	copy(k, key)
	binary.BigEndian.PutUint64(k[len(key):], value)
	b.bt.Insert(k, value)
}

func (b *btreeOffsetMapIndex) Get(key string) (uint64, bool) {
	prefix := []byte(key)
	var result uint64
	found := false
	b.bt.Scan(func(k []byte, v uint64) bool {
		if len(k) >= len(prefix)+8 && bytes.Equal(k[:len(prefix)], prefix) {
			result = v
			found = true
			return false
		}
		if bytes.Compare(k[:min(len(k), len(prefix))], prefix) > 0 {
			return false
		}
		return true
	})
	return result, found
}

func (b *btreeOffsetMapIndex) GetAll(key string) ([]uint64, bool) {
	prefix := []byte(key)
	var results []uint64
	b.bt.Scan(func(k []byte, v uint64) bool {
		if len(k) >= len(prefix)+8 && bytes.Equal(k[:len(prefix)], prefix) {
			results = append(results, v)
			return true
		}
		if bytes.Compare(k[:min(len(k), len(prefix))], prefix) > 0 {
			return false
		}
		return true
	})
	if len(results) == 0 {
		return nil, false
	}
	return results, true
}

func (b *btreeOffsetMapIndex) Delete(key string) {
	prefix := []byte(key)
	var toDelete [][]byte
	b.bt.Scan(func(k []byte, v uint64) bool {
		if len(k) >= len(prefix)+8 && bytes.Equal(k[:len(prefix)], prefix) {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			toDelete = append(toDelete, keyCopy)
			return true
		}
		if bytes.Compare(k[:min(len(k), len(prefix))], prefix) > 0 {
			return false
		}
		return true
	})
	for _, k := range toDelete {
		b.bt.Delete(k)
	}
}

func (b *btreeOffsetMapIndex) Range(fn func(k string, v uint64) bool) {
	b.bt.Scan(func(key []byte, v uint64) bool {
		if len(key) < 8 {
			return true
		}
		origKey := string(key[:len(key)-8])
		return fn(origKey, v)
	})
}

func (b *btreeOffsetMapIndex) SortedKeys() []string {
	seen := make(map[string]struct{})
	keys := make([]string, 0)
	b.bt.Scan(func(key []byte, v uint64) bool {
		if len(key) < 8 {
			return true
		}
		origKey := string(key[:len(key)-8])
		if _, exists := seen[origKey]; !exists {
			seen[origKey] = struct{}{}
			keys = append(keys, origKey)
		}
		return true
	})
	return keys
}

func (b *btreeOffsetMapIndex) Size() int {
	return b.bt.count
}

func (b *btreeOffsetMapIndex) RootOffset() uint64 {
	return b.bt.RootOffset()
}

func (b *btreeOffsetMapIndex) SetRootOffset(off uint64) {
	b.bt.SetRootOffset(off)
}
