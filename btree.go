package embeddb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/yay101/embeddbmmap"
)

const (
	PageSize       = 4096
	PageHeaderSize = 27
	PageCRCSize    = 4
	PageFooterOff  = PageSize - PageCRCSize
	RootCountOff   = 23

	PageTypeLeaf         byte = 1
	PageTypeInternal     byte = 2
	PageTypeRootLeaf     byte = 3
	PageTypeRootInternal byte = 4

	MinLeafKeys = 1
)

var ErrKeyNotFound = errors.New("key not found")

var pageBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, PageSize)
		return &buf
	},
}

type BTree struct {
	db        *database
	alloc     *allocator
	rootOff   uint64
	mu        sync.RWMutex
	cache     map[uint64]*BTreeNode
	cacheMu   sync.RWMutex
	cacheRing []uint64
	cacheHead int
	cacheSize int
	minSize   int
	maxSize   int
	count     int
	hits      uint64
	misses    uint64
	lastAdjust time.Time
	adjustInterval time.Duration
}

type BTreeNode struct {
	IsLeaf   bool
	Count    int
	Keys     [][]byte
	Values   []uint64
	Children []uint64
	PrevLeaf uint64
	NextLeaf uint64
	Offset   uint64
	Dirty    bool
}

func (db *database) openBTree(rootOff uint64) (*BTree, error) {
	cacheSize := 4096
	if db.parent != nil && db.parent.cachePages > 0 {
		cacheSize = db.parent.cachePages
	}
	minSize := 64
	maxSize := 16384
	if cacheSize > 0 {
		minSize = cacheSize / 4
		if minSize < 64 {
			minSize = 64
		}
		maxSize = cacheSize * 4
		if maxSize > 16384 {
			maxSize = 16384
		}
	}
	bt := &BTree{
		db:             db,
		alloc:          db.alloc,
		rootOff:        rootOff,
		cache:          make(map[uint64]*BTreeNode, cacheSize),
		cacheRing:      make([]uint64, cacheSize),
		cacheHead:      0,
		cacheSize:      cacheSize,
		minSize:        minSize,
		maxSize:        maxSize,
		lastAdjust:     time.Now(),
		adjustInterval: 5 * time.Second,
	}

	if bt.rootOff == 0 {
		root, err := bt.newNode(true)
		if err != nil {
			return nil, err
		}
		bt.rootOff = root.Offset
		bt.writeNode(root)
	} else {
		root, err := bt.readNode(bt.rootOff)
		if err != nil {
			return nil, fmt.Errorf("openBTree: read root at %d: %w", bt.rootOff, err)
		}
		bt.count = bt.readCount(root)
	}

	return bt, nil
}

func (bt *BTree) readCount(node *BTreeNode) int {
	if node.Offset == bt.rootOff {
		r := bt.db.region.Load()
		if r != nil {
			r.RLock()
			page := bt.pageData(node.Offset)
			isRoot := page[0] == PageTypeRootLeaf || page[0] == PageTypeRootInternal
			var count uint32
			if isRoot {
				count = binary.LittleEndian.Uint32(page[RootCountOff : RootCountOff+4])
			}
			r.RUnlock()
			if isRoot {
				return int(count)
			}
		} else {
			buf := make([]byte, PageSize)
			if err := bt.db.readAt(buf, int64(node.Offset)); err == nil {
				isRoot := buf[0] == PageTypeRootLeaf || buf[0] == PageTypeRootInternal
				if isRoot {
					count := binary.LittleEndian.Uint32(buf[RootCountOff : RootCountOff+4])
					return int(count)
				}
			}
		}
	}
	return 0
}

func (bt *BTree) pageData(offset uint64) []byte {
	r := bt.db.region.Load()
	if r == nil {
		buf := make([]byte, PageSize)
		bt.db.readAt(buf, int64(offset))
		return buf
	}
	base := r.Pointer()
	return unsafe.Slice((*byte)(unsafe.Add(base, int(offset))), PageSize)
}

func (bt *BTree) newNode(isLeaf bool) (*BTreeNode, error) {
	node := &BTreeNode{
		IsLeaf: isLeaf,
		Count:  0,
		Keys:   make([][]byte, 0),
		Dirty:  true,
	}

	if isLeaf {
		node.Values = make([]uint64, 0)
	} else {
		node.Children = make([]uint64, 0)
	}

	off, _, err := bt.alloc.Allocate(PageSize)
	if err != nil {
		return nil, fmt.Errorf("btree allocate: %w", err)
	}
	node.Offset = off

	if err := bt.db.ensureRegion(int64(off) + PageSize); err != nil {
		return nil, fmt.Errorf("btree newNode ensureRegion: %w", err)
	}

	buf := make([]byte, PageSize)
	if isLeaf {
		buf[0] = PageTypeLeaf
	} else {
		buf[0] = PageTypeInternal
	}
	checksum := crc32.ChecksumIEEE(buf[:PageFooterOff])
	binary.LittleEndian.PutUint32(buf[PageFooterOff:], checksum)

	r := bt.db.region.Load()
	if r != nil {
		if int64(off)+PageSize > r.Size() {
			return nil, fmt.Errorf("btree newNode: offset %d + PageSize %d > regionSize %d", off, PageSize, r.Size())
		}
		r.RLock()
		base := r.Pointer()
		copy(unsafe.Slice((*byte)(unsafe.Add(base, int(off))), PageSize), buf)
		r.RUnlock()
	} else {
		if err := bt.db.writeAt(buf, int64(off)); err != nil {
			return nil, fmt.Errorf("btree newNode writeAt: %w", err)
		}
	}

	return node, nil
}

func (bt *BTree) wouldOverflow(node *BTreeNode, extraKeyLen int) bool {
	cellOverhead := 2 + extraKeyLen
	if node.IsLeaf {
		cellOverhead += 8
	} else {
		cellOverhead += 8
	}

	currentUsed := PageHeaderSize
	for i := 0; i < node.Count; i++ {
		currentUsed += 2 + len(node.Keys[i])
		if node.IsLeaf {
			currentUsed += 8
		} else {
			currentUsed += 8
		}
	}
	if !node.IsLeaf {
		currentUsed += 8
	}
	currentUsed += PageCRCSize

	return currentUsed+cellOverhead > PageSize
}

func (bt *BTree) serializeNode(node *BTreeNode) []byte {
	buf := make([]byte, PageSize)

	if node.IsLeaf {
		buf[0] = PageTypeLeaf
	} else {
		buf[0] = PageTypeInternal
	}
	binary.LittleEndian.PutUint16(buf[3:5], uint16(node.Count))

	if node.IsLeaf {
		binary.LittleEndian.PutUint64(buf[7:15], node.PrevLeaf)
		binary.LittleEndian.PutUint64(buf[15:23], node.NextLeaf)
	} else if len(node.Children) > 0 {
		binary.LittleEndian.PutUint64(buf[15:23], node.Children[0])
	}

	cellEnd := uint16(PageSize - PageCRCSize)

	if node.IsLeaf {
		for i := node.Count - 1; i >= 0; i-- {
			cellEnd -= bt.encodeLeafCell(buf, cellEnd, node.Keys[i], node.Values[i])
		}
	} else {
		for i := node.Count - 1; i >= 0; i-- {
			cellEnd -= bt.encodeInternalCell(buf, cellEnd, node.Keys[i], node.Children[i+1])
		}
	}

	binary.LittleEndian.PutUint16(buf[5:7], cellEnd)

	if bt.rootOff == node.Offset {
		if node.IsLeaf {
			buf[0] = PageTypeRootLeaf
		} else {
			buf[0] = PageTypeRootInternal
		}
		binary.LittleEndian.PutUint32(buf[RootCountOff:RootCountOff+4], uint32(bt.count))
	}

	checksum := crc32.ChecksumIEEE(buf[:PageFooterOff])
	binary.LittleEndian.PutUint32(buf[PageFooterOff:], checksum)

	return buf
}

func (bt *BTree) encodeLeafCell(buf []byte, end uint16, key []byte, value uint64) uint16 {
	cellSize := 2 + len(key) + 8
	start := end - uint16(cellSize)

	binary.LittleEndian.PutUint16(buf[start:], uint16(len(key)))
	copy(buf[start+2:], key)
	binary.LittleEndian.PutUint64(buf[start+2+uint16(len(key)):], value)

	return uint16(cellSize)
}

func (bt *BTree) encodeInternalCell(buf []byte, end uint16, key []byte, leftChild uint64) uint16 {
	cellSize := 2 + len(key) + 8
	start := end - uint16(cellSize)

	binary.LittleEndian.PutUint16(buf[start:], uint16(len(key)))
	copy(buf[start+2:], key)
	binary.LittleEndian.PutUint64(buf[start+2+uint16(len(key)):], leftChild)

	return uint16(cellSize)
}

func (bt *BTree) writeNode(node *BTreeNode) error {
	if node.Offset == 0 {
		off, _, err := bt.alloc.Allocate(PageSize)
		if err != nil {
			return fmt.Errorf("btree allocate: %w", err)
		}
		node.Offset = off
	}

	if err := bt.db.ensureRegion(int64(node.Offset) + PageSize); err != nil {
		return fmt.Errorf("btree writeNode ensureRegion: %w", err)
	}

	data := bt.serializeNode(node)
	r := bt.db.region.Load()
	if r != nil {
		r.RLock()
		copy(bt.pageData(node.Offset), data)
		r.RUnlock()
	} else {
		if err := bt.db.writeAt(data, int64(node.Offset)); err != nil {
			return fmt.Errorf("btree writeNode writeAt: %w", err)
		}
	}
	bt.cacheNode(node)
	return nil
}

func (bt *BTree) readNode(offset uint64) (*BTreeNode, error) {
	if offset == 0 {
		return nil, errors.New("invalid offset")
	}

	bt.cacheMu.RLock()
	if node, ok := bt.cache[offset]; ok {
		bt.cacheMu.RUnlock()
		bt.trackAccess(true)
		return node, nil
	}
	bt.cacheMu.RUnlock()
	bt.trackAccess(false)

	if err := bt.db.ensureRegion(int64(offset) + PageSize); err != nil {
		return nil, fmt.Errorf("btree readNode ensureRegion: %w", err)
	}

	pageBufPtr := pageBufPool.Get().(*[]byte)
	pageCopy := *pageBufPtr

	r := bt.db.region.Load()
	if r != nil {
		r.RLock()
		page := bt.pageData(offset)
		copy(pageCopy, page)
		r.RUnlock()
	} else {
		if err := bt.db.readAt(pageCopy, int64(offset)); err != nil {
			pageBufPool.Put(pageBufPtr)
			return nil, fmt.Errorf("btree readNode readAt: %w", err)
		}
	}

	storedCRC := binary.LittleEndian.Uint32(pageCopy[PageFooterOff:])
	computedCRC := crc32.ChecksumIEEE(pageCopy[:PageFooterOff])
	if storedCRC != computedCRC {
		filePageBufPtr := pageBufPool.Get().(*[]byte)
		filePageCopy := *filePageBufPtr
		if _, err := bt.db.file.ReadAt(filePageCopy, int64(offset)); err == nil {
			fileCRC := binary.LittleEndian.Uint32(filePageCopy[PageFooterOff:])
			fileComputed := crc32.ChecksumIEEE(filePageCopy[:PageFooterOff])
			if fileCRC == fileComputed {
				copy(pageCopy, filePageCopy)
			} else {
				pageBufPool.Put(filePageBufPtr)
				pageBufPool.Put(pageBufPtr)
				return nil, fmt.Errorf("btree page %d: CRC mismatch (stored=%08x computed=%08x)", offset, storedCRC, computedCRC)
			}
		} else {
			pageBufPool.Put(filePageBufPtr)
			pageBufPool.Put(pageBufPtr)
			return nil, fmt.Errorf("btree page %d: CRC mismatch (stored=%08x computed=%08x)", offset, storedCRC, computedCRC)
		}
		pageBufPool.Put(filePageBufPtr)
	}

	nodeType := pageCopy[0]
	count := int(binary.LittleEndian.Uint16(pageCopy[3:5]))
	cellStart := binary.LittleEndian.Uint16(pageCopy[5:7])

	isLeaf := nodeType == PageTypeLeaf || nodeType == PageTypeRootLeaf

	node := &BTreeNode{
		IsLeaf: isLeaf,
		Count:  count,
		Keys:   make([][]byte, 0, count),
		Offset: offset,
		Dirty:  false,
	}

	if isLeaf {
		node.Values = make([]uint64, 0, count)
		node.PrevLeaf = binary.LittleEndian.Uint64(pageCopy[7:15])
		node.NextLeaf = binary.LittleEndian.Uint64(pageCopy[15:23])
	} else {
		node.Children = make([]uint64, 0, count+1)
		node.Children = append(node.Children, binary.LittleEndian.Uint64(pageCopy[15:23]))
	}

	pos := int(cellStart)
	for i := 0; i < count; i++ {
		if pos+2 > PageFooterOff {
			break
		}
		keyLen := int(binary.LittleEndian.Uint16(pageCopy[pos : pos+2]))
		pos += 2
		if pos+keyLen > PageFooterOff {
			break
		}
		key := make([]byte, keyLen)
		copy(key, pageCopy[pos:pos+keyLen])
		node.Keys = append(node.Keys, key)
		pos += keyLen

		if isLeaf {
			if pos+8 > PageFooterOff {
				break
			}
			node.Values = append(node.Values, binary.LittleEndian.Uint64(pageCopy[pos:pos+8]))
			pos += 8
		} else {
			if pos+8 > PageFooterOff {
				break
			}
			childPage := binary.LittleEndian.Uint64(pageCopy[pos : pos+8])
			node.Children = append(node.Children, childPage)
			pos += 8
		}
	}

	node.Count = len(node.Keys)

	if nodeType == PageTypeRootLeaf || nodeType == PageTypeRootInternal {
		bt.count = int(binary.LittleEndian.Uint32(pageCopy[RootCountOff : RootCountOff+4]))
	}

	pageBufPool.Put(pageBufPtr)

	bt.cacheNode(node)
	return node, nil
}

func (bt *BTree) cacheNode(node *BTreeNode) {
	bt.cacheMu.Lock()
	defer bt.cacheMu.Unlock()
	if _, exists := bt.cache[node.Offset]; exists {
		bt.cache[node.Offset] = node
		return
	}
	if node.Offset == bt.rootOff {
		bt.cache[node.Offset] = node
		return
	}
	if len(bt.cache) >= bt.cacheSize {
		for i := 0; i < bt.cacheSize; i++ {
			idx := (bt.cacheHead + i) % bt.cacheSize
			old := bt.cacheRing[idx]
			if old != 0 && old != bt.rootOff {
				delete(bt.cache, old)
				bt.cacheRing[idx] = node.Offset
				bt.cache[node.Offset] = node
				bt.cacheHead = (idx + 1) % bt.cacheSize
				return
			}
		}
	}
	bt.cacheRing[bt.cacheHead] = node.Offset
	bt.cache[node.Offset] = node
	bt.cacheHead = (bt.cacheHead + 1) % len(bt.cacheRing)
}

func (bt *BTree) trackAccess(hit bool) {
	if hit {
		bt.hits++
	} else {
		bt.misses++
	}

	if time.Since(bt.lastAdjust) < bt.adjustInterval {
		return
	}

	total := bt.hits + bt.misses
	if total < 100 {
		return
	}

	hitRate := float64(bt.hits) / float64(total)

	if hitRate > 0.90 && bt.cacheSize < bt.maxSize {
		newSize := bt.cacheSize * 3 / 2
		if newSize > bt.maxSize {
			newSize = bt.maxSize
		}
		bt.resizeCache(newSize)
	} else if hitRate < 0.30 && bt.cacheSize > bt.minSize {
		newSize := bt.cacheSize / 2
		if newSize < bt.minSize {
			newSize = bt.minSize
		}
		bt.resizeCache(newSize)
	}

	bt.hits = 0
	bt.misses = 0
	bt.lastAdjust = time.Now()
}

func (bt *BTree) resizeCache(newSize int) {
	if newSize == bt.cacheSize {
		return
	}

	oldCache := bt.cache
	oldRing := bt.cacheRing
	oldSize := bt.cacheSize

	bt.cache = make(map[uint64]*BTreeNode, newSize)
	bt.cacheRing = make([]uint64, newSize)
	bt.cacheSize = newSize
	bt.cacheHead = 0

	keep := newSize
	if keep > len(oldCache) {
		keep = len(oldCache)
	}

	n := 0
	for i := 0; i < oldSize && n < keep; i++ {
		idx := (oldSize - 1 - i)
		if idx < 0 || idx >= len(oldRing) {
			continue
		}
		off := oldRing[idx]
		if off == 0 || off == bt.rootOff {
			continue
		}
		if node, ok := oldCache[off]; ok {
			bt.cache[off] = node
			bt.cacheRing[n] = off
			n++
		}
	}

	if bt.rootOff != 0 {
		if node, ok := oldCache[bt.rootOff]; ok {
			bt.cache[bt.rootOff] = node
		}
	}

	bt.cacheHead = n
}

func (bt *BTree) Insert(key []byte, value uint64) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return err
	}

	if bt.wouldOverflow(root, len(key)) {
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

	inserted, err := bt.insertNonFull(root, key, value)
	if err != nil {
		return err
	}

	if inserted {
		bt.count++
	}

	return nil
}

func (bt *BTree) Put(key []byte, value uint64) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return err
	}

	if bt.wouldOverflow(root, len(key)) {
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

	_, err = bt.insertNonFull(root, key, value)
	return err
}

func (bt *BTree) insertNonFull(node *BTreeNode, key []byte, value uint64) (bool, error) {
	if node.IsLeaf {
		i := sort.Search(node.Count, func(j int) bool {
			return bytes.Compare(node.Keys[j], key) >= 0
		})
		if i < node.Count && bytes.Equal(node.Keys[i], key) {
			node.Values[i] = value
			return false, bt.writeNode(node)
		}
		node.Keys = append(node.Keys, nil)
		node.Values = append(node.Values, 0)
		copy(node.Keys[i+1:], node.Keys[i:])
		copy(node.Values[i+1:], node.Values[i:])
		node.Keys[i] = make([]byte, len(key))
		copy(node.Keys[i], key)
		node.Values[i] = value
		node.Count++
		return true, bt.writeNode(node)
	}

	i := sort.Search(node.Count, func(j int) bool {
		return bytes.Compare(node.Keys[j], key) > 0
	})

	child, err := bt.readNode(node.Children[i])
	if err != nil {
		return false, err
	}

	if bt.wouldOverflow(child, len(key)) {
		if err := bt.splitChild(node, i, child); err != nil {
			return false, err
		}
		if i < node.Count && bytes.Compare(key, node.Keys[i]) >= 0 {
			i++
		}
		child, err = bt.readNode(node.Children[i])
		if err != nil {
			return false, err
		}
	}
	return bt.insertNonFull(child, key, value)
}

func (bt *BTree) splitChild(parent *BTreeNode, idx int, child *BTreeNode) error {
	// In a B+ tree:
	// - Leaf split: promoted key is copied up (stays in leaf), right sibling gets keys[mid..]
	// - Internal split: promoted key is pushed up (removed from children)
	mid := child.Count / 2

	// For leaf: right gets keys[mid..] (mid is duplicated as separator in parent)
	// For internal: right gets keys[mid+1..] (mid is promoted to parent, not in children)

	splitPoint := mid
	if !child.IsLeaf {
		splitPoint = mid + 1 // skip the promoted key for internal nodes
	}

	newNode, err := bt.newNode(child.IsLeaf)
	if err != nil {
		return err
	}

	rightCount := child.Count - splitPoint
	if child.IsLeaf {
		rightCount = child.Count - mid
	}

	newNode.Count = rightCount

	if child.IsLeaf {
		for j := 0; j < rightCount; j++ {
			newNode.Keys = append(newNode.Keys, child.Keys[mid+j])
			newNode.Values = append(newNode.Values, child.Values[mid+j])
		}
		newNode.PrevLeaf = child.Offset
		newNode.NextLeaf = child.NextLeaf
		child.NextLeaf = newNode.Offset

		// Promoted key is a copy of the first key in the right sibling
		promoteKey := make([]byte, len(child.Keys[mid]))
		copy(promoteKey, child.Keys[mid])

		child.Keys = child.Keys[:mid]
		child.Values = child.Values[:mid]
		child.Count = mid

		// Insert promoteKey into parent at position idx
		parent.Keys = append(parent.Keys, nil)
		parent.Children = append(parent.Children, 0)
		copy(parent.Keys[idx+1:], parent.Keys[idx:])
		copy(parent.Children[idx+2:], parent.Children[idx+1:])
		parent.Keys[idx] = promoteKey
		parent.Children[idx] = child.Offset
		parent.Children[idx+1] = newNode.Offset
		parent.Count++
	} else {
		// Internal node: promote key at mid, split children around it
		promoteKey := make([]byte, len(child.Keys[mid]))
		copy(promoteKey, child.Keys[mid])

		for j := 0; j < child.Count-mid-1; j++ {
			newNode.Keys = append(newNode.Keys, child.Keys[mid+1+j])
		}
		for j := 0; j < len(child.Children)-mid-1; j++ {
			newNode.Children = append(newNode.Children, child.Children[mid+1+j])
		}
		newNode.Count = child.Count - mid - 1

		// Trim left child
		leftChildren := make([]uint64, mid+1)
		copy(leftChildren, child.Children[:mid+1])
		child.Keys = child.Keys[:mid]
		child.Children = leftChildren
		child.Count = mid

		// Insert promoteKey into parent
		parent.Keys = append(parent.Keys, nil)
		parent.Children = append(parent.Children, 0)
		copy(parent.Keys[idx+1:], parent.Keys[idx:])
		copy(parent.Children[idx+2:], parent.Children[idx+1:])
		parent.Keys[idx] = promoteKey
		parent.Children[idx] = child.Offset
		parent.Children[idx+1] = newNode.Offset
		parent.Count++
	}

	if err := bt.writeNode(child); err != nil {
		return err
	}
	if err := bt.writeNode(newNode); err != nil {
		return err
	}
	return bt.writeNode(parent)
}

func (bt *BTree) Get(key []byte) (uint64, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.searchUnlocked(key)
}

func (bt *BTree) searchUnlocked(key []byte) (uint64, error) {
	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return 0, err
	}
	result, err := bt.searchNode(root, key)
	return result, err
}

func (bt *BTree) searchNode(node *BTreeNode, key []byte) (uint64, error) {
	if node.IsLeaf {
		i := sort.Search(node.Count, func(j int) bool {
			return bytes.Compare(node.Keys[j], key) >= 0
		})
		if i < node.Count && bytes.Equal(node.Keys[i], key) {
			return node.Values[i], nil
		}
		return 0, ErrKeyNotFound
	}

	i := sort.Search(node.Count, func(j int) bool {
		return bytes.Compare(node.Keys[j], key) > 0
	})

	if i >= len(node.Children) {
		return 0, fmt.Errorf("searchNode: internal node at %d has Count=%d but Children=%d, i=%d", node.Offset, node.Count, len(node.Children), i)
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

	_, err := bt.searchUnlocked(key)
	if err != nil {
		return nil
	}

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return err
	}

	bt.deleteFromNode(root, key)
	bt.count--

	root, err = bt.readNode(bt.rootOff)
	if err == nil && !root.IsLeaf && root.Count == 0 {
		if len(root.Children) > 0 {
			bt.rootOff = root.Children[0]
		}
	}

	return nil
}

func (bt *BTree) deleteFromNode(node *BTreeNode, key []byte) {
	if node.IsLeaf {
		i := sort.Search(node.Count, func(j int) bool {
			return bytes.Compare(node.Keys[j], key) >= 0
		})
		if i < node.Count && bytes.Equal(node.Keys[i], key) {
			copy(node.Keys[i:], node.Keys[i+1:])
			copy(node.Values[i:], node.Values[i+1:])
			node.Keys = node.Keys[:node.Count-1]
			node.Values = node.Values[:node.Count-1]
			node.Count--
			bt.writeNode(node)
		}
		return
	}

	childIdx := sort.Search(node.Count, func(j int) bool {
		return bytes.Compare(node.Keys[j], key) > 0
	})

	child, err := bt.readNode(node.Children[childIdx])
	if err != nil || child == nil {
		return
	}

	bt.deleteFromNode(child, key)

	if childIdx > 0 && child.Count > 0 && bytes.Compare(node.Keys[childIdx-1], key) == 0 {
		newSep := make([]byte, len(child.Keys[0]))
		copy(newSep, child.Keys[0])
		node.Keys[childIdx-1] = newSep
		bt.writeNode(node)
	}

	bt.rebalance(node, childIdx)
}

func (bt *BTree) rebalance(parent *BTreeNode, childIdx int) {
	child, err := bt.readNode(parent.Children[childIdx])
	if err != nil || child == nil {
		return
	}

	minKeys := MinLeafKeys
	if !child.IsLeaf {
		minKeys = 1
	}
	if child.Count >= minKeys {
		return
	}

	if parent.Count == 0 {
		return
	}

	var leftSibling *BTreeNode
	var rightSibling *BTreeNode

	if childIdx > 0 {
		leftSibling, _ = bt.readNode(parent.Children[childIdx-1])
	}
	if childIdx < parent.Count {
		rightSibling, _ = bt.readNode(parent.Children[childIdx+1])
	}

	if leftSibling != nil && leftSibling.Count > minKeys {
		bt.borrowFromLeft(parent, childIdx, child, leftSibling)
		return
	}

	if rightSibling != nil && rightSibling.Count > minKeys {
		bt.borrowFromRight(parent, childIdx, child, rightSibling)
		return
	}

	if leftSibling != nil {
		bt.mergeNodes(parent, childIdx-1, leftSibling, child)
	} else if rightSibling != nil {
		bt.mergeNodes(parent, childIdx, child, rightSibling)
	}
}

func (bt *BTree) borrowFromLeft(parent *BTreeNode, childIdx int, child *BTreeNode, left *BTreeNode) {
	parentKey := parent.Keys[childIdx-1]

	if child.IsLeaf {
		child.Keys = append([][]byte{left.Keys[left.Count-1]}, child.Keys...)
		child.Values = append([]uint64{left.Values[left.Count-1]}, child.Values...)
		child.Count++

		parent.Keys[childIdx-1] = make([]byte, len(left.Keys[left.Count-1]))
		copy(parent.Keys[childIdx-1], left.Keys[left.Count-1])

		left.Keys = left.Keys[:left.Count-1]
		left.Values = left.Values[:left.Count-1]
		left.Count--
	} else {
		child.Keys = append([][]byte{nil}, child.Keys...)
		copy(child.Keys[1:], child.Keys[:child.Count])
		child.Keys[0] = parentKey
		child.Children = append([]uint64{0}, child.Children...)
		copy(child.Children[1:], child.Children[:len(child.Children)-1])
		child.Children[0] = left.Children[left.Count]
		child.Count++

		parent.Keys[childIdx-1] = left.Keys[left.Count-1]
		left.Keys = left.Keys[:left.Count-1]
		left.Children = left.Children[:left.Count]
		left.Count--
	}

	bt.writeNode(left)
	bt.writeNode(child)
	bt.writeNode(parent)
}

func (bt *BTree) borrowFromRight(parent *BTreeNode, childIdx int, child *BTreeNode, right *BTreeNode) {
	parentKey := parent.Keys[childIdx]

	if child.IsLeaf {
		child.Keys = append(child.Keys, right.Keys[0])
		child.Values = append(child.Values, right.Values[0])
		child.Count++

		parent.Keys[childIdx] = make([]byte, len(right.Keys[0]))
		copy(parent.Keys[childIdx], right.Keys[0])

		copy(right.Keys, right.Keys[1:])
		copy(right.Values, right.Values[1:])
		right.Keys = right.Keys[:right.Count-1]
		right.Values = right.Values[:right.Count-1]
		right.Count--
	} else {
		child.Keys = append(child.Keys, parentKey)
		child.Children = append(child.Children, right.Children[0])
		child.Count++

		parent.Keys[childIdx] = right.Keys[0]

		copy(right.Children, right.Children[1:])
		right.Children = right.Children[:right.Count]
		copy(right.Keys, right.Keys[1:])
		right.Keys = right.Keys[:right.Count-1]
		right.Count--
	}

	bt.writeNode(right)
	bt.writeNode(child)
	bt.writeNode(parent)
}

func (bt *BTree) mergeNodes(parent *BTreeNode, leftIdx int, left *BTreeNode, right *BTreeNode) {
	parentKey := parent.Keys[leftIdx]

	if left.IsLeaf {
		left.Keys = append(left.Keys, right.Keys...)
		left.Values = append(left.Values, right.Values...)
		left.Count += right.Count
		left.NextLeaf = right.NextLeaf
	} else {
		left.Keys = append(left.Keys, parentKey)
		left.Keys = append(left.Keys, right.Keys...)
		left.Children = append(left.Children, right.Children...)
		left.Count += right.Count + 1
	}

	copy(parent.Keys[leftIdx:], parent.Keys[leftIdx+1:])
	copy(parent.Children[leftIdx+1:], parent.Children[leftIdx+2:])
	parent.Keys = parent.Keys[:parent.Count-1]
	parent.Children = parent.Children[:parent.Count]
	parent.Count--

	bt.writeNode(left)
	bt.writeNode(parent)
}

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
	keyCopy := make([]byte, len(node.Keys[node.Count-1]))
	copy(keyCopy, node.Keys[node.Count-1])
	return keyCopy, node.Values[node.Count-1]
}

func (bt *BTree) Scan(fn func(key []byte, value uint64) bool) error {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return fmt.Errorf("scan: read root: %w", err)
	}

	node := root
	for !node.IsLeaf {
		if node.Count == 0 {
			return nil
		}
		childOff := node.Children[0]
		if childOff == 0 {
			return fmt.Errorf("scan: internal node at %d has zero child[0]", node.Offset)
		}
		node, err = bt.readNode(childOff)
		if err != nil {
			return fmt.Errorf("scan: read child: %w", err)
		}
	}

	for node != nil {
		for i := 0; i < node.Count; i++ {
			if i >= len(node.Keys) || i >= len(node.Values) {
				return fmt.Errorf("scan: leaf at %d has Count=%d but Keys=%d Values=%d", node.Offset, node.Count, len(node.Keys), len(node.Values))
			}
			if !fn(node.Keys[i], node.Values[i]) {
				return nil
			}
		}
		if node.NextLeaf == 0 {
			break
		}
		node, err = bt.readNode(node.NextLeaf)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bt *BTree) ScanRange(startKey, endKey []byte, fn func(key []byte, value uint64) bool) error {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	root, err := bt.readNode(bt.rootOff)
	if err != nil {
		return fmt.Errorf("scan range: read root: %w", err)
	}

	node, startIdx, err := bt.findLeafPosition(root, startKey)
	if err != nil {
		return fmt.Errorf("scan range: find start: %w", err)
	}

	for node != nil {
		for i := startIdx; i < node.Count; i++ {
			if i >= len(node.Keys) || i >= len(node.Values) {
				break
			}
			cmp := bytes.Compare(node.Keys[i], endKey)
			if cmp > 0 {
				return nil
			}
			if !fn(node.Keys[i], node.Values[i]) {
				return nil
			}
		}
		startIdx = 0
		if node.NextLeaf == 0 {
			break
		}
		node, err = bt.readNode(node.NextLeaf)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bt *BTree) findLeafPosition(node *BTreeNode, key []byte) (*BTreeNode, int, error) {
	for !node.IsLeaf {
		i := sort.Search(node.Count, func(j int) bool {
			return bytes.Compare(node.Keys[j], key) > 0
		})
		if i >= len(node.Children) {
			return nil, 0, fmt.Errorf("findLeafPosition: internal node at %d has Count=%d but Children=%d, i=%d", node.Offset, node.Count, len(node.Children), i)
		}
		child, err := bt.readNode(node.Children[i])
		if err != nil {
			return nil, 0, err
		}
		node = child
	}

	i := sort.Search(node.Count, func(j int) bool {
		return bytes.Compare(node.Keys[j], key) >= 0
	})

	return node, i, nil
}

func (bt *BTree) Close() error {
	return nil
}

type CacheStats struct {
	Size    int
	Filled  int
	Hits    uint64
	Misses  uint64
	HitRate float64
}

func (bt *BTree) GetCacheStats() CacheStats {
	bt.cacheMu.RLock()
	defer bt.cacheMu.RUnlock()
	total := bt.hits + bt.misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(bt.hits) / float64(total)
	}
	return CacheStats{
		Size:    bt.cacheSize,
		Filled:  len(bt.cache),
		Hits:    bt.hits,
		Misses:  bt.misses,
		HitRate: hitRate,
	}
}

func (bt *BTree) BulkInsert(entries []struct{ key []byte; value uint64 }) error {
	if len(entries) == 0 {
		return nil
	}

	bt.mu.Lock()
	defer bt.mu.Unlock()

	sorted := make([]struct{ key []byte; value uint64 }, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].key, sorted[j].key) < 0
	})

	deduped := sorted[:0]
	for i, e := range sorted {
		if i > 0 && bytes.Equal(e.key, sorted[i-1].key) {
			continue
		}
		deduped = append(deduped, e)
	}

	bt.count += len(deduped)

	leafNodes, err := bt.buildLeafNodes(deduped)
	if err != nil {
		return err
	}

	if len(leafNodes) == 1 {
		bt.rootOff = leafNodes[0].Offset
		bt.writeNode(leafNodes[0])
		return nil
	}

	root, err := bt.buildInternalNodes(leafNodes)
	if err != nil {
		return err
	}
	bt.rootOff = root.Offset
	bt.writeNode(root)

	return nil
}

func (bt *BTree) buildLeafNodes(entries []struct{ key []byte; value uint64 }) ([]*BTreeNode, error) {
	var nodes []*BTreeNode
	var current *BTreeNode

	for _, e := range entries {
		if current == nil {
			var err error
			current, err = bt.newNode(true)
			if err != nil {
				return nil, err
			}
		}

		if current.Count > 0 && bt.wouldOverflow(current, len(e.key)) {
			nodes = append(nodes, current)
			var err error
			current, err = bt.newNode(true)
			if err != nil {
				return nil, err
			}
		}

		current.Keys = append(current.Keys, e.key)
		current.Values = append(current.Values, e.value)
		current.Count++
	}

	if current != nil && current.Count > 0 {
		nodes = append(nodes, current)
	}

	for i, n := range nodes {
		if i > 0 {
			n.PrevLeaf = nodes[i-1].Offset
		}
		if i < len(nodes)-1 {
			n.NextLeaf = nodes[i+1].Offset
		}
		bt.writeNode(n)
	}

	return nodes, nil
}

func (bt *BTree) buildInternalNodes(children []*BTreeNode) (*BTreeNode, error) {
	if len(children) <= 1 {
		return children[0], nil
	}

	var nextLevel []*BTreeNode
	for i := 0; i < len(children); {
		node, err := bt.newNode(false)
		if err != nil {
			return nil, err
		}

		node.Children = append(node.Children, children[i].Offset)
		i++

		for i < len(children) && !bt.wouldOverflowInternal(node, len(children[i].Keys[0])) {
			promoteKey := make([]byte, len(children[i].Keys[0]))
			copy(promoteKey, children[i].Keys[0])
			node.Keys = append(node.Keys, promoteKey)
			node.Children = append(node.Children, children[i].Offset)
			node.Count++
			i++
		}

		bt.writeNode(node)
		nextLevel = append(nextLevel, node)
	}

	if len(nextLevel) == 1 {
		return nextLevel[0], nil
	}

	return bt.buildInternalNodes(nextLevel)
}

func (bt *BTree) wouldOverflowInternal(node *BTreeNode, extraKeyLen int) bool {
	cellOverhead := 2 + extraKeyLen + 8
	currentUsed := PageHeaderSize
	for i := 0; i < node.Count; i++ {
		currentUsed += 2 + len(node.Keys[i]) + 8
	}
	currentUsed += 8 + PageCRCSize
	return currentUsed+cellOverhead > PageSize
}

func (bt *BTree) Sync() error {
	if r := bt.db.region.Load(); r != nil {
		return r.Sync(embeddbmmap.SyncSync)
	}
	return nil
}

func (bt *BTree) RootOffset() uint64 {
	return bt.rootOff
}

func (bt *BTree) SetRootOffset(off uint64) {
	bt.rootOff = off
}

func (bt *BTree) Verify() error {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	type kv struct {
		key   []byte
		value uint64
	}
	var entries []kv
	bt.Scan(func(key []byte, value uint64) bool {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		entries = append(entries, kv{keyCopy, value})
		return true
	})

	missingCount := 0
	for _, entry := range entries {
		found, err := bt.Get(entry.key)
		if err != nil || found != entry.value {
			missingCount++
		}
	}
	if missingCount > 0 {
		return fmt.Errorf("btree verify: %d/%d keys not found via Get", missingCount, len(entries))
	}
	return nil
}
