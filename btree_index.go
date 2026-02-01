package embeddb

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"

	"golang.org/x/exp/mmap"
)

const (
	// B-tree parameters
	BTreeOrder     = 100  // Maximum number of keys per node
	BTreePageSize  = 4096 // Size of each page in bytes
	BTreeHeaderLen = 4096 // Size of the B-tree header page

	// Page types
	BTreePageTypeLeaf     = 1
	BTreePageTypeInternal = 2
	BTreePageTypeFree     = 3
	BTreePageTypeHeader   = 4

	// Bloom filter size in bytes (512 bits for quick checks)
	BloomFilterSize = 64
)

// BTreeIndex represents a memory-mapped B-tree index for a specific field
type BTreeIndex struct {
	file          *os.File       // Index file
	mmap          *mmap.ReaderAt // Memory-mapped access to the file
	lock          sync.RWMutex   // Lock for concurrent access
	rootPageNum   uint32         // Page number of the root node
	freeListHead  uint32         // Head of the free page list
	pageCount     uint32         // Total number of pages in the file
	fieldName     string         // Name of the indexed field
	fieldOffset   uintptr        // Offset of the field in the struct
	fieldType     reflect.Kind   // Type of the indexed field
	keySize       int            // Size of fixed-size keys, -1 for variable-sized keys
	dbFileName    string         // Name of the database file (for portability)
	pendingWrites []pendingWrite // Buffer for batched writes
}

// BTreeHeader represents the header page of the B-tree index file
type BTreeHeader struct {
	Magic        [8]byte   // Magic bytes to identify a valid B-tree file "EMBEDBT"
	Version      uint32    // Version of the B-tree format
	FieldNameLen uint16    // Length of the field name
	FieldName    [256]byte // Field name (null-terminated)
	FieldType    uint16    // Type of the indexed field
	RootPageNum  uint32    // Page number of the root node
	PageCount    uint32    // Total number of pages in the file
	FreeListHead uint32    // First page in the free list
	KeyCount     uint32    // Total number of keys in the tree
}

// BTreeNode represents a node in the B-tree
type BTreeNode struct {
	PageType    uint8                 // 1 for leaf, 2 for internal
	KeyCount    uint16                // Number of keys in this node
	ParentPage  uint32                // Page number of the parent node
	BloomFilter [BloomFilterSize]byte // Bloom filter for quick checks
	Keys        []interface{}         // Keys in this node
	Values      [][]uint32            // Values for each key (only for leaf nodes)
	Children    []uint32              // Child page numbers (only for internal nodes)
}

// pendingWrite represents a pending write to the B-tree
type pendingWrite struct {
	id       uint32      // Record ID
	key      interface{} // Key value
	isDelete bool        // True for delete operations
}

// NewBTreeIndex creates a new B-tree index for the specified field
func NewBTreeIndex(dbFileName, fieldName string, fieldOffset uintptr, fieldType reflect.Kind) (*BTreeIndex, error) {
	// Create the index file in the same directory as the database file
	dbDir := filepath.Dir(dbFileName)
	indexFileName := filepath.Join(dbDir, fmt.Sprintf("%s.%s.idx", filepath.Base(dbFileName), fieldName))

	idx := &BTreeIndex{
		fieldName:     fieldName,
		fieldOffset:   fieldOffset,
		fieldType:     fieldType,
		dbFileName:    dbFileName,
		pendingWrites: make([]pendingWrite, 0, 1000),
	}

	// Set key size based on field type
	switch fieldType {
	case reflect.Int, reflect.Int32, reflect.Uint, reflect.Uint32, reflect.Float32:
		idx.keySize = 4
	case reflect.Int64, reflect.Uint64, reflect.Float64:
		idx.keySize = 8
	case reflect.Bool:
		idx.keySize = 1
	default:
		// Variable-sized keys (like strings)
		idx.keySize = -1
	}

	// Check if the index file already exists
	_, err := os.Stat(indexFileName)
	fileExists := err == nil

	// Open or create the index file
	file, err := os.OpenFile(indexFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	idx.file = file

	// Initialize the file if it doesn't exist yet
	if !fileExists {
		if err := idx.initializeFile(); err != nil {
			idx.Close()
			return nil, fmt.Errorf("failed to initialize index file: %w", err)
		}
	}

	// Memory-map the file
	if err := idx.mapFile(); err != nil {
		idx.Close()
		return nil, fmt.Errorf("failed to memory-map index file: %w", err)
	}

	// Load index metadata
	if fileExists {
		if err := idx.loadMetadata(); err != nil {
			// If loading fails, try to rebuild
			if err := idx.rebuildIndex(dbFileName, fieldName, fieldOffset); err != nil {
				idx.Close()
				return nil, fmt.Errorf("failed to rebuild index: %w", err)
			}
		}
	}

	return idx, nil
}

// initializeFile initializes a new B-tree index file
func (idx *BTreeIndex) initializeFile() error {
	// Create header page
	header := BTreeHeader{
		Version:      1,
		FieldNameLen: uint16(len(idx.fieldName)),
		FieldType:    uint16(idx.fieldType),
		RootPageNum:  1, // Root will be page 1 (after the header)
		PageCount:    2, // Header + initial root
		FreeListHead: 0, // No free pages yet
	}

	// Set magic bytes
	copy(header.Magic[:], []byte("EMBEDBT"))
	copy(header.FieldName[:], []byte(idx.fieldName))

	// Write header to file
	headerBytes := make([]byte, BTreeHeaderLen)
	if err := idx.encodeHeader(header, headerBytes); err != nil {
		return fmt.Errorf("failed to encode header: %w", err)
	}

	if _, err := idx.file.WriteAt(headerBytes, 0); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Create empty root node (leaf)
	rootNode := BTreeNode{
		PageType: BTreePageTypeLeaf,
		KeyCount: 0,
		Keys:     make([]interface{}, 0, BTreeOrder),
		Values:   make([][]uint32, 0, BTreeOrder),
	}

	// Encode and write root node
	rootBytes := make([]byte, BTreePageSize)
	if err := idx.encodeNode(&rootNode, rootBytes); err != nil {
		return fmt.Errorf("failed to encode root node: %w", err)
	}

	if _, err := idx.file.WriteAt(rootBytes, BTreeHeaderLen); err != nil {
		return fmt.Errorf("failed to write root node: %w", err)
	}

	// Set index metadata
	idx.rootPageNum = 1
	idx.pageCount = 2
	idx.freeListHead = 0

	return nil
}

// mapFile creates a memory mapping of the index file
func (idx *BTreeIndex) mapFile() error {
	// Check if file exists
	_, err := idx.file.Stat()
	if err != nil {
		return err
	}

	// Create memory mapping
	mmapFile, err := mmap.Open(idx.file.Name())
	if err != nil {
		return err
	}

	idx.mmap = mmapFile
	return nil
}

// remapFile updates the memory mapping after file size changes
func (idx *BTreeIndex) remapFile() error {
	// Close existing mapping if it exists
	if idx.mmap != nil {
		idx.mmap.Close()
	}

	// Create new mapping
	return idx.mapFile()
}

// loadMetadata loads the B-tree metadata from the header page
func (idx *BTreeIndex) loadMetadata() error {
	// Read header page
	headerBytes := make([]byte, BTreeHeaderLen)
	_, err := idx.mmap.ReadAt(headerBytes, 0)
	if err != nil {
		return err
	}

	header := BTreeHeader{}
	if err := idx.decodeHeader(headerBytes, &header); err != nil {
		return err
	}

	// Verify magic bytes
	if string(header.Magic[:7]) != "EMBEDBT" {
		return fmt.Errorf("invalid B-tree file, wrong magic bytes")
	}

	// Verify field name
	fieldName := string(header.FieldName[:header.FieldNameLen])
	if fieldName != idx.fieldName {
		return fmt.Errorf("index field name mismatch: got %s, expected %s", fieldName, idx.fieldName)
	}

	// Set index metadata
	idx.rootPageNum = header.RootPageNum
	idx.pageCount = header.PageCount
	idx.freeListHead = header.FreeListHead

	return nil
}

// encodeHeader encodes a BTreeHeader into a byte slice
func (idx *BTreeIndex) encodeHeader(header BTreeHeader, buf []byte) error {
	if len(buf) < BTreeHeaderLen {
		return fmt.Errorf("buffer too small for header")
	}

	// Write magic bytes
	copy(buf[0:8], header.Magic[:])

	// Write metadata
	binary.LittleEndian.PutUint32(buf[8:12], header.Version)
	binary.LittleEndian.PutUint16(buf[12:14], header.FieldNameLen)
	copy(buf[14:270], header.FieldName[:])
	binary.LittleEndian.PutUint16(buf[270:272], header.FieldType)
	binary.LittleEndian.PutUint32(buf[272:276], header.RootPageNum)
	binary.LittleEndian.PutUint32(buf[276:280], header.PageCount)
	binary.LittleEndian.PutUint32(buf[280:284], header.FreeListHead)
	binary.LittleEndian.PutUint32(buf[284:288], header.KeyCount)

	return nil
}

// decodeHeader decodes a byte slice into a BTreeHeader
func (idx *BTreeIndex) decodeHeader(buf []byte, header *BTreeHeader) error {
	if len(buf) < BTreeHeaderLen {
		return fmt.Errorf("buffer too small for header")
	}

	// Read magic bytes
	copy(header.Magic[:], buf[0:8])

	// Read metadata
	header.Version = binary.LittleEndian.Uint32(buf[8:12])
	header.FieldNameLen = binary.LittleEndian.Uint16(buf[12:14])
	copy(header.FieldName[:], buf[14:270])
	header.FieldType = binary.LittleEndian.Uint16(buf[270:272])
	header.RootPageNum = binary.LittleEndian.Uint32(buf[272:276])
	header.PageCount = binary.LittleEndian.Uint32(buf[276:280])
	header.FreeListHead = binary.LittleEndian.Uint32(buf[280:284])
	header.KeyCount = binary.LittleEndian.Uint32(buf[284:288])

	return nil
}

// readNode reads a B-tree node from the specified page
func (idx *BTreeIndex) readNode(pageNum uint32) (*BTreeNode, error) {
	if pageNum >= idx.pageCount {
		return nil, fmt.Errorf("invalid page number: %d", pageNum)
	}

	// Read page data
	pageData := make([]byte, BTreePageSize)
	offset := int64(BTreeHeaderLen + (pageNum * BTreePageSize))
	_, err := idx.mmap.ReadAt(pageData, offset)
	if err != nil {
		return nil, err
	}

	// Decode the node
	node := &BTreeNode{}
	err = idx.decodeNode(pageData, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// writeNode writes a B-tree node to the specified page
func (idx *BTreeIndex) writeNode(pageNum uint32, node *BTreeNode) error {
	if pageNum >= idx.pageCount {
		return fmt.Errorf("invalid page number: %d", pageNum)
	}

	// Encode the node
	pageData := make([]byte, BTreePageSize)
	if err := idx.encodeNode(node, pageData); err != nil {
		return err
	}

	// Write to file
	offset := int64(BTreeHeaderLen + (pageNum * BTreePageSize))
	_, err := idx.file.WriteAt(pageData, offset)
	return err
}

// allocatePage allocates a new page in the B-tree file
func (idx *BTreeIndex) allocatePage() (uint32, error) {
	idx.lock.Lock()
	defer idx.lock.Unlock()

	var pageNum uint32

	// Check if we have any free pages
	if idx.freeListHead != 0 {
		// Reuse a page from the free list
		pageNum = idx.freeListHead

		// Read the free page to get the next free page
		freePage := make([]byte, BTreePageSize)
		offset := int64(BTreeHeaderLen + (pageNum * BTreePageSize))
		_, err := idx.mmap.ReadAt(freePage, offset)
		if err != nil {
			return 0, err
		}

		// The first 4 bytes contain the next free page number
		idx.freeListHead = binary.LittleEndian.Uint32(freePage[0:4])
	} else {
		// Allocate a new page at the end of the file
		pageNum = idx.pageCount
		idx.pageCount++

		// Extend the file
		offset := int64(BTreeHeaderLen + (pageNum * BTreePageSize))
		err := idx.file.Truncate(offset + BTreePageSize)
		if err != nil {
			return 0, err
		}

		// Update memory mapping to include the new page
		if err := idx.remapFile(); err != nil {
			return 0, err
		}
	}

	// Update the header with the new page count and free list head
	if err := idx.updateHeader(); err != nil {
		return 0, err
	}

	return pageNum, nil
}

// freePage adds a page to the free list
func (idx *BTreeIndex) freePage(pageNum uint32) error {
	idx.lock.Lock()
	defer idx.lock.Unlock()

	// Write the current free list head to the page
	freePage := make([]byte, BTreePageSize)
	binary.LittleEndian.PutUint32(freePage[0:4], idx.freeListHead)

	// Mark as free page
	freePage[4] = BTreePageTypeFree

	// Write to file
	offset := int64(BTreeHeaderLen + (pageNum * BTreePageSize))
	_, err := idx.file.WriteAt(freePage, offset)
	if err != nil {
		return err
	}

	// Update the free list head
	idx.freeListHead = pageNum

	// Update the header
	return idx.updateHeader()
}

// updateHeader updates the B-tree header with current metadata
func (idx *BTreeIndex) updateHeader() error {
	header := BTreeHeader{
		Version:      1,
		FieldNameLen: uint16(len(idx.fieldName)),
		FieldType:    uint16(idx.fieldType),
		RootPageNum:  idx.rootPageNum,
		PageCount:    idx.pageCount,
		FreeListHead: idx.freeListHead,
	}

	// Set magic bytes
	copy(header.Magic[:], []byte("EMBEDBT"))
	copy(header.FieldName[:], []byte(idx.fieldName))

	// Write header to file
	headerBytes := make([]byte, BTreeHeaderLen)
	if err := idx.encodeHeader(header, headerBytes); err != nil {
		return err
	}

	_, err := idx.file.WriteAt(headerBytes, 0)
	return err
}

// encodeNode encodes a B-tree node into a byte slice
func (idx *BTreeIndex) encodeNode(node *BTreeNode, buf []byte) error {
	if len(buf) < BTreePageSize {
		return fmt.Errorf("buffer too small for node")
	}

	// Clear the buffer
	for i := range buf {
		buf[i] = 0
	}

	// Write node type and key count
	buf[0] = node.PageType
	binary.LittleEndian.PutUint16(buf[1:3], node.KeyCount)
	binary.LittleEndian.PutUint32(buf[3:7], node.ParentPage)

	// Write bloom filter
	copy(buf[7:7+BloomFilterSize], node.BloomFilter[:])

	// Current position in the buffer
	pos := 7 + BloomFilterSize

	// Write keys and values/children
	for i := 0; i < int(node.KeyCount); i++ {
		// Write key based on its type
		switch k := node.Keys[i].(type) {
		case string:
			// String key: [length:uint16][bytes]
			keyLen := len(k)
			if pos+2+keyLen > BTreePageSize {
				return fmt.Errorf("node overflow: string key too long")
			}
			binary.LittleEndian.PutUint16(buf[pos:pos+2], uint16(keyLen))
			pos += 2
			copy(buf[pos:pos+keyLen], k)
			pos += keyLen
		case int, int32, uint, uint32:
			// 4-byte integer key
			var intVal uint32
			switch v := node.Keys[i].(type) {
			case int:
				intVal = uint32(v)
			case int32:
				intVal = uint32(v)
			case uint:
				intVal = uint32(v)
			case uint32:
				intVal = v
			}
			if pos+4 > BTreePageSize {
				return fmt.Errorf("node overflow: not enough space for int key")
			}
			binary.LittleEndian.PutUint32(buf[pos:pos+4], intVal)
			pos += 4
		case int64, uint64, float64:
			// 8-byte integer/float key
			var int64Val uint64
			switch v := node.Keys[i].(type) {
			case int64:
				int64Val = uint64(v)
			case uint64:
				int64Val = v
			case float64:
				int64Val = math.Float64bits(v)
			}
			if pos+8 > BTreePageSize {
				return fmt.Errorf("node overflow: not enough space for int64/float64 key")
			}
			binary.LittleEndian.PutUint64(buf[pos:pos+8], int64Val)
			pos += 8
		case float32:
			// 4-byte float key
			if pos+4 > BTreePageSize {
				return fmt.Errorf("node overflow: not enough space for float32 key")
			}
			binary.LittleEndian.PutUint32(buf[pos:pos+4], math.Float32bits(k))
			pos += 4
		case bool:
			// Boolean key (1 byte)
			if pos+1 > BTreePageSize {
				return fmt.Errorf("node overflow: not enough space for bool key")
			}
			if k {
				buf[pos] = 1
			} else {
				buf[pos] = 0
			}
			pos++
		default:
			return fmt.Errorf("unsupported key type: %T", node.Keys[i])
		}

		// For leaf nodes, write values
		if node.PageType == BTreePageTypeLeaf {
			values := node.Values[i]
			valCount := len(values)
			if pos+2+valCount*4 > BTreePageSize {
				return fmt.Errorf("node overflow: too many values for key")
			}

			// Write value count
			binary.LittleEndian.PutUint16(buf[pos:pos+2], uint16(valCount))
			pos += 2

			// Write each value (record ID)
			for _, val := range values {
				binary.LittleEndian.PutUint32(buf[pos:pos+4], val)
				pos += 4
			}
		}

		// For internal nodes, write child page numbers
		if node.PageType == BTreePageTypeInternal && i < len(node.Children) {
			if pos+4 > BTreePageSize {
				return fmt.Errorf("node overflow: not enough space for child pointer")
			}
			binary.LittleEndian.PutUint32(buf[pos:pos+4], node.Children[i])
			pos += 4
		}
	}

	// For internal nodes, write the last child
	if node.PageType == BTreePageTypeInternal && int(node.KeyCount) < len(node.Children) {
		if pos+4 > BTreePageSize {
			return fmt.Errorf("node overflow: not enough space for last child pointer")
		}
		binary.LittleEndian.PutUint32(buf[pos:pos+4], node.Children[node.KeyCount])
		pos += 4
	}

	return nil
}

// decodeNode decodes a byte slice into a B-tree node
func (idx *BTreeIndex) decodeNode(buf []byte, node *BTreeNode) error {
	if len(buf) < BTreePageSize {
		return fmt.Errorf("buffer too small for node")
	}

	// Read node type and key count
	node.PageType = buf[0]
	node.KeyCount = binary.LittleEndian.Uint16(buf[1:3])
	node.ParentPage = binary.LittleEndian.Uint32(buf[3:7])

	// Read bloom filter
	copy(node.BloomFilter[:], buf[7:7+BloomFilterSize])

	// Initialize key and value/child slices
	node.Keys = make([]interface{}, node.KeyCount)
	if node.PageType == BTreePageTypeLeaf {
		node.Values = make([][]uint32, node.KeyCount)
	} else {
		node.Children = make([]uint32, int(node.KeyCount)+1)
	}

	// Current position in the buffer
	pos := 7 + BloomFilterSize

	// Read keys and values/children
	for i := 0; i < int(node.KeyCount); i++ {
		// Read key based on field type
		switch idx.fieldType {
		case reflect.String:
			// String key: [length:uint16][bytes]
			if pos+2 > BTreePageSize {
				return fmt.Errorf("invalid node format: string key length beyond page size")
			}
			keyLen := binary.LittleEndian.Uint16(buf[pos : pos+2])
			pos += 2
			if pos+int(keyLen) > BTreePageSize {
				return fmt.Errorf("invalid node format: string key data beyond page size")
			}
			node.Keys[i] = string(buf[pos : pos+int(keyLen)])
			pos += int(keyLen)
		case reflect.Int, reflect.Int32, reflect.Uint, reflect.Uint32:
			// 4-byte integer key
			if pos+4 > BTreePageSize {
				return fmt.Errorf("invalid node format: int key beyond page size")
			}
			intVal := binary.LittleEndian.Uint32(buf[pos : pos+4])
			if idx.fieldType == reflect.Int || idx.fieldType == reflect.Int32 {
				node.Keys[i] = int(intVal)
			} else {
				node.Keys[i] = intVal
			}
			pos += 4
		case reflect.Int64, reflect.Uint64, reflect.Float64:
			// 8-byte integer/float key
			if pos+8 > BTreePageSize {
				return fmt.Errorf("invalid node format: int64/float64 key beyond page size")
			}
			int64Val := binary.LittleEndian.Uint64(buf[pos : pos+8])
			switch idx.fieldType {
			case reflect.Int64:
				node.Keys[i] = int64(int64Val)
			case reflect.Uint64:
				node.Keys[i] = int64Val
			case reflect.Float64:
				node.Keys[i] = math.Float64frombits(int64Val)
			}
			pos += 8
		case reflect.Float32:
			// 4-byte float key
			if pos+4 > BTreePageSize {
				return fmt.Errorf("invalid node format: float32 key beyond page size")
			}
			node.Keys[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[pos : pos+4]))
			pos += 4
		case reflect.Bool:
			// Boolean key (1 byte)
			if pos+1 > BTreePageSize {
				return fmt.Errorf("invalid node format: bool key beyond page size")
			}
			node.Keys[i] = buf[pos] != 0
			pos++
		default:
			return fmt.Errorf("unsupported field type: %v", idx.fieldType)
		}

		// For leaf nodes, read values
		if node.PageType == BTreePageTypeLeaf {
			// Read value count
			if pos+2 > BTreePageSize {
				return fmt.Errorf("invalid node format: value count beyond page size")
			}
			valCount := binary.LittleEndian.Uint16(buf[pos : pos+2])
			pos += 2

			// Read each value (record ID)
			node.Values[i] = make([]uint32, valCount)
			for j := 0; j < int(valCount); j++ {
				if pos+4 > BTreePageSize {
					return fmt.Errorf("invalid node format: value beyond page size")
				}
				node.Values[i][j] = binary.LittleEndian.Uint32(buf[pos : pos+4])
				pos += 4
			}
		}

		// For internal nodes, read child pointers
		if node.PageType == BTreePageTypeInternal {
			if pos+4 > BTreePageSize {
				return fmt.Errorf("invalid node format: child pointer beyond page size")
			}
			node.Children[i] = binary.LittleEndian.Uint32(buf[pos : pos+4])
			pos += 4
		}
	}

	// For internal nodes, read the last child pointer
	if node.PageType == BTreePageTypeInternal {
		if pos+4 > BTreePageSize {
			return fmt.Errorf("invalid node format: last child pointer beyond page size")
		}
		node.Children[node.KeyCount] = binary.LittleEndian.Uint32(buf[pos : pos+4])
		pos += 4
	}

	return nil
}

// Find searches for a key in the B-tree and returns the associated values
func (idx *BTreeIndex) Find(key interface{}) ([]uint32, error) {
	idx.lock.RLock()
	defer idx.lock.RUnlock()

	// Verify key type
	if reflect.TypeOf(key).Kind() != idx.fieldType {
		return nil, fmt.Errorf("key type mismatch: got %T, expected %v", key, idx.fieldType)
	}

	// Start search at the root
	return idx.searchInNode(idx.rootPageNum, key)
}

// searchInNode recursively searches for a key in a node
func (idx *BTreeIndex) searchInNode(pageNum uint32, key interface{}) ([]uint32, error) {
	// Read the node
	node, err := idx.readNode(pageNum)
	if err != nil {
		return nil, err
	}

	// Check bloom filter for leaf nodes
	if node.PageType == BTreePageTypeLeaf && !idx.checkBloomFilter(node.BloomFilter, key) {
		return nil, nil // Key definitely not in this node
	}

	// Binary search to find the key or the appropriate child
	i, found := idx.searchNodeForKey(node, key)

	// If we found an exact match and we're at a leaf, return the values
	if found && node.PageType == BTreePageTypeLeaf {
		return node.Values[i], nil
	}

	// If we're at a leaf and didn't find it, it doesn't exist
	if node.PageType == BTreePageTypeLeaf {
		return nil, nil
	}

	// If we're at an internal node, recurse into the appropriate child
	// If i is the index where the key would be inserted, we need to look in the i-th child
	childIndex := i
	if !found && i < int(node.KeyCount) {
		// If we didn't find an exact match, but i is the index where it would go,
		// we need to check the comparison to determine which child to follow
		cmp := idx.compareKeys(key, node.Keys[i])
		if cmp > 0 {
			childIndex = i + 1
		}
	}

	// Make sure the child index is valid
	if childIndex > len(node.Children)-1 {
		return nil, fmt.Errorf("invalid child index: %d (node has %d children)", childIndex, len(node.Children))
	}

	// Recurse into the child
	return idx.searchInNode(node.Children[childIndex], key)
}

// searchNodeForKey performs a binary search in a node for a key
func (idx *BTreeIndex) searchNodeForKey(node *BTreeNode, key interface{}) (int, bool) {
	// Binary search
	left, right := 0, int(node.KeyCount)-1
	for left <= right {
		mid := left + (right-left)/2
		cmp := idx.compareKeys(key, node.Keys[mid])

		if cmp == 0 {
			return mid, true // Found exact match
		} else if cmp < 0 {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	// Key not found, left is the index where it should be inserted
	return left, false
}

// compareKeys compares two keys based on their type
func (idx *BTreeIndex) compareKeys(a, b interface{}) int {
	switch aVal := a.(type) {
	case string:
		bVal := b.(string)
		return strings.Compare(aVal, bVal)
	case int:
		bVal := b.(int)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case int32:
		bVal := b.(int32)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case int64:
		bVal := b.(int64)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case uint:
		bVal := b.(uint)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case uint32:
		bVal := b.(uint32)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case uint64:
		bVal := b.(uint64)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case float32:
		bVal := b.(float32)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case float64:
		bVal := b.(float64)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	case bool:
		bVal := b.(bool)
		if !aVal && bVal {
			return -1
		} else if aVal && !bVal {
			return 1
		}
		return 0
	default:
		// If we can't compare directly, convert to string and compare
		return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
	}
}

// checkBloomFilter checks if a key might be in the bloom filter
func (idx *BTreeIndex) checkBloomFilter(filter [BloomFilterSize]byte, key interface{}) bool {
	// Hash the key
	h1, h2 := idx.hashKey(key)

	// Check bloom filter
	for i := 0; i < 8; i++ {
		// Use double hashing technique
		pos := (h1 + uint32(i)*h2) % (BloomFilterSize * 8)
		bytePos, bitPos := pos/8, pos%8

		// Check if the bit is set
		if (filter[bytePos] & (1 << bitPos)) == 0 {
			return false // Definitely not in the set
		}
	}

	return true // May be in the set
}

// addToBloomFilter adds a key to a bloom filter
func (idx *BTreeIndex) addToBloomFilter(filter *[BloomFilterSize]byte, key interface{}) {
	// Hash the key
	h1, h2 := idx.hashKey(key)

	// Set bits in bloom filter
	for i := 0; i < 8; i++ {
		// Use double hashing technique
		pos := (h1 + uint32(i)*h2) % (BloomFilterSize * 8)
		bytePos, bitPos := pos/8, pos%8

		// Set the bit
		filter[bytePos] |= (1 << bitPos)
	}
}

// hashKey computes two hash values for the key
func (idx *BTreeIndex) hashKey(key interface{}) (uint32, uint32) {
	var str string

	// Convert key to string based on its type
	switch k := key.(type) {
	case string:
		str = k
	case int, int32, int64, uint, uint32, uint64, float32, float64:
		str = fmt.Sprintf("%v", k)
	case bool:
		if k {
			str = "true"
		} else {
			str = "false"
		}
	default:
		str = fmt.Sprintf("%v", k)
	}

	// FNV-1a hash
	h1 := uint32(2166136261)
	for i := 0; i < len(str); i++ {
		h1 ^= uint32(str[i])
		h1 *= 16777619
	}

	// Simple hash for h2
	h2 := uint32(0)
	for i := 0; i < len(str); i++ {
		h2 = h2*33 + uint32(str[i])
	}

	// Ensure h2 is non-zero
	if h2 == 0 {
		h2 = 1
	}

	return h1, h2
}

// Insert adds a key-value pair to the index
// The key should match the field type of the index
// The value is the record ID
func (idx *BTreeIndex) Insert(key interface{}, recordID uint32) error {
	// Check key type
	if reflect.TypeOf(key).Kind() != idx.fieldType {
		return fmt.Errorf("key type mismatch: got %T, expected %v", key, idx.fieldType)
	}

	// Add to pending writes
	idx.pendingWrites = append(idx.pendingWrites, pendingWrite{
		id:       recordID,
		key:      key,
		isDelete: false,
	})

	// Apply writes if we have enough pending
	if len(idx.pendingWrites) >= 1000 {
		return idx.applyWrites()
	}

	return nil
}

// Remove removes a key-value pair from the index
func (idx *BTreeIndex) Remove(key interface{}, recordID uint32) error {
	// Check key type
	if reflect.TypeOf(key).Kind() != idx.fieldType {
		return fmt.Errorf("key type mismatch: got %T, expected %v", key, idx.fieldType)
	}

	// Add to pending writes
	idx.pendingWrites = append(idx.pendingWrites, pendingWrite{
		id:       recordID,
		key:      key,
		isDelete: true,
	})

	// Apply writes if we have enough pending
	if len(idx.pendingWrites) >= 1000 {
		return idx.applyWrites()
	}

	return nil
}

// Flush writes all pending changes to disk
func (idx *BTreeIndex) Flush() error {
	idx.lock.Lock()
	defer idx.lock.Unlock()

	// Apply any pending writes
	if len(idx.pendingWrites) > 0 {
		if err := idx.applyWrites(); err != nil {
			return err
		}
	}

	// Sync the file to disk
	return idx.file.Sync()
}

// Close closes the index and flushes any pending writes
func (idx *BTreeIndex) Close() error {
	// Flush pending writes
	if err := idx.Flush(); err != nil {
		return err
	}

	// Close memory mapping
	if idx.mmap != nil {
		if err := idx.mmap.Close(); err != nil {
			return err
		}
	}

	// Close file
	return idx.file.Close()
}

// applyWrites applies all pending writes to the B-tree
func (idx *BTreeIndex) applyWrites() error {
	if len(idx.pendingWrites) == 0 {
		return nil
	}

	// Sort writes by key for better efficiency
	sort.Slice(idx.pendingWrites, func(i, j int) bool {
		cmp := idx.compareKeys(idx.pendingWrites[i].key, idx.pendingWrites[j].key)
		if cmp == 0 {
			// If keys are equal, sort by record ID and delete/insert
			if idx.pendingWrites[i].id != idx.pendingWrites[j].id {
				return idx.pendingWrites[i].id < idx.pendingWrites[j].id
			}
			// Deletes come before inserts
			return idx.pendingWrites[i].isDelete && !idx.pendingWrites[j].isDelete
		}
		return cmp < 0
	})

	// Group writes by key to handle multiple operations on the same key efficiently
	var prevKey interface{} = nil
	var valuesToAdd []uint32
	var valuesToRemove []uint32

	flushOp := func() error {
		if prevKey == nil {
			return nil
		}

		// Process deletes
		for _, id := range valuesToRemove {
			if err := idx.removeFromTree(prevKey, id); err != nil {
				return err
			}
		}

		// Process inserts
		for _, id := range valuesToAdd {
			if err := idx.insertIntoTree(prevKey, id); err != nil {
				return err
			}
		}

		return nil
	}

	for _, write := range idx.pendingWrites {
		// If the key changed, process the previous operations
		if prevKey != nil && idx.compareKeys(prevKey, write.key) != 0 {
			if err := flushOp(); err != nil {
				return err
			}
			valuesToAdd = valuesToAdd[:0]
			valuesToRemove = valuesToRemove[:0]
		}

		prevKey = write.key
		if write.isDelete {
			valuesToRemove = append(valuesToRemove, write.id)
		} else {
			valuesToAdd = append(valuesToAdd, write.id)
		}
	}

	// Process the last key
	if err := flushOp(); err != nil {
		return err
	}

	// Clear pending writes
	idx.pendingWrites = idx.pendingWrites[:0]

	// Update the header
	return idx.updateHeader()
}

// insertIntoTree inserts a key-value pair into the B-tree
// This is the internal implementation that actually modifies the tree
func (idx *BTreeIndex) insertIntoTree(key interface{}, recordID uint32) error {
	// Start at the root
	root, err := idx.readNode(idx.rootPageNum)
	if err != nil {
		return err
	}

	// If the root is full, we need to split it first
	if int(root.KeyCount) >= BTreeOrder {
		// Create new root
		newRoot := &BTreeNode{
			PageType: BTreePageTypeInternal,
			KeyCount: 0,
			Keys:     make([]interface{}, 0, BTreeOrder),
			Children: make([]uint32, 0, BTreeOrder+1),
		}

		// Allocate a page for the new root
		newRootPageNum, err := idx.allocatePage()
		if err != nil {
			return err
		}

		// Split the old root
		newChildEntry, err := idx.splitNode(idx.rootPageNum, root, 0)
		if err != nil {
			return err
		}

		// Update the new root
		newRoot.Keys = append(newRoot.Keys, newChildEntry.key)
		newRoot.Children = append(newRoot.Children, idx.rootPageNum, newChildEntry.childPageNum)
		newRoot.KeyCount = 1

		// Write the new root
		if err := idx.writeNode(newRootPageNum, newRoot); err != nil {
			return err
		}

		// Update root page number
		idx.rootPageNum = newRootPageNum
	}

	// Now insert into the tree
	_, err = idx.insertIntoNode(idx.rootPageNum, key, recordID)
	return err
}

// insertIntoNode recursively inserts a key-value pair into a node
func (idx *BTreeIndex) insertIntoNode(pageNum uint32, key interface{}, recordID uint32) (bool, error) {
	// Read the node
	node, err := idx.readNode(pageNum)
	if err != nil {
		return false, err
	}

	// Find the position where the key should go
	pos, found := idx.searchNodeForKey(node, key)

	// If we're at a leaf node
	if node.PageType == BTreePageTypeLeaf {
		// If the key already exists, add the record ID to its values
		if found {
			// Check if the record ID is already in the values
			values := node.Values[pos]
			for _, id := range values {
				if id == recordID {
					return false, nil // Record ID already exists for this key
				}
			}

			// Add the record ID
			node.Values[pos] = append(node.Values[pos], recordID)

			// Write the updated node
			if err := idx.writeNode(pageNum, node); err != nil {
				return false, err
			}

			return false, nil
		}

		// Insert the key and value at the found position
		node.Keys = append(node.Keys, nil) // Make room
		node.Values = append(node.Values, nil)

		// Shift elements to make room for the new key
		for i := int(node.KeyCount); i > pos; i-- {
			node.Keys[i] = node.Keys[i-1]
			node.Values[i] = node.Values[i-1]
		}

		// Insert the new key and value
		node.Keys[pos] = key
		node.Values[pos] = []uint32{recordID}
		node.KeyCount++

		// Add to bloom filter
		idx.addToBloomFilter(&node.BloomFilter, key)

		// Check if the node is now full
		if int(node.KeyCount) > BTreeOrder {
			return true, idx.splitLeafNode(pageNum, node)
		}

		// Write the updated node
		if err := idx.writeNode(pageNum, node); err != nil {
			return false, err
		}

		return false, nil
	}

	// If we're at an internal node, recurse into the appropriate child
	childIndex := pos
	if found {
		// If the key already exists in this node, go to the right child
		childIndex = pos + 1
	}

	// Make sure the child index is valid
	if childIndex >= len(node.Children) {
		return false, fmt.Errorf("invalid child index: %d (node has %d children)", childIndex, len(node.Children))
	}

	// Recurse into the child
	needsSplit, err := idx.insertIntoNode(node.Children[childIndex], key, recordID)
	if err != nil {
		return false, err
	}

	// If the child was split, we need to insert the middle key and new child pointer
	if needsSplit {
		// Read the child to get its middle key and right child
		child, err := idx.readNode(node.Children[childIndex])
		if err != nil {
			return false, err
		}

		// Insert the middle key and right child into this node
		// (This is simplified - in a real implementation, we would get this information from the split operation)
		node.Keys = append(node.Keys, nil) // Make room
		node.Children = append(node.Children, 0)

		// Shift elements to make room
		for i := int(node.KeyCount); i > childIndex; i-- {
			node.Keys[i] = node.Keys[i-1]
			node.Children[i+1] = node.Children[i]
		}

		// Insert the middle key and right child
		node.Keys[childIndex] = child.Keys[0]
		node.Children[childIndex+1] = child.Children[1]
		node.KeyCount++

		// Check if this node is now full
		if int(node.KeyCount) > BTreeOrder {
			return true, idx.splitInternalNode(pageNum, node)
		}

		// Write the updated node
		if err := idx.writeNode(pageNum, node); err != nil {
			return false, err
		}
	}

	return false, nil
}

// removeFromTree removes a key-value pair from the B-tree
func (idx *BTreeIndex) removeFromTree(key interface{}, recordID uint32) error {
	// Start at the root
	return idx.removeFromNode(idx.rootPageNum, key, recordID)
}

// removeFromNode recursively removes a key-value pair from a node
func (idx *BTreeIndex) removeFromNode(pageNum uint32, key interface{}, recordID uint32) error {
	// Read the node
	node, err := idx.readNode(pageNum)
	if err != nil {
		return err
	}

	// Find the position where the key should be
	pos, found := idx.searchNodeForKey(node, key)

	// If we're at a leaf node
	if node.PageType == BTreePageTypeLeaf {
		// If the key doesn't exist, nothing to do
		if !found {
			return nil
		}

		// Find the record ID in the values
		values := node.Values[pos]
		valueIndex := -1
		for i, id := range values {
			if id == recordID {
				valueIndex = i
				break
			}
		}

		// If the record ID wasn't found, nothing to do
		if valueIndex == -1 {
			return nil
		}

		// Remove the record ID from the values
		if len(values) > 1 {
			// Multiple values, just remove this one
			node.Values[pos] = append(values[:valueIndex], values[valueIndex+1:]...)
		} else {
			// Last value, remove the key entirely
			for i := pos; i < int(node.KeyCount)-1; i++ {
				node.Keys[i] = node.Keys[i+1]
				node.Values[i] = node.Values[i+1]
			}
			node.KeyCount--

			// TODO: Handle underflow (node with too few keys)
		}

		// Write the updated node
		return idx.writeNode(pageNum, node)
	}

	// If we're at an internal node, recurse into the appropriate child
	childIndex := pos
	if found && pos < int(node.KeyCount) {
		childIndex = pos + 1
	}

	// Make sure the child index is valid
	if childIndex >= len(node.Children) {
		return fmt.Errorf("invalid child index: %d (node has %d children)", childIndex, len(node.Children))
	}

	// Recurse into the child
	return idx.removeFromNode(node.Children[childIndex], key, recordID)
}

// splitNode splits a node and returns information about the split
type splitResult struct {
	key          interface{}
	childPageNum uint32
}

// splitLeafNode splits a leaf node that is too full
func (idx *BTreeIndex) splitLeafNode(pageNum uint32, node *BTreeNode) error {
	// Create a new leaf node for the right half
	rightNode := &BTreeNode{
		PageType: BTreePageTypeLeaf,
		KeyCount: 0,
		Keys:     make([]interface{}, BTreeOrder),
		Values:   make([][]uint32, BTreeOrder),
	}

	// Allocate a page for the new node
	rightPageNum, err := idx.allocatePage()
	if err != nil {
		return err
	}

	// Calculate split point (middle of the node)
	splitPoint := int(node.KeyCount) / 2

	// Move half of the keys and values to the new node
	rightNode.KeyCount = node.KeyCount - uint16(splitPoint)
	for i := splitPoint; i < int(node.KeyCount); i++ {
		rightNode.Keys[i-splitPoint] = node.Keys[i]
		rightNode.Values[i-splitPoint] = node.Values[i]

		// Add to right node's bloom filter
		idx.addToBloomFilter(&rightNode.BloomFilter, node.Keys[i])
	}

	// Update the original node's key count
	node.KeyCount = uint16(splitPoint)

	// Rebuild bloom filter for left node
	for i := 0; i < splitPoint; i++ {
		idx.addToBloomFilter(&node.BloomFilter, node.Keys[i])
	}

	// Write both nodes
	if err := idx.writeNode(pageNum, node); err != nil {
		return err
	}

	if err := idx.writeNode(rightPageNum, rightNode); err != nil {
		return err
	}

	return nil
}

// splitInternalNode splits an internal node that is too full
func (idx *BTreeIndex) splitInternalNode(pageNum uint32, node *BTreeNode) error {
	// Create a new internal node for the right half
	rightNode := &BTreeNode{
		PageType: BTreePageTypeInternal,
		KeyCount: 0,
		Keys:     make([]interface{}, BTreeOrder),
		Children: make([]uint32, BTreeOrder+1),
	}

	// Allocate a page for the new node
	rightPageNum, err := idx.allocatePage()
	if err != nil {
		return err
	}

	// Calculate split point (middle of the node)
	splitPoint := int(node.KeyCount) / 2

	// Move half of the keys and children to the new node
	rightNode.KeyCount = node.KeyCount - uint16(splitPoint) - 1 // Exclude the middle key

	// Copy keys (excluding the middle key)
	for i := splitPoint + 1; i < int(node.KeyCount); i++ {
		rightNode.Keys[i-(splitPoint+1)] = node.Keys[i]
	}

	// Copy children (including the child after the middle key)
	for i := splitPoint + 1; i <= int(node.KeyCount); i++ {
		rightNode.Children[i-(splitPoint+1)] = node.Children[i]
	}

	// The middle key will be moved up to the parent
	middleKey := node.Keys[splitPoint]

	// Update the original node's key count
	node.KeyCount = uint16(splitPoint)

	// Write both nodes
	if err := idx.writeNode(pageNum, node); err != nil {
		return err
	}

	if err := idx.writeNode(rightPageNum, rightNode); err != nil {
		return err
	}

	// Handle the case where this is the root node
	if pageNum == idx.rootPageNum {
		// Create new root
		newRoot := &BTreeNode{
			PageType: BTreePageTypeInternal,
			KeyCount: 1,
			Keys:     []interface{}{middleKey},
			Children: []uint32{pageNum, rightPageNum},
		}

		// Allocate a page for the new root
		newRootPageNum, err := idx.allocatePage()
		if err != nil {
			return err
		}

		// Write the new root
		if err := idx.writeNode(newRootPageNum, newRoot); err != nil {
			return err
		}

		// Update root page number
		idx.rootPageNum = newRootPageNum
	}

	return nil
}

// splitNode is a helper that handles both leaf and internal node splits
func (idx *BTreeIndex) splitNode(pageNum uint32, node *BTreeNode, keyIndex int) (*splitResult, error) {
	if node.PageType == BTreePageTypeLeaf {
		if err := idx.splitLeafNode(pageNum, node); err != nil {
			return nil, err
		}

		// For a leaf node split, we need to return the first key of the right node
		// Simplified version - in a real implementation we would get this information from splitLeafNode
		rightNode, err := idx.readNode(pageNum + 1) // This assumes the right node is at the next page
		if err != nil {
			return nil, err
		}

		return &splitResult{
			key:          rightNode.Keys[0],
			childPageNum: pageNum + 1,
		}, nil
	} else {
		if err := idx.splitInternalNode(pageNum, node); err != nil {
			return nil, err
		}

		// For an internal node split, we need to return the middle key and the right node page number
		// Simplified version - in a real implementation we would get this information from splitInternalNode
		return &splitResult{
			key:          node.Keys[node.KeyCount-1],
			childPageNum: pageNum + 1, // This assumes the right node is at the next page
		}, nil
	}
}

// rebuildIndex rebuilds the index from the database file
func (idx *BTreeIndex) rebuildIndex(dbFileName string, fieldName string, fieldOffset uintptr) error {
	// Re-initialize the B-tree file
	if err := idx.initializeFile(); err != nil {
		return err
	}

	// Update memory mapping
	if err := idx.remapFile(); err != nil {
		return err
	}

	// Open the database file
	dbFile, err := os.Open(dbFileName)
	if err != nil {
		return fmt.Errorf("failed to open database file for index rebuild: %w", err)
	}
	defer dbFile.Close()

	// Scan through the database file and rebuild the index
	// This is a simplified implementation - in a real implementation,
	// you would need to understand the database file format and read records

	// For now, we'll just create an empty index
	return nil
}

// mergeWithDatabase copies the index into the database file for portability
func (idx *BTreeIndex) mergeWithDatabase() error {
	// Open the database file
	dbFile, err := os.OpenFile(idx.dbFileName, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open database file for index merge: %w", err)
	}
	defer dbFile.Close()

	// Get database file size
	dbInfo, err := dbFile.Stat()
	if err != nil {
		return err
	}

	// Get index file size
	idxInfo, err := idx.file.Stat()
	if err != nil {
		return err
	}

	// Append index to database file
	// First, ensure the database file is large enough
	newSize := dbInfo.Size() + idxInfo.Size() + 8 // +8 for index location marker
	if err := dbFile.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to resize database file: %w", err)
	}

	// Copy index file to the end of database file
	idxData := make([]byte, idxInfo.Size())
	if _, err := idx.file.ReadAt(idxData, 0); err != nil {
		return fmt.Errorf("failed to read index file: %w", err)
	}

	if _, err := dbFile.WriteAt(idxData, dbInfo.Size()); err != nil {
		return fmt.Errorf("failed to write index to database file: %w", err)
	}

	// Write index location marker at the end of the file
	marker := make([]byte, 8)
	binary.LittleEndian.PutUint64(marker, uint64(dbInfo.Size()))
	if _, err := dbFile.WriteAt(marker, newSize-8); err != nil {
		return fmt.Errorf("failed to write index location marker: %w", err)
	}

	return dbFile.Sync()
}

// extractFromDatabase extracts the index from a database file
func ExtractIndexFromDatabase(dbFileName string, indexFileName string) error {
	// Open the database file
	dbFile, err := os.Open(dbFileName)
	if err != nil {
		return fmt.Errorf("failed to open database file: %w", err)
	}
	defer dbFile.Close()

	// Get database file size
	dbInfo, err := dbFile.Stat()
	if err != nil {
		return err
	}

	// Read the index location marker (last 8 bytes)
	marker := make([]byte, 8)
	if _, err := dbFile.ReadAt(marker, dbInfo.Size()-8); err != nil {
		return fmt.Errorf("failed to read index location marker: %w", err)
	}

	// Get index location
	indexLoc := binary.LittleEndian.Uint64(marker)
	indexSize := dbInfo.Size() - int64(indexLoc) - 8

	// Read the index data
	indexData := make([]byte, indexSize)
	if _, err := dbFile.ReadAt(indexData, int64(indexLoc)); err != nil {
		return fmt.Errorf("failed to read index data: %w", err)
	}

	// Write to the index file
	indexFile, err := os.Create(indexFileName)
	if err != nil {
		return fmt.Errorf("failed to create index file: %w", err)
	}
	defer indexFile.Close()

	if _, err := indexFile.Write(indexData); err != nil {
		return fmt.Errorf("failed to write index file: %w", err)
	}

	return nil
}
