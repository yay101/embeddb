package embeddb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"

	"github.com/yay101/embeddbmmap"
)

func pageAlign(n int64) int64 {
	ps := int64(embeddbmmap.PageSize())
	return ((n + ps - 1) / ps) * ps
}

// allocator manages free space in the database file.
type allocator struct {
	file        *os.File
	region      atomic.Pointer[embeddbmmap.MappedRegion]
	nextOffset  uint64
	freeList    []freeBlock
	actualSize  uint64
	truncatedTo uint64
	mu          sync.Mutex
}

// freeBlock represents a free region in the file.
type freeBlock struct {
	offset uint64
	length uint64
}

// newAllocator creates a new allocator for the given file.
func newAllocator(file *os.File) *allocator {
	info, _ := file.Stat()
	sz := uint64(0)
	if info != nil {
		sz = uint64(info.Size())
	}
	return &allocator{
		file:        file,
		nextOffset:  4096,
		freeList:    nil,
		actualSize:  sz,
		truncatedTo: sz,
	}
}

// SetFile updates the allocator to use a new file handle (after file reopen).
func (a *allocator) SetFile(file *os.File) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.file = file
	info, _ := file.Stat()
	if info != nil {
		a.actualSize = uint64(info.Size())
		a.truncatedTo = a.actualSize
	}
}

// Allocate returns a free region of at least size bytes.
// It prefers to reuse a free block; if none is large enough, it extends the file.
func (a *allocator) Allocate(size uint64) (offset uint64, length uint64, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i, fb := range a.freeList {
		if fb.length >= size {
			if fb.length == size {
				a.freeList = append(a.freeList[:i], a.freeList[i+1:]...)
			} else {
				a.freeList[i].offset += size
				a.freeList[i].length -= size
			}
			return fb.offset, size, nil
		}
	}
	offset = a.actualSize
	a.actualSize += size
	a.nextOffset = a.actualSize
	if r := a.region.Load(); r != nil {
		r.RLock()
		regionSize := r.Size()
		r.RUnlock()
		needed := pageAlign(int64(a.actualSize))
		if uint64(needed) > uint64(regionSize) {
			growBy := uint64(needed) - uint64(regionSize) + 4*1024*1024
			newSize := uint64(regionSize) + growBy
			a.mu.Unlock()
			if err := a.file.Truncate(int64(newSize)); err != nil {
				a.mu.Lock()
				a.actualSize = offset
				a.nextOffset = offset
				return 0, 0, fmt.Errorf("allocate: truncate failed: %w", err)
			}
			a.mu.Lock()
			a.truncatedTo = newSize
			relocated, err := r.Resize(int64(newSize))
			if err != nil {
				a.actualSize = offset
				a.nextOffset = offset
				return 0, 0, fmt.Errorf("allocate: mmap resize failed: %w", err)
			}
			if relocated {
				a.region.Store(r)
			}
		} else {
			minFile := uint64(needed)
			if uint64(regionSize) > minFile {
				minFile = uint64(regionSize)
			}
			if minFile > a.truncatedTo {
				if err := a.file.Truncate(int64(minFile)); err != nil {
					a.actualSize = offset
					a.nextOffset = offset
					return 0, 0, fmt.Errorf("allocate: truncate to %d failed: %w", minFile, err)
				}
				a.truncatedTo = minFile
			}
		}
	}
	return offset, size, nil
}

// Free releases the region [offset, offset+length) back to the free list.
// It coalesces with adjacent free blocks.
func (a *allocator) Free(offset uint64, length uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Insert the new block in the correct position to keep the list sorted by offset.
	newBlock := freeBlock{offset: offset, length: length}
	var inserted bool
	for i, fb := range a.freeList {
		if fb.offset > offset {
			// Insert before fb.
			a.freeList = append(a.freeList[:i], append([]freeBlock{newBlock}, a.freeList[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		a.freeList = append(a.freeList, newBlock)
	}
	// Now coalesce adjacent blocks.
	// We'll make a single pass merging consecutive blocks.
	var merged []freeBlock
	for _, fb := range a.freeList {
		if len(merged) == 0 {
			merged = append(merged, fb)
			continue
		}
		last := &merged[len(merged)-1]
		if last.offset+last.length == fb.offset {
			// Merge with previous.
			last.length += fb.length
		} else {
			merged = append(merged, fb)
		}
	}
	a.freeList = merged
}

// ReclaimIfNeeded returns the number of bytes that can be reclaimed by running a full coalesce.
// It is used to decide when to run a more aggressive cleanup (e.g., when >1MB has been freed).
// Note: This function does not modify the allocator; it only reports the potential reclaim.
// For simplicity, we just return the total free length.
func (a *allocator) ReclaimIfNeeded() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var total uint64
	for _, fb := range a.freeList {
		total += fb.length
	}
	return total
}

// Reset sets the allocator to a known state (used when loading from header).
func (a *allocator) Reset(nextOffset uint64, freeList []freeBlock) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nextOffset = nextOffset
	a.freeList = freeList
	if a.actualSize < nextOffset {
		a.actualSize = nextOffset
	}
}

// Save writes the free list to the file at the given offset.
func (a *allocator) Save(file *os.File, offset int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Write block count (4 bytes)
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(a.freeList)))

	// Write free blocks: [8 byte offset LE][8 byte length LE] each
	for _, fb := range a.freeList {
		var block [16]byte
		binary.LittleEndian.PutUint64(block[:8], fb.offset)
		binary.LittleEndian.PutUint64(block[8:], fb.length)
		buf = append(buf, block[:]...)
	}

	// Calculate CRC32
	crc := crc32.ChecksumIEEE(buf)

	// Write CRC at end (4 bytes)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	buf = append(buf, crcBuf[:]...)

	_, err := file.WriteAt(buf, offset)
	return err
}

// Load loads the free list from the file at the given offset.
func (a *allocator) Load(file *os.File, offset int64) error {
	// Read block count
	var buf4 [4]byte
	_, err := file.ReadAt(buf4[:], offset)
	if err != nil {
		return err
	}
	blockCount := int(binary.LittleEndian.Uint32(buf4[:]))

	// Read all blocks
	totalSize := 4 + blockCount*16 + 4 // count + blocks + crc
	buf := make([]byte, totalSize)
	_, err = file.ReadAt(buf, offset)
	if err != nil {
		return err
	}

	// Verify CRC
	storedCRC := binary.LittleEndian.Uint32(buf[totalSize-4:])
	computedCRC := crc32.ChecksumIEEE(buf[:totalSize-4])
	if storedCRC != computedCRC {
		return fmt.Errorf("free list CRC mismatch")
	}

	// Parse blocks
	pos := 4
	a.freeList = make([]freeBlock, 0, blockCount)
	for i := 0; i < blockCount; i++ {
		a.freeList = append(a.freeList, freeBlock{
			offset: binary.LittleEndian.Uint64(buf[pos : pos+8]),
			length: binary.LittleEndian.Uint64(buf[pos+8 : pos+16]),
		})
		pos += 16
	}

	return nil
}

// CopyFreeList returns a copy of the current free list (for serialization).
func (a *allocator) CopyFreeList() []freeBlock {
	a.mu.Lock()
	defer a.mu.Unlock()
	freelistCopy := make([]freeBlock, len(a.freeList))
	copy(freelistCopy, a.freeList)
	return freelistCopy
}
