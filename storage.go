package embeddb

import (
	"os"
	"sync"

	"github.com/yay101/embeddbmmap"
)

func pageAlign(n int64) int64 {
	ps := int64(embeddbmmap.PageSize())
	return ((n + ps - 1) / ps) * ps
}

// allocator manages free space in the database file.
type allocator struct {
	file       *os.File
	region     *embeddbmmap.MappedRegion
	nextOffset uint64      // next free offset assuming no reuse
	freeList   []freeBlock // sorted by offset, coalesced
	actualSize uint64      // actual file size on disk
	mu         sync.Mutex  // protects freeList, nextOffset, and actualSize
}

// freeBlock represents a free region in the file.
type freeBlock struct {
	offset uint64
	length uint64
}

// newAllocator creates a new allocator for the given file.
func newAllocator(file *os.File) *allocator {
	info, _ := file.Stat()
	return &allocator{
		file:       file,
		nextOffset: 4096,
		freeList:   nil,
		actualSize: uint64(info.Size()),
	}
}

// SetFile updates the allocator to use a new file handle (after file reopen).
func (a *allocator) SetFile(file *os.File) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.file = file
	info, _ := file.Stat()
	a.actualSize = uint64(info.Size())
}

// Allocate returns a free region of at least size bytes.
// It prefers to reuse a free block; if none is large enough, it extends the file.
func (a *allocator) Allocate(size uint64) (offset uint64, length uint64) {
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
			return fb.offset, size
		}
	}
	offset = a.actualSize
	a.actualSize += size
	a.nextOffset = a.actualSize
	if a.region != nil {
		alignedSize := pageAlign(int64(a.actualSize))
		if alignedSize > a.region.Size() {
			a.file.Truncate(alignedSize)
			a.region.Resize(alignedSize)
		}
	}
	return offset, size
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
	// Ensure actualSize is at least nextOffset
	if a.actualSize < nextOffset {
		a.actualSize = nextOffset
	}
}

// CopyFreeList returns a copy of the current free list (for serialization).
func (a *allocator) CopyFreeList() []freeBlock {
	a.mu.Lock()
	defer a.mu.Unlock()
	freelistCopy := make([]freeBlock, len(a.freeList))
	copy(freelistCopy, a.freeList)
	return freelistCopy
}
