package embeddb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
)

type index struct {
	next uint32
}

func (db *Database[T]) IDFromOffset(o uint32) (r uint32) {
	// For backward compatibility, use table 0
	idx := db.indexes[0]
	for p := range idx {
		r = p - 1
		if idx[p] > o && idx[r] < o {
			return r
		}
	}
	return 0
}

// this function isnt needed and should be replaced with db.index[id]. Its just here to get my thoughts out.
func (db *Database[T]) OffsetFromID(id uint32) uint32 {
	// For backward compatibility, use table 0
	offset, _ := db.getRecordOffset(0, id)
	return offset
}

// WriteIndex writes the index to the database file.
// This is the public API that acquires locks.
func (db *Database[T]) WriteIndex() (err error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.writeIndexLocked()
}

// writeIndexLocked writes the index to the database file.
// IMPORTANT: Caller must hold db.lock before calling this method.
func (db *Database[T]) writeIndexLocked() (err error) {
	// Calculate total size needed for all table indexes
	totalContentSize := 0
	idBytes := make([]byte, 4)

	for _, idx := range db.indexes {
		// Per table: tableID(1) + count(4) + entries(8 each)
		tableSize := 1 + 4 + (len(idx) * 8)
		totalContentSize += tableSize
	}

	// Calculate required space for the index (content + markers)
	totalIndexSize := 1 + totalContentSize + 1

	// Determine write position
	var writePosition uint32

	if db.header.indexStart > 0 && db.header.indexCapacity >= uint32(totalIndexSize) {
		writePosition = db.header.indexStart
	} else {
		writePosition = db.header.nextOffset
		newCapacity := uint32(totalIndexSize) * 2
		if newCapacity < defaultIndexPreallocation {
			newCapacity = defaultIndexPreallocation
		}
		atomic.StoreUint32(&db.header.indexCapacity, newCapacity)
		atomic.StoreUint32(&db.header.nextOffset, writePosition+newCapacity)
	}

	// Build index data
	indexData := make([]byte, 0, totalIndexSize)
	indexData = append(indexData, startMarker)

	// Write each table's index block
	for tableID := uint8(0); tableID < 255; tableID++ {
		idx, exists := db.indexes[tableID]
		if !exists || len(idx) == 0 {
			continue
		}

		indexData = append(indexData, tableID)
		binary.BigEndian.PutUint32(idBytes, uint32(len(idx)))
		indexData = append(indexData, idBytes...)

		for id, offset := range idx {
			binary.BigEndian.PutUint32(idBytes, id)
			indexData = append(indexData, idBytes...)
			binary.BigEndian.PutUint32(idBytes, offset)
			indexData = append(indexData, idBytes...)
		}
	}

	indexData = append(indexData, endMarker)

	// Write the index data
	if _, err := db.file.WriteAt(indexData, int64(writePosition)); err != nil {
		return fmt.Errorf("failed to write index: %w", err)
	}

	if db.header.indexStart != writePosition {
		db.header.lgIndexStart = db.header.indexStart
	}

	atomic.StoreUint32(&db.header.indexStart, writePosition)
	atomic.StoreUint32(&db.header.indexEnd, writePosition+uint32(len(indexData)))

	return db.encodeHeaderLocked()
}

func (db *Database[T]) ReadIndex() (err error) {
	// If no index has been written yet
	if db.header.indexStart == 0 {
		return nil
	}

	// Ensure we have a memory-mapped file (ReloadMMap acquires its own lock)
	db.mlock.RLock()
	needsReload := db.mfile == nil
	db.mlock.RUnlock()

	if needsReload {
		if err := db.ReloadMMap(); err != nil {
			return fmt.Errorf("failed to reload memory-mapped file: %w", err)
		}
	}

	db.mlock.Lock()
	defer db.mlock.Unlock()

	// Check for start marker
	if db.mfile.At(int(db.header.indexStart)) != startMarker {
		// Try the last known good index location
		if db.header.lgIndexStart > 0 && db.header.lgIndexStart != db.header.indexStart {
			log.Printf("Current index location is invalid, trying last good index at offset %d", db.header.lgIndexStart)
			atomic.StoreUint32(&db.header.indexStart, db.header.lgIndexStart)
			return db.ReadIndex()
		}
		return errors.New("index location doesn't start correctly")
	}

	// Read the index length
	lengthBytes := make([]byte, 4)
	_, err = db.mfile.ReadAt(lengthBytes, int64(db.header.indexStart+1))
	if err != nil {
		return fmt.Errorf("failed to read index length: %w", err)
	}
	length := binary.BigEndian.Uint32(lengthBytes)

	// Check for end marker
	endMarkerPos := db.header.indexStart + 5 + length
	if int(endMarkerPos) >= db.mfile.Len() || db.mfile.At(int(endMarkerPos)) != endMarker {
		// Try the last known good index location
		if db.header.lgIndexStart > 0 && db.header.lgIndexStart != db.header.indexStart {
			log.Printf("Current index appears corrupted, trying last good index at offset %d", db.header.lgIndexStart)
			atomic.StoreUint32(&db.header.indexStart, db.header.lgIndexStart)
			return db.ReadIndex()
		}
		return errors.New("index location doesn't end correctly")
	}

	// Read the index data
	indexBytes := make([]byte, int(length))
	_, err = db.mfile.ReadAt(indexBytes, int64(db.header.indexStart+5))
	if err != nil {
		return fmt.Errorf("failed to read index data: %w", err)
	}

	// Clear the existing index
	db.index = make(map[uint32]uint32)

	// Parse the index data
	buf := bytes.NewReader(indexBytes)

	// Read the number of entries
	countBytes := make([]byte, 4)
	if _, err := buf.Read(countBytes); err != nil {
		return fmt.Errorf("failed to read index entry count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBytes)

	// Read each (id, offset) pair
	idBytes := make([]byte, 4)
	offsetBytes := make([]byte, 4)

	for i := uint32(0); i < count; i++ {
		// Read the record ID
		if _, err := buf.Read(idBytes); err != nil {
			return fmt.Errorf("failed to read record ID: %w", err)
		}
		id := binary.BigEndian.Uint32(idBytes)

		// Read the record offset
		if _, err := buf.Read(offsetBytes); err != nil {
			return fmt.Errorf("failed to read record offset: %w", err)
		}
		offset := binary.BigEndian.Uint32(offsetBytes)

		// Add to the index
		db.index[id] = offset
	}

	return nil
}

func (db *Database[T]) Vacuum() error {
	// Create a temporary file in the same directory as the database
	// This ensures rename() works (can't rename across filesystems)
	dbDir := filepath.Dir(db.file.Name())
	tempFile, err := os.CreateTemp(dbDir, "embeddb-vacuum-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file for vacuum: %w", err)
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	// Acquire lock for the entire vacuum operation
	db.lock.Lock()

	// Begin a transaction to ensure atomicity (using locked version since we hold the lock)
	if err := db.beginTransactionLocked(); err != nil {
		db.lock.Unlock()
		return fmt.Errorf("failed to begin vacuum transaction: %w", err)
	}

	// Create a new, compact index
	newIndex := make(map[uint32]uint32)

	// Write header placeholder to the temporary file
	headerPlaceholder := make([]byte, headerSize)
	if _, err := tempFile.Write(headerPlaceholder); err != nil {
		db.rollbackTransaction()
		return fmt.Errorf("failed to write header placeholder: %w", err)
	}

	// Current write position starts after header
	writePos := uint32(headerSize)

	// Collect records that will remain after vacuum
	keptRecords := make(map[uint32]*T)

	// Copy the index keys to avoid holding lock while iterating
	indexCopy := make(map[uint32]uint32)
	for k, v := range db.index {
		indexCopy[k] = v
	}

	// Release lock temporarily to read records (readRecordBytes needs mlock)
	db.lock.Unlock()

	// Process each active record (using copy to avoid race)
	var vacuumErr error
	for id, oldOffset := range indexCopy {
		// Read the record
		recordBytes, err := db.readRecordBytes(oldOffset)
		if err != nil {
			vacuumErr = fmt.Errorf("failed to read record %d during vacuum: %w", id, err)
			break
		}

		// Skip inactive records
		if !isActiveRecord(recordBytes) {
			continue
		}

		// Decode the record for index maintenance
		record, err := db.decodeRecord(recordBytes)
		if err != nil {
			vacuumErr = fmt.Errorf("failed to decode record %d during vacuum: %w", id, err)
			break
		}

		// Store the record for index rebuilding
		keptRecords[id] = record

		// Write the record to the temporary file
		if _, err := tempFile.WriteAt(recordBytes, int64(writePos)); err != nil {
			vacuumErr = fmt.Errorf("failed to write record %d to temporary file: %w", id, err)
			break
		}

		// Update the index
		newIndex[id] = writePos
		writePos += uint32(len(recordBytes))
	}

	// Re-acquire lock for the rest of the operation
	db.lock.Lock()

	// Check if there was an error during record processing
	if vacuumErr != nil {
		db.rollbackTransactionLocked()
		db.lock.Unlock()
		return vacuumErr
	}

	// Copy the original header
	newHeader := DBHeader{
		Version:      Version,
		indexStart:   0, // Will be set when writing the index
		indexEnd:     0, // Will be set when writing the index
		nextRecordID: db.header.nextRecordID,
		nextOffset:   writePos,
	}

	// Write the index to the temporary file
	indexBuf := bytes.NewBuffer(nil)

	// Write the number of entries
	countBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(countBytes, uint32(len(newIndex)))
	indexBuf.Write(countBytes)

	// Write each (id, offset) pair
	idBytes := make([]byte, 4)
	for id, offset := range newIndex {
		binary.BigEndian.PutUint32(idBytes, id)
		indexBuf.Write(idBytes)
		binary.BigEndian.PutUint32(idBytes, offset)
		indexBuf.Write(idBytes)
	}

	// Calculate index size
	indexSize := indexBuf.Len()

	// Prepare index with markers
	headerBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(headerBytes, uint32(indexSize))
	indexData := slices.Concat([]byte{startMarker}, headerBytes, indexBuf.Bytes(), []byte{endMarker})

	// Write the index to the temporary file
	if _, err := tempFile.WriteAt(indexData, int64(writePos)); err != nil {
		db.rollbackTransactionLocked()
		db.lock.Unlock()
		return fmt.Errorf("failed to write index to temporary file: %w", err)
	}

	// Update header with index information
	newHeader.indexStart = writePos
	newHeader.indexEnd = writePos + uint32(len(indexData))
	newHeader.indexCapacity = uint32(len(indexData)) * 2 // Double for future growth

	// Write the header to the temporary file
	headerBytes = make([]byte, headerSize)
	// TODO: Write the header data to headerBytes
	if _, err := tempFile.WriteAt(headerBytes, 0); err != nil {
		db.rollbackTransactionLocked()
		db.lock.Unlock()
		return fmt.Errorf("failed to write header to temporary file: %w", err)
	}

	// Close the database file and memory mapping
	db.mlock.Lock()
	if db.mfile != nil {
		db.mfile.Close()
		db.mfile = nil
	}
	db.mlock.Unlock()

	// Save the original filename before closing
	originalFileName := db.file.Name()
	db.file.Close()

	// Save existing indexes
	var indexManager *IndexManager[T]
	if db.indexManager != nil {
		indexManager = db.indexManager
		// Close all indexes to make sure they're saved
		for _, index := range indexManager.indexes {
			if err := index.Flush(); err != nil {
				// Reopen the original file before rolling back
				db.file, _ = os.OpenFile(originalFileName, os.O_RDWR, 0644)
				db.rollbackTransactionLocked()
				db.lock.Unlock()
				return fmt.Errorf("failed to flush indexes: %w", err)
			}
		}
	}

	// Rename the temporary file to the original
	if err := os.Rename(tempFile.Name(), originalFileName); err != nil {
		// Reopen the original file before rolling back
		db.file, _ = os.OpenFile(originalFileName, os.O_RDWR, 0644)
		db.rollbackTransactionLocked()
		db.lock.Unlock()
		return fmt.Errorf("failed to replace original file: %w", err)
	}

	// Reopen the file
	file, err := os.OpenFile(originalFileName, os.O_RDWR, 0644)
	if err != nil {
		db.rollbackTransactionLocked()
		db.lock.Unlock()
		return fmt.Errorf("failed to reopen database file: %w", err)
	}

	// Update the database
	db.file = file
	// Update header fields individually to avoid copying the mutex
	db.header.Version = newHeader.Version
	db.header.indexStart = newHeader.indexStart
	db.header.indexEnd = newHeader.indexEnd
	db.header.nextRecordID = newHeader.nextRecordID
	db.header.nextOffset = newHeader.nextOffset
	db.header.tocStart = newHeader.tocStart
	db.header.entryStart = newHeader.entryStart
	db.header.lgIndexStart = newHeader.lgIndexStart
	db.header.indexCapacity = newHeader.indexCapacity
	db.index = newIndex

	// Release lock before reloading mmap (ReloadMMap uses mlock)
	db.lock.Unlock()

	// Reload memory mapping
	// Note: At this point, the file has been replaced - we can't rollback anymore
	if err := db.ReloadMMap(); err != nil {
		// Can't rollback since file is already replaced, just return error
		// The transaction will be cleaned up when Close() is called
		if db.transaction != nil {
			db.transaction.state = TransactionRolledBack
			if db.transaction.tempFile != nil {
				db.transaction.tempFile.Close()
			}
			db.transaction = nil
		}
		return fmt.Errorf("failed to reload memory mapping after vacuum: %w", err)
	}

	// Rebuild indexes if needed
	if indexManager != nil {
		db.indexManager = indexManager

		// Force rebuild all indexes with the new record positions
		indexFields := indexManager.GetIndexedFields()
		for _, fieldName := range indexFields {
			// Drop and recreate the index
			if err := db.DropIndex(fieldName); err != nil {
				fmt.Printf("Warning: failed to drop index %s: %v\n", fieldName, err)
				continue
			}
			if err := db.CreateIndex(fieldName); err != nil {
				fmt.Printf("Warning: failed to recreate index %s: %v\n", fieldName, err)
				continue
			}
		}

		// Build the indexes with the kept records
		for id, record := range keptRecords {
			if err := db.indexManager.InsertIntoIndexes(record, id); err != nil {
				fmt.Printf("Warning: failed to reindex record %d: %v\n", id, err)
			}
		}
	}

	// Commit the transaction
	// At this point we've successfully completed the vacuum, so commit should succeed
	if err := db.commitTransaction(); err != nil {
		// Log but don't fail - the vacuum itself succeeded
		fmt.Printf("Warning: failed to commit vacuum transaction: %v\n", err)
	}

	return nil
}
