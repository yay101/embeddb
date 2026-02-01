package embeddb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"sync/atomic"
)

type index struct {
	next uint32
}

func (db *Database[T]) IDFromOffset(o uint32) (r uint32) {
	for p := range db.index {
		r = p - 1
		if db.index[p] > o && db.index[r] < o {
			return r
		}
	}
	return 0
}

// this function isnt needed and should be replaced with db.index[id]. Its just here to get my thoughts out.
func (db *Database[T]) OffsetFromID(id uint32) uint32 {
	return db.index[id]
}

func (db *Database[T]) WriteIndex() (err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Prepare a buffer for the index data
	buf := bytes.NewBuffer(nil)

	// Write the number of entries in the index
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, uint32(len(db.index)))
	buf.Write(idBytes)

	// Write each (id, offset) pair as two uint32 values
	for id, offset := range db.index {
		binary.BigEndian.PutUint32(idBytes, id)
		buf.Write(idBytes)
		binary.BigEndian.PutUint32(idBytes, offset)
		buf.Write(idBytes)
	}

	// Calculate required space for the index
	indexSize := buf.Len()

	// Check if we need to allocate more space for the index
	if db.header.indexCapacity < uint32(indexSize) {
		// Double the capacity to avoid frequent reallocations
		newCapacity := uint32(indexSize) * 2
		if newCapacity < defaultIndexPreallocation {
			newCapacity = defaultIndexPreallocation
		}

		// Update the header with the new capacity
		atomic.StoreUint32(&db.header.indexCapacity, newCapacity)
	}

	// Prepare the full index data with markers
	headerBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(headerBytes, uint32(indexSize))
	indexData := slices.Concat([]byte{startMarker}, headerBytes, buf.Bytes(), []byte{endMarker})

	// Decide where to write the index
	var writePosition int64
	if db.header.indexStart == 0 {
		// First time writing the index - append to the file
		fileInfo, err := db.file.Stat()
		if err != nil {
			return fmt.Errorf("failed to get file stats: %w", err)
		}
		writePosition = fileInfo.Size()
	} else if uint32(len(indexData)) <= db.header.indexCapacity {
		// Index fits in the preallocated space - use the existing location
		writePosition = int64(db.header.indexStart)
	} else {
		// Need to move the index to the end of the file
		fileInfo, err := db.file.Stat()
		if err != nil {
			return fmt.Errorf("failed to get file stats: %w", err)
		}
		writePosition = fileInfo.Size()
	}

	// Write the index data
	if _, err := db.file.WriteAt(indexData, writePosition); err != nil {
		return fmt.Errorf("failed to write index: %w", err)
	}

	// Save the previous index location before updating
	db.header.lgIndexStart = db.header.indexStart

	// Update the header with the new index location
	atomic.StoreUint32(&db.header.indexStart, uint32(writePosition))
	atomic.StoreUint32(&db.header.indexEnd, uint32(writePosition+int64(len(indexData))))

	// Update the header on disk
	return db.encodeHeader()
}

func (db *Database[T]) ReadIndex() (err error) {
	db.mlock.Lock()
	defer db.mlock.Unlock()

	// If no index has been written yet
	if db.header.indexStart == 0 {
		return nil
	}

	// Ensure we have a memory-mapped file
	if db.mfile == nil {
		if err := db.ReloadMMap(); err != nil {
			return fmt.Errorf("failed to reload memory-mapped file: %w", err)
		}
	}

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
	// Create a temporary file for the vacuum operation
	tempFile, err := os.CreateTemp("", "embeddb-vacuum-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file for vacuum: %w", err)
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	// Begin a transaction to ensure atomicity
	if err := db.beginTransaction(); err != nil {
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

	// Process each active record
	for id, oldOffset := range db.index {
		// Read the record
		recordBytes, err := db.readRecordBytes(oldOffset)
		if err != nil {
			db.rollbackTransaction()
			return fmt.Errorf("failed to read record %d during vacuum: %w", id, err)
		}

		// Skip inactive records
		if !isActiveRecord(recordBytes) {
			continue
		}

		// Decode the record for index maintenance
		record, err := db.decodeRecord(recordBytes)
		if err != nil {
			db.rollbackTransaction()
			return fmt.Errorf("failed to decode record %d during vacuum: %w", id, err)
		}

		// Store the record for index rebuilding
		keptRecords[id] = record

		// Write the record to the temporary file
		if _, err := tempFile.WriteAt(recordBytes, int64(writePos)); err != nil {
			db.rollbackTransaction()
			return fmt.Errorf("failed to write record %d to temporary file: %w", id, err)
		}

		// Update the index
		newIndex[id] = writePos
		writePos += uint32(len(recordBytes))
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
		db.rollbackTransaction()
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
		db.rollbackTransaction()
		return fmt.Errorf("failed to write header to temporary file: %w", err)
	}

	// Close the database file and memory mapping
	db.mlock.Lock()
	if db.mfile != nil {
		db.mfile.Close()
		db.mfile = nil
	}
	db.mlock.Unlock()

	db.lock.Lock()
	db.file.Close()

	// Save existing indexes
	var indexManager *IndexManager[T]
	if db.indexManager != nil {
		indexManager = db.indexManager
		// Close all indexes to make sure they're saved
		for _, index := range indexManager.indexes {
			if err := index.Flush(); err != nil {
				db.rollbackTransaction()
				return fmt.Errorf("failed to flush indexes: %w", err)
			}
		}
	}

	// Rename the temporary file to the original
	if err := os.Rename(tempFile.Name(), db.file.Name()); err != nil {
		db.rollbackTransaction()
		return fmt.Errorf("failed to replace original file: %w", err)
	}

	// Reopen the file
	file, err := os.OpenFile(db.file.Name(), os.O_RDWR, 0644)
	if err != nil {
		db.rollbackTransaction()
		return fmt.Errorf("failed to reopen database file: %w", err)
	}

	// Update the database
	db.file = file
	db.header = newHeader
	db.index = newIndex
	db.lock.Unlock()

	// Reload memory mapping
	if err := db.ReloadMMap(); err != nil {
		db.rollbackTransaction()
		return fmt.Errorf("failed to reload memory mapping: %w", err)
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
	return db.commitTransaction()
}
