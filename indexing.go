package embeddb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"unsafe"
)

// IndexManager handles the creation, management, and querying of indexes for a database
type IndexManager[T any] struct {
	db             *Database[T]           // Reference to the parent database
	indexes        map[string]*BTreeIndex // Map of field name to index
	layout         *StructLayout          // Struct layout for fast field access
	lock           sync.RWMutex           // Lock for concurrent access
	pendingIndexes map[string]struct{}    // Set of indexes that need to be rebuilt
}

// NewIndexManager creates a new index manager for a database
func NewIndexManager[T any](db *Database[T], layout *StructLayout) *IndexManager[T] {
	return &IndexManager[T]{
		db:             db,
		indexes:        make(map[string]*BTreeIndex),
		layout:         layout,
		pendingIndexes: make(map[string]struct{}),
	}
}

// CreateIndex creates a new index for a specific field
func (im *IndexManager[T]) CreateIndex(fieldName string) error {
	im.lock.Lock()
	defer im.lock.Unlock()

	// Check if the index already exists
	if _, exists := im.indexes[fieldName]; exists {
		return nil // Index already exists
	}

	// Find the field in the layout
	var fieldOffset FieldOffset
	var fieldFound bool

	for _, offset := range im.layout.FieldOffsets {
		if strings.EqualFold(offset.Name, fieldName) {
			fieldOffset = offset
			fieldFound = true
			break
		}
	}

	if !fieldFound {
		return fmt.Errorf("field '%s' not found in struct", fieldName)
	}

	// Create the B-tree index
	index, err := NewBTreeIndex(im.db.file.Name(), fieldName, fieldOffset.Offset, fieldOffset.Type)
	if err != nil {
		return fmt.Errorf("failed to create index for field '%s': %w", fieldName, err)
	}

	// Store the index
	im.indexes[fieldName] = index

	// Add to pending indexes to be built
	im.pendingIndexes[fieldName] = struct{}{}

	return nil
}

// DropIndex removes an index for a field
func (im *IndexManager[T]) DropIndex(fieldName string) error {
	im.lock.Lock()
	defer im.lock.Unlock()

	// Check if the index exists
	index, exists := im.indexes[fieldName]
	if !exists {
		return nil // Index doesn't exist
	}

	// Close the index
	if err := index.Close(); err != nil {
		return fmt.Errorf("failed to close index for field '%s': %w", fieldName, err)
	}

	// Remove the index file
	indexFilePath := filepath.Join(
		filepath.Dir(im.db.file.Name()),
		fmt.Sprintf("%s.%s.idx", filepath.Base(im.db.file.Name()), fieldName),
	)
	if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove index file for field '%s': %w", fieldName, err)
	}

	// Remove from pending indexes if it's there
	delete(im.pendingIndexes, fieldName)

	// Remove from indexes map
	delete(im.indexes, fieldName)

	return nil
}

// BuildPendingIndexes builds all pending indexes by scanning the database
func (im *IndexManager[T]) BuildPendingIndexes() error {
	im.lock.Lock()
	pendingList := make([]string, 0, len(im.pendingIndexes))
	for fieldName := range im.pendingIndexes {
		pendingList = append(pendingList, fieldName)
	}
	im.lock.Unlock()

	// Build each pending index
	for _, fieldName := range pendingList {
		if err := im.BuildIndex(fieldName); err != nil {
			return err
		}
	}

	return nil
}

// BuildIndex builds or rebuilds an index for a field by scanning the database
func (im *IndexManager[T]) BuildIndex(fieldName string) error {
	im.lock.Lock()
	index, exists := im.indexes[fieldName]
	if !exists {
		im.lock.Unlock()
		return fmt.Errorf("index for field '%s' does not exist", fieldName)
	}

	// Find the field offset
	var fieldOffset FieldOffset
	var fieldFound bool
	for _, offset := range im.layout.FieldOffsets {
		if strings.EqualFold(offset.Name, fieldName) {
			fieldOffset = offset
			fieldFound = true
			break
		}
	}

	if !fieldFound {
		im.lock.Unlock()
		return fmt.Errorf("field '%s' not found in struct", fieldName)
	}
	im.lock.Unlock()

	// Get all record IDs from the database
	im.db.lock.RLock()
	recordIDs := make([]uint32, 0, len(im.db.index))
	for id := range im.db.index {
		recordIDs = append(recordIDs, id)
	}
	im.db.lock.RUnlock()

	// Process records in batches
	batchSize := 1000
	for i := 0; i < len(recordIDs); i += batchSize {
		end := i + batchSize
		if end > len(recordIDs) {
			end = len(recordIDs)
		}

		batch := recordIDs[i:end]
		for _, id := range batch {
			// Get the record
			record, err := im.db.Get(id)
			if err != nil || record == nil {
				continue // Skip problematic records
			}

			// Extract the field value using the field offset
			fieldPtr := unsafe.Pointer(uintptr(unsafe.Pointer(record)) + fieldOffset.Offset)
			var fieldValue interface{}

			// Get the field value based on its type
			switch fieldOffset.Type {
			case reflect.String:
				fieldValue = *(*string)(fieldPtr)
			case reflect.Int, reflect.Int32:
				fieldValue = *(*int)(fieldPtr)
			case reflect.Int64:
				fieldValue = *(*int64)(fieldPtr)
			case reflect.Uint, reflect.Uint32:
				fieldValue = *(*uint)(fieldPtr)
			case reflect.Uint64:
				fieldValue = *(*uint64)(fieldPtr)
			case reflect.Float32:
				fieldValue = *(*float32)(fieldPtr)
			case reflect.Float64:
				fieldValue = *(*float64)(fieldPtr)
			case reflect.Bool:
				fieldValue = *(*bool)(fieldPtr)
			default:
				continue // Skip unsupported types
			}

			// Insert into the index
			if err := index.Insert(fieldValue, id); err != nil {
				return fmt.Errorf("failed to index record %d: %w", id, err)
			}
		}
	}

	// Flush the index to ensure all changes are written
	if err := index.Flush(); err != nil {
		return fmt.Errorf("failed to flush index for field '%s': %w", fieldName, err)
	}

	// Mark as no longer pending
	im.lock.Lock()
	delete(im.pendingIndexes, fieldName)
	im.lock.Unlock()

	return nil
}

// InsertIntoIndexes adds a record to all relevant indexes
func (im *IndexManager[T]) InsertIntoIndexes(record *T, recordID uint32) error {
	im.lock.RLock()
	defer im.lock.RUnlock()

	for fieldName, index := range im.indexes {
		// Find the field offset
		var fieldOffset FieldOffset
		var fieldFound bool
		for _, offset := range im.layout.FieldOffsets {
			if strings.EqualFold(offset.Name, fieldName) {
				fieldOffset = offset
				fieldFound = true
				break
			}
		}

		if !fieldFound {
			continue // Skip this index
		}

		// Extract the field value
		fieldPtr := unsafe.Pointer(uintptr(unsafe.Pointer(record)) + fieldOffset.Offset)
		var fieldValue interface{}

		// Get the field value based on its type
		switch fieldOffset.Type {
		case reflect.String:
			fieldValue = *(*string)(fieldPtr)
		case reflect.Int, reflect.Int32:
			fieldValue = *(*int)(fieldPtr)
		case reflect.Int64:
			fieldValue = *(*int64)(fieldPtr)
		case reflect.Uint, reflect.Uint32:
			fieldValue = *(*uint)(fieldPtr)
		case reflect.Uint64:
			fieldValue = *(*uint64)(fieldPtr)
		case reflect.Float32:
			fieldValue = *(*float32)(fieldPtr)
		case reflect.Float64:
			fieldValue = *(*float64)(fieldPtr)
		case reflect.Bool:
			fieldValue = *(*bool)(fieldPtr)
		default:
			continue // Skip unsupported types
		}

		// Insert into the index
		if err := index.Insert(fieldValue, recordID); err != nil {
			return fmt.Errorf("failed to index record %d: %w", recordID, err)
		}
	}

	return nil
}

// RemoveFromIndexes removes a record from all relevant indexes
func (im *IndexManager[T]) RemoveFromIndexes(record *T, recordID uint32) error {
	im.lock.RLock()
	defer im.lock.RUnlock()

	for fieldName, index := range im.indexes {
		// Find the field offset
		var fieldOffset FieldOffset
		var fieldFound bool
		for _, offset := range im.layout.FieldOffsets {
			if strings.EqualFold(offset.Name, fieldName) {
				fieldOffset = offset
				fieldFound = true
				break
			}
		}

		if !fieldFound {
			continue // Skip this index
		}

		// Extract the field value
		fieldPtr := unsafe.Pointer(uintptr(unsafe.Pointer(record)) + fieldOffset.Offset)
		var fieldValue interface{}

		// Get the field value based on its type
		switch fieldOffset.Type {
		case reflect.String:
			fieldValue = *(*string)(fieldPtr)
		case reflect.Int, reflect.Int32:
			fieldValue = *(*int)(fieldPtr)
		case reflect.Int64:
			fieldValue = *(*int64)(fieldPtr)
		case reflect.Uint, reflect.Uint32:
			fieldValue = *(*uint)(fieldPtr)
		case reflect.Uint64:
			fieldValue = *(*uint64)(fieldPtr)
		case reflect.Float32:
			fieldValue = *(*float32)(fieldPtr)
		case reflect.Float64:
			fieldValue = *(*float64)(fieldPtr)
		case reflect.Bool:
			fieldValue = *(*bool)(fieldPtr)
		default:
			continue // Skip unsupported types
		}

		// Remove from the index
		if err := index.Remove(fieldValue, recordID); err != nil {
			return fmt.Errorf("failed to remove record %d from index: %w", recordID, err)
		}
	}

	return nil
}

// UpdateInIndexes updates a record in all relevant indexes
func (im *IndexManager[T]) UpdateInIndexes(oldRecord *T, newRecord *T, recordID uint32) error {
	// First remove the old record
	if err := im.RemoveFromIndexes(oldRecord, recordID); err != nil {
		return err
	}

	// Then insert the new record
	return im.InsertIntoIndexes(newRecord, recordID)
}

// Query finds records that match a field value
func (im *IndexManager[T]) Query(fieldName string, value interface{}) ([]uint32, error) {
	im.lock.RLock()
	index, exists := im.indexes[fieldName]
	im.lock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	// Find all matching records
	return index.Find(value)
}

// Close closes all indexes
func (im *IndexManager[T]) Close() error {
	im.lock.Lock()
	defer im.lock.Unlock()

	// First flush all indexes
	for _, index := range im.indexes {
		if err := index.Flush(); err != nil {
			return fmt.Errorf("failed to flush index: %w", err)
		}
	}

	// Then merge them with the database file
	for fieldName, index := range im.indexes {
		if err := index.mergeWithDatabase(); err != nil {
			return fmt.Errorf("failed to merge index '%s' with database: %w", fieldName, err)
		}
	}

	// Finally close all index files
	for _, index := range im.indexes {
		if err := index.Close(); err != nil {
			return fmt.Errorf("failed to close index: %w", err)
		}
	}

	// Clear the maps
	im.indexes = make(map[string]*BTreeIndex)
	im.pendingIndexes = make(map[string]struct{})

	return nil
}

// CheckIndexes checks if all indexes exist and creates any missing ones
func (im *IndexManager[T]) CheckIndexes() error {
	im.lock.Lock()
	defer im.lock.Unlock()

	// Get the base name of the database file
	dbBaseName := filepath.Base(im.db.file.Name())
	dirPath := filepath.Dir(im.db.file.Name())

	// Try to find all index files
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	// Pattern for index files: <dbname>.<fieldname>.idx
	prefix := dbBaseName + "."
	suffix := ".idx"

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) || !strings.HasSuffix(entry.Name(), suffix) {
			continue
		}

		// Extract field name
		fieldName := strings.TrimPrefix(entry.Name(), prefix)
		fieldName = strings.TrimSuffix(fieldName, suffix)

		// Check if we already have this index
		if _, exists := im.indexes[fieldName]; exists {
			continue
		}

		// Find the field in the layout
		var fieldOffset FieldOffset
		var fieldFound bool
		for _, offset := range im.layout.FieldOffsets {
			if strings.EqualFold(offset.Name, fieldName) {
				fieldOffset = offset
				fieldFound = true
				break
			}
		}

		if !fieldFound {
			// Field doesn't exist in current struct - skip or delete the index
			continue
		}

		// Try to load the index
		index, err := NewBTreeIndex(im.db.file.Name(), fieldName, fieldOffset.Offset, fieldOffset.Type)
		if err != nil {
			// If we fail to load, mark it for rebuild
			im.pendingIndexes[fieldName] = struct{}{}
		} else {
			im.indexes[fieldName] = index
		}
	}

	return nil
}

// ExtractIndexesFromDatabase extracts indexes embedded in the database file
func (im *IndexManager[T]) ExtractIndexesFromDatabase() error {
	// Get database file size
	dbInfo, err := im.db.file.Stat()
	if err != nil {
		return err
	}

	if dbInfo.Size() < 8 {
		return nil // File too small to have embedded indexes
	}

	// Read the last 8 bytes to check if there's an index marker
	marker := make([]byte, 8)
	if _, err := im.db.file.ReadAt(marker, dbInfo.Size()-8); err != nil {
		return fmt.Errorf("failed to read index marker: %w", err)
	}

	// Check if marker looks valid
	indexLoc := binary.LittleEndian.Uint64(marker)
	if indexLoc >= uint64(dbInfo.Size())-8 {
		return nil // Invalid marker
	}

	// Read index metadata to find all embedded indexes
	// This would need to parse the index data at the specified location
	// to identify all the indexes embedded in the file

	// Simplified approach: try to extract any index that might be there
	dirPath := filepath.Dir(im.db.file.Name())
	dbBaseName := filepath.Base(im.db.file.Name())

	// For each field in the layout, check if there's an index
	for _, offset := range im.layout.FieldOffsets {
		fieldName := offset.Name
		indexFileName := filepath.Join(dirPath, fmt.Sprintf("%s.%s.idx", dbBaseName, fieldName))

		// Try to extract the index
		if err := ExtractIndexFromDatabase(im.db.file.Name(), indexFileName); err != nil {
			// Just log and continue - this field might not have an index
			continue
		}

		// Try to load the extracted index
		index, err := NewBTreeIndex(im.db.file.Name(), fieldName, offset.Offset, offset.Type)
		if err != nil {
			// If we fail to load, mark it for rebuild
			im.pendingIndexes[fieldName] = struct{}{}
		} else {
			im.indexes[fieldName] = index
		}
	}

	return nil
}

// HasIndex checks if an index exists for a field
func (im *IndexManager[T]) HasIndex(fieldName string) bool {
	im.lock.RLock()
	defer im.lock.RUnlock()

	_, exists := im.indexes[fieldName]
	return exists
}

// GetIndexedFields returns a list of fields that have indexes
func (im *IndexManager[T]) GetIndexedFields() []string {
	im.lock.RLock()
	defer im.lock.RUnlock()

	fields := make([]string, 0, len(im.indexes))
	for field := range im.indexes {
		fields = append(fields, field)
	}
	return fields
}
