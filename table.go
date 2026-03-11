package embeddb

import (
	"encoding/binary"
	"fmt"
)

// Table provides typed table access for a specific struct type
type Table[T any] struct {
	db           *Database[T]
	name         string
	tableID      uint8
	layout       *StructLayout
	indexManager *IndexManager[T]
}

// Insert inserts a new record into the table
func (t *Table[T]) Insert(record *T) (uint32, error) {
	// Ensure table is registered
	if err := t.ensureTableRegistered(); err != nil {
		return 0, err
	}

	// Get next record ID from catalog
	recordID := t.db.tableCatalog.IncrementNextRecordID(t.name)

	// Encode the record
	encoded, err := t.db.encodeRecordWithLayout(record, t.layout)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}

	// Begin transaction
	if err := t.db.beginTransaction(); err != nil {
		return 0, fmt.Errorf("failed to begin insert transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			if err := t.db.rollbackTransaction(); err != nil {
				fmt.Printf("Error rolling back transaction: %v\n", err)
			}
		}
	}()

	t.db.lock.Lock()

	// Write record header: escCode + startMarker + tableID + id + length + active = 12 bytes
	headerBytes := make([]byte, 12)
	headerBytes[0] = escCode
	headerBytes[1] = startMarker
	headerBytes[2] = t.tableID

	binary.BigEndian.PutUint32(headerBytes[3:7], recordID)

	recordLength := uint32(len(encoded))
	binary.BigEndian.PutUint32(headerBytes[7:11], recordLength)

	headerBytes[11] = 1 // active

	footerBytes := []byte{escCode, endMarker}

	completeRecord := make([]byte, 0, len(headerBytes)+len(encoded)+len(footerBytes))
	completeRecord = append(completeRecord, headerBytes...)
	completeRecord = append(completeRecord, encoded...)
	completeRecord = append(completeRecord, footerBytes...)

	// Write the record
	nextOffset := t.db.header.nextOffset
	if err := t.db.writeData(completeRecord, int64(nextOffset)); err != nil {
		return 0, fmt.Errorf("failed to write record: %w", err)
	}

	// Update index
	t.db.index[recordID] = nextOffset
	t.db.header.nextOffset += uint32(len(completeRecord))

	// Update indexes
	if t.indexManager != nil {
		if err := t.indexManager.InsertIntoIndexes(record, recordID); err != nil {
			t.db.lock.Unlock()
			return 0, fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	t.db.lock.Unlock()

	// Commit
	if err := t.db.commitTransaction(); err != nil {
		return 0, fmt.Errorf("failed to commit insert transaction: %w", err)
	}
	committed = true

	// Reload mmap
	if err := t.db.ReloadMMap(); err != nil {
		fmt.Printf("Warning: failed to reload mmap after insert: %v\n", err)
	}

	return recordID, nil
}

// Get retrieves a record by ID
func (t *Table[T]) Get(id uint32) (*T, error) {
	t.db.lock.RLock()
	defer t.db.lock.RUnlock()

	// Look up the record offset
	offset, exists := t.db.index[id]
	if !exists {
		return nil, fmt.Errorf("record with id %d not found", id)
	}

	// Read the record bytes
	recordBytes, err := t.db.readRecordBytesAt(offset, t.tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to read record bytes: %w", err)
	}

	// Check if active
	if !isActiveRecord(recordBytes) {
		return nil, fmt.Errorf("record with id %d is not active", id)
	}

	// Decode the record
	return t.db.decodeRecordWithLayout(recordBytes, t.layout)
}

// Update updates an existing record
func (t *Table[T]) Update(id uint32, record *T) error {
	// Ensure table is registered
	if err := t.ensureTableRegistered(); err != nil {
		return err
	}

	// Begin transaction
	if err := t.db.beginTransaction(); err != nil {
		return fmt.Errorf("failed to begin update transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			if err := t.db.rollbackTransaction(); err != nil {
				fmt.Printf("Error rolling back transaction: %v\n", err)
			}
		}
	}()

	t.db.lock.Lock()

	// Check if record exists
	oldOffset, exists := t.db.index[id]
	if !exists {
		t.db.lock.Unlock()
		return fmt.Errorf("record with id %d not found", id)
	}

	// Read old record
	oldBytes, err := t.db.readRecordBytesAt(oldOffset, t.tableID)
	if err != nil {
		return fmt.Errorf("failed to read old record: %w", err)
	}

	if !isActiveRecord(oldBytes) {
		t.db.lock.Unlock()
		return fmt.Errorf("cannot update inactive record with id %d", id)
	}

	// Get old record for index update
	oldRecord, err := t.db.decodeRecordWithLayout(oldBytes, t.layout)
	if err != nil {
		t.db.lock.Unlock()
		return fmt.Errorf("failed to decode old record: %w", err)
	}

	// Mark old record as inactive
	_, err = t.db.file.WriteAt([]byte{0}, int64(oldOffset+11))
	if err != nil {
		t.db.lock.Unlock()
		return fmt.Errorf("failed to mark old record as inactive: %w", err)
	}

	// Encode updated record
	encoded, err := t.db.encodeRecordWithLayout(record, t.layout)
	if err != nil {
		t.db.lock.Unlock()
		return fmt.Errorf("failed to encode record: %w", err)
	}

	// Write new record header (12 bytes with tableID)
	headerBytes := make([]byte, 12)
	headerBytes[0] = escCode
	headerBytes[1] = startMarker
	headerBytes[2] = t.tableID

	binary.BigEndian.PutUint32(headerBytes[3:7], id)

	recordLength := uint32(len(encoded))
	binary.BigEndian.PutUint32(headerBytes[7:11], recordLength)

	headerBytes[11] = 1 // active

	footerBytes := []byte{escCode, endMarker}

	completeRecord := make([]byte, 0, len(headerBytes)+len(encoded)+len(footerBytes))
	completeRecord = append(completeRecord, headerBytes...)
	completeRecord = append(completeRecord, encoded...)
	completeRecord = append(completeRecord, footerBytes...)

	// Write updated record
	nextOffset := t.db.header.nextOffset
	if err := t.db.writeData(completeRecord, int64(nextOffset)); err != nil {
		return fmt.Errorf("failed to write updated record: %w", err)
	}

	// Update index
	t.db.index[id] = nextOffset
	t.db.header.nextOffset += uint32(len(completeRecord))

	// Update indexes
	if t.indexManager != nil && oldRecord != nil {
		if err := t.indexManager.UpdateInIndexes(oldRecord, record, id); err != nil {
			t.db.lock.Unlock()
			return fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	t.db.lock.Unlock()

	// Commit
	if err := t.db.commitTransaction(); err != nil {
		return fmt.Errorf("failed to commit update transaction: %w", err)
	}
	committed = true

	// Reload mmap
	if err := t.db.ReloadMMap(); err != nil {
		fmt.Printf("Warning: failed to reload mmap after update: %v\n", err)
	}

	return nil
}

// Delete soft-deletes a record
func (t *Table[T]) Delete(id uint32) error {
	// Begin transaction
	if err := t.db.beginTransaction(); err != nil {
		return fmt.Errorf("failed to begin delete transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			if err := t.db.rollbackTransaction(); err != nil {
				fmt.Printf("Error rolling back transaction: %v\n", err)
			}
		}
	}()

	t.db.lock.Lock()

	// Check if record exists
	offset, exists := t.db.index[id]
	if !exists {
		t.db.lock.Unlock()
		return fmt.Errorf("record with id %d not found", id)
	}

	// Check if already inactive
	recordBytes, err := t.db.readRecordBytesAt(offset, t.tableID)
	if err != nil {
		return fmt.Errorf("failed to read record: %w", err)
	}

	if !isActiveRecord(recordBytes) {
		t.db.lock.Unlock()
		return nil
	}

	// Get record for index update
	record, err := t.db.decodeRecordWithLayout(recordBytes, t.layout)
	if err != nil {
		t.db.lock.Unlock()
		return fmt.Errorf("failed to get record for index updates: %w", err)
	}

	// Mark as inactive
	_, err = t.db.file.WriteAt([]byte{0}, int64(offset+11))
	if err != nil {
		t.db.lock.Unlock()
		return fmt.Errorf("failed to mark record as deleted: %w", err)
	}

	// Write index
	if err := t.db.writeIndexLocked(); err != nil {
		t.db.lock.Unlock()
		return fmt.Errorf("failed to update index: %w", err)
	}

	// Update indexes
	if t.indexManager != nil && record != nil {
		if err := t.indexManager.RemoveFromIndexes(record, id); err != nil {
			t.db.lock.Unlock()
			return fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	t.db.lock.Unlock()

	// Commit
	if err := t.db.commitTransaction(); err != nil {
		return fmt.Errorf("failed to commit delete transaction: %w", err)
	}
	committed = true

	// Reload mmap
	if err := t.db.ReloadMMap(); err != nil {
		fmt.Printf("Warning: failed to reload mmap after delete: %v\n", err)
	}

	return nil
}

// Query finds records by indexed field value
func (t *Table[T]) Query(fieldName string, value interface{}) ([]T, error) {
	if t.indexManager == nil || !t.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	recordIDs, err := t.indexManager.Query(fieldName, value)
	if err != nil {
		return nil, err
	}

	results := make([]T, 0, len(recordIDs))
	for _, id := range recordIDs {
		record, err := t.Get(id)
		if err == nil && record != nil {
			results = append(results, *record)
		}
	}

	return results, nil
}

// QueryRangeGreaterThan finds records where field > value
func (t *Table[T]) QueryRangeGreaterThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	if t.indexManager == nil || !t.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	return t.indexManager.QueryRangeGreaterThan(fieldName, value, inclusive)
}

// QueryRangeLessThan finds records where field < value
func (t *Table[T]) QueryRangeLessThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	if t.indexManager == nil || !t.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	return t.indexManager.QueryRangeLessThan(fieldName, value, inclusive)
}

// QueryRangeBetween finds records where min <= field <= max
func (t *Table[T]) QueryRangeBetween(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]T, error) {
	if t.indexManager == nil || !t.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	return t.indexManager.QueryRangeBetween(fieldName, min, max, inclusiveMin, inclusiveMax)
}

// Filter scans all records and returns those matching the filter
func (t *Table[T]) Filter(fn func(T) bool) ([]T, error) {
	t.db.lock.RLock()
	defer t.db.lock.RUnlock()

	results := make([]T, 0)

	for id := range t.db.index {
		record, err := t.Get(id)
		if err != nil {
			continue
		}

		if fn(*record) {
			results = append(results, *record)
		}
	}

	return results, nil
}

// Scan iterates over all records
func (t *Table[T]) Scan(fn func(T) bool) error {
	t.db.lock.RLock()
	defer t.db.lock.RUnlock()

	for id := range t.db.index {
		record, err := t.Get(id)
		if err != nil {
			continue
		}

		if !fn(*record) {
			break
		}
	}

	return nil
}

// Count returns the number of records in the table
func (t *Table[T]) Count() int {
	t.db.lock.RLock()
	defer t.db.lock.RUnlock()

	return len(t.db.index)
}

// CreateIndex creates an index on a field
func (t *Table[T]) CreateIndex(fieldName string) error {
	if t.indexManager == nil {
		t.indexManager = NewIndexManager(t.db, t.layout)
	}
	return t.indexManager.CreateIndex(fieldName)
}

// DropIndex removes an index
func (t *Table[T]) DropIndex(fieldName string) error {
	if t.indexManager == nil {
		return nil
	}
	return t.indexManager.DropIndex(fieldName)
}

// ensureTableRegistered ensures the table is registered in the catalog
func (t *Table[T]) ensureTableRegistered() error {
	if t.db.tableCatalog == nil {
		t.db.tableCatalog = NewTableCatalog()
	}

	// Check if table already exists
	if _, ok := t.db.tableCatalog.GetTableID(t.name); ok {
		return nil
	}

	// Register the table
	t.db.tableCatalog.AddTable(t.name, t.layout.Hash)

	// Write the updated catalog
	return t.db.writeTableCatalog()
}

// Name returns the table name
func (t *Table[T]) Name() string {
	return t.name
}
