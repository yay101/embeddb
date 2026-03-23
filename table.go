package embeddb

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"

	embedcore "github.com/yay101/embeddbcore"
)

// Table provides typed table access for a specific struct type
type Table[T any] struct {
	db           *Database[T]
	name         string
	tableID      uint8
	layout       *embedcore.StructLayout
	indexManager *IndexManager[T]
}

// Insert inserts a new record into the table
func (t *Table[T]) Insert(record *T) (uint32, error) {
	// Ensure table is registered
	if err := t.ensureTableRegistered(); err != nil {
		return 0, err
	}

	// Serialize write operations
	t.db.writeLock.Lock()
	defer t.db.writeLock.Unlock()

	strictSync := t.db.needsCrossHandleSync()
	if strictSync {
		if err := t.db.syncStateFromDiskForWrite(); err != nil {
			return 0, fmt.Errorf("failed to sync database state before insert: %w", err)
		}
	}

	// Get next record ID from catalog (now inside write lock)
	recordID := t.db.tableCatalog.IncrementNextRecordID(t.name)

	// Encode the record
	encoded, err := t.db.encodeRecordWithLayout(record, t.layout)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}

	// Use main lock for header/index access
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
		t.db.lock.Unlock()
		return 0, fmt.Errorf("failed to write record: %w", err)
	}

	// Update index
	t.db.setRecordOffset(t.tableID, recordID, nextOffset)
	t.db.header.nextOffset += uint32(len(completeRecord))

	// Update indexes
	if t.indexManager != nil {
		if err := t.indexManager.InsertIntoIndexes(record, recordID); err != nil {
			t.db.lock.Unlock()
			return 0, fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	if strictSync {
		if err := t.db.writeIndexLocked(); err != nil {
			t.db.lock.Unlock()
			return 0, fmt.Errorf("failed to persist index after insert: %w", err)
		}
	}

	t.db.lock.Unlock()
	t.db.markMutation()

	return recordID, nil
}

// Get retrieves a record by ID
// Supports uint32, int, or string primary keys based on the struct definition
func (t *Table[T]) Get(id any) (*T, error) {
	t.db.lock.RLock()
	defer t.db.lock.RUnlock()

	var recordID uint32
	var offset uint32
	var exists bool

	// If no PK or PK is uint32, use internal index
	if t.layout.PrimaryKey >= 255 || t.layout.PKType == reflect.Uint32 {
		switch v := id.(type) {
		case uint32:
			recordID = v
		case int:
			recordID = uint32(v)
		case int64:
			recordID = uint32(v)
		default:
			return nil, fmt.Errorf("expected integer ID, got %T", id)
		}
		offset, exists = t.db.getRecordOffset(t.tableID, recordID)
		if !exists {
			return nil, fmt.Errorf("record with id %d not found", recordID)
		}
	} else {
		// Use the PK field's index
		pkField := t.layout.FieldOffsets[t.layout.PrimaryKey]
		if t.indexManager == nil {
			return nil, fmt.Errorf("no index manager for table")
		}

		recordIDs, err := t.indexManager.Query(pkField.Name, id)
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}
		if len(recordIDs) == 0 {
			return nil, fmt.Errorf("record not found")
		}
		// Use the first match (PKs should be unique)
		recordID = recordIDs[0]
		offset, exists = t.db.getRecordOffset(t.tableID, recordID)
		if !exists {
			return nil, fmt.Errorf("record not found")
		}
	}

	// Read the record bytes
	recordBytes, err := t.db.readRecordBytesAt(offset, t.tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to read record bytes: %w", err)
	}

	// Check if active
	if !isActiveRecord(recordBytes) {
		return nil, fmt.Errorf("record is not active")
	}

	// Decode the record
	return t.db.decodeRecordWithLayout(recordBytes, t.layout)
}

// Update updates an existing record
func (t *Table[T]) Update(id any, record *T) error {
	// Ensure table is registered
	if err := t.ensureTableRegistered(); err != nil {
		return err
	}

	// Serialize write operations
	t.db.writeLock.Lock()

	strictSync := t.db.needsCrossHandleSync()
	if strictSync {
		if err := t.db.syncStateFromDiskForWrite(); err != nil {
			t.db.writeLock.Unlock()
			return fmt.Errorf("failed to sync database state before update: %w", err)
		}
	}

	t.db.lock.Lock()
	defer t.db.lock.Unlock()
	defer t.db.writeLock.Unlock()

	// Resolve ID to internal uint32
	var recordID uint32
	var oldOffset uint32
	var exists bool

	if t.layout.PrimaryKey >= 255 || t.layout.PKType == reflect.Uint32 {
		// Use internal index
		switch v := id.(type) {
		case uint32:
			recordID = v
		case int:
			recordID = uint32(v)
		case int64:
			recordID = uint32(v)
		default:
			return fmt.Errorf("expected integer ID, got %T", id)
		}
		oldOffset, exists = t.db.getRecordOffset(t.tableID, recordID)
		if !exists {
			return fmt.Errorf("record with id %d not found", recordID)
		}
	} else {
		// Use PK index
		pkField := t.layout.FieldOffsets[t.layout.PrimaryKey]
		if t.indexManager == nil {
			return fmt.Errorf("no index manager for table")
		}

		recordIDs, err := t.indexManager.Query(pkField.Name, id)
		if err != nil {
			return fmt.Errorf("query failed: %w", err)
		}
		if len(recordIDs) == 0 {
			return fmt.Errorf("record not found")
		}
		recordID = recordIDs[0]
		oldOffset, exists = t.db.getRecordOffset(t.tableID, recordID)
		if !exists {
			return fmt.Errorf("record not found")
		}
	}

	// Read old record
	oldBytes, err := t.db.readRecordBytesAt(oldOffset, t.tableID)
	if err != nil {
		return fmt.Errorf("failed to read old record: %w", err)
	}

	if !isActiveRecord(oldBytes) {
		return fmt.Errorf("cannot update inactive record")
	}

	// Get old record for index update
	oldRecord, err := t.db.decodeRecordWithLayout(oldBytes, t.layout)
	if err != nil {
		return fmt.Errorf("failed to decode old record: %w", err)
	}

	// Mark old record as inactive
	_, err = t.db.file.WriteAt([]byte{0}, int64(oldOffset+11))
	if err != nil {
		return fmt.Errorf("failed to mark old record as inactive: %w", err)
	}

	// Encode updated record
	encoded, err := t.db.encodeRecordWithLayout(record, t.layout)
	if err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}

	// Write new record header (12 bytes with tableID)
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

	// Write updated record
	nextOffset := t.db.header.nextOffset
	if err := t.db.writeData(completeRecord, int64(nextOffset)); err != nil {
		return fmt.Errorf("failed to write updated record: %w", err)
	}

	// Update index
	t.db.setRecordOffset(t.tableID, recordID, nextOffset)
	t.db.header.nextOffset += uint32(len(completeRecord))

	// Update indexes
	if t.indexManager != nil && oldRecord != nil {
		if err := t.indexManager.UpdateInIndexes(oldRecord, record, recordID); err != nil {
			return fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	if strictSync {
		if err := t.db.writeIndexLocked(); err != nil {
			return fmt.Errorf("failed to persist index after update: %w", err)
		}
	}

	return nil
}

// Delete soft-deletes a record
func (t *Table[T]) Delete(id any) error {
	// Serialize write operations
	t.db.writeLock.Lock()
	defer t.db.writeLock.Unlock()

	if t.db.needsCrossHandleSync() {
		if err := t.db.syncStateFromDiskForWrite(); err != nil {
			return fmt.Errorf("failed to sync database state before delete: %w", err)
		}
	}

	t.db.lock.Lock()
	defer t.db.lock.Unlock()

	// Resolve ID to internal uint32
	var recordID uint32
	var offset uint32
	var exists bool

	if t.layout.PrimaryKey >= 255 || t.layout.PKType == reflect.Uint32 {
		// Use internal index
		switch v := id.(type) {
		case uint32:
			recordID = v
		case int:
			recordID = uint32(v)
		case int64:
			recordID = uint32(v)
		default:
			return fmt.Errorf("expected integer ID, got %T", id)
		}
		offset, exists = t.db.getRecordOffset(t.tableID, recordID)
		if !exists {
			return fmt.Errorf("record with id %d not found", recordID)
		}
	} else {
		// Use PK index
		pkField := t.layout.FieldOffsets[t.layout.PrimaryKey]
		if t.indexManager == nil {
			return fmt.Errorf("no index manager for table")
		}

		recordIDs, err := t.indexManager.Query(pkField.Name, id)
		if err != nil {
			return fmt.Errorf("query failed: %w", err)
		}
		if len(recordIDs) == 0 {
			return fmt.Errorf("record not found")
		}
		recordID = recordIDs[0]
		offset, exists = t.db.getRecordOffset(t.tableID, recordID)
		if !exists {
			return fmt.Errorf("record not found")
		}
	}

	// Check if already inactive
	recordBytes, err := t.db.readRecordBytesAt(offset, t.tableID)
	if err != nil {
		return fmt.Errorf("failed to read record: %w", err)
	}

	if !isActiveRecord(recordBytes) {
		return nil
	}

	// Get record for index update
	record, err := t.db.decodeRecordWithLayout(recordBytes, t.layout)
	if err != nil {
		return fmt.Errorf("failed to get record for index updates: %w", err)
	}

	// Mark as inactive
	_, err = t.db.file.WriteAt([]byte{0}, int64(offset+11))
	if err != nil {
		return fmt.Errorf("failed to mark record as deleted: %w", err)
	}

	// Write index
	if err := t.db.writeIndexLocked(); err != nil {
		return fmt.Errorf("failed to update index: %w", err)
	}

	// Update indexes
	if t.indexManager != nil && record != nil {
		if err := t.indexManager.RemoveFromIndexes(record, recordID); err != nil {
			return fmt.Errorf("failed to update indexes: %w", err)
		}
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
	slices.Sort(recordIDs)
	t.db.lock.RLock()
	defer t.db.lock.RUnlock()
	for _, id := range recordIDs {
		record, err := t.db.getLockedForTable(id, t.tableID)
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
	scanner := t.ScanRecords()
	defer scanner.Close()

	results := make([]T, 0)

	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}

		if fn(*record) {
			results = append(results, *record)
		}
	}

	return results, scanner.Err()
}

// Scanner provides efficient sequential record scanning
type Scanner[T any] struct {
	db        *Database[T]
	tableID   uint8
	layout    *embedcore.StructLayout
	indexSnap []uint32 // Snapshot of record IDs
	pos       int
	buf       []byte
	err       error
}

// ScanRecords returns a Scanner for efficient sequential access
// The scanner uses batch reads for better cache locality and doesn't
// hold the database lock during iteration
func (t *Table[T]) ScanRecords() *Scanner[T] {
	// Quick snapshot of index keys
	t.db.lock.RLock()
	idx := t.db.indexes[t.tableID]
	if idx == nil {
		t.db.lock.RUnlock()
		return &Scanner[T]{db: t.db, tableID: t.tableID, layout: t.layout}
	}

	// Copy keys to slice
	ids := make([]uint32, 0, len(idx))
	for id := range idx {
		ids = append(ids, id)
	}
	t.db.lock.RUnlock()

	return &Scanner[T]{
		db:        t.db,
		tableID:   t.tableID,
		layout:    t.layout,
		indexSnap: ids,
		pos:       0,
		buf:       make([]byte, 4096),
	}
}

// Next advances the scanner to the next record
// Returns false when iteration is complete or on error
func (s *Scanner[T]) Next() bool {
	if s.err != nil || s.pos >= len(s.indexSnap) {
		return false
	}

	offset, exists := s.db.getRecordOffset(s.tableID, s.indexSnap[s.pos])
	if !exists {
		s.pos++
		return s.Next()
	}

	// Read record
	recordBytes, err := s.db.readRecordBytesAt(offset, s.tableID)
	if err != nil {
		s.err = err
		return false
	}

	// Check if active
	if !isActiveRecord(recordBytes) {
		s.pos++
		return s.Next()
	}

	// Decode into buffer
	s.buf = recordBytes
	s.pos++
	return true
}

// Record returns the current record
// Must be called after Next() returns true
func (s *Scanner[T]) Record() (*T, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.db.decodeRecordWithLayout(s.buf, s.layout)
}

// Err returns the last error encountered
func (s *Scanner[T]) Err() error {
	return s.err
}

// Close releases any resources
func (s *Scanner[T]) Close() {
	s.indexSnap = nil
	s.buf = nil
}

// Scan iterates over all records
func (t *Table[T]) Scan(fn func(T) bool) error {
	scanner := t.ScanRecords()
	defer scanner.Close()

	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}

		if !fn(*record) {
			break
		}
	}

	return scanner.Err()
}

// Count returns the number of records in the table
func (t *Table[T]) Count() int {
	t.db.lock.RLock()
	defer t.db.lock.RUnlock()

	return len(t.db.indexes[t.tableID])
}

// CreateIndex creates an index on a field
func (t *Table[T]) CreateIndex(fieldName string) error {
	if t.indexManager == nil {
		t.indexManager = NewIndexManager(t.db, t.layout, t.name)
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

// Drop marks the table as dropped (soft delete)
// The table's data will be cleaned up during Vacuum()
// After Drop, the table can no longer be used for inserts/queries
func (t *Table[T]) Drop() error {
	if t.db.tableCatalog == nil {
		return nil
	}
	t.db.tableCatalog.DropTable(t.name)
	t.db.markTableDropped()
	return t.db.writeTableCatalog()
}

// IsDropped returns true if the table has been dropped
func (t *Table[T]) IsDropped() bool {
	if t.db.tableCatalog == nil {
		return false
	}
	return t.db.tableCatalog.IsTableDropped(t.name)
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

// GetIndexedFields returns a list of fields that have indexes
func (t *Table[T]) GetIndexedFields() []string {
	if t.indexManager == nil {
		return nil
	}
	return t.indexManager.GetIndexedFields()
}

// QueryPaged finds records that match a field value with pagination
func (t *Table[T]) QueryPaged(fieldName string, value interface{}, offset, limit int) (*PagedResult[T], error) {
	if t.indexManager == nil || !t.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	all, err := t.Query(fieldName, value)
	if err != nil {
		return nil, err
	}
	return paginateTableResults(all, offset, limit), nil
}

// QueryRangeGreaterThanPaged finds records where field > value with pagination
func (t *Table[T]) QueryRangeGreaterThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	if t.indexManager == nil || !t.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	all, err := t.QueryRangeGreaterThan(fieldName, value, inclusive)
	if err != nil {
		return nil, err
	}
	return paginateTableResults(all, offset, limit), nil
}

// QueryRangeLessThanPaged finds records where field < value with pagination
func (t *Table[T]) QueryRangeLessThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	if t.indexManager == nil || !t.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	all, err := t.QueryRangeLessThan(fieldName, value, inclusive)
	if err != nil {
		return nil, err
	}
	return paginateTableResults(all, offset, limit), nil
}

// QueryRangeBetweenPaged finds records where min <= field <= max with pagination
func (t *Table[T]) QueryRangeBetweenPaged(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool, offset, limit int) (*PagedResult[T], error) {
	if t.indexManager == nil || !t.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	all, err := t.QueryRangeBetween(fieldName, min, max, inclusiveMin, inclusiveMax)
	if err != nil {
		return nil, err
	}
	return paginateTableResults(all, offset, limit), nil
}

func paginateTableResults[T any](all []T, offset, limit int) *PagedResult[T] {
	if limit < 0 {
		limit = 0
	}
	if offset < 0 {
		offset = 0
	}
	total := len(all)
	if offset >= total || limit == 0 {
		return &PagedResult[T]{
			Records:    []T{},
			TotalCount: total,
			HasMore:    false,
			Offset:     offset,
			Limit:      limit,
		}
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return &PagedResult[T]{
		Records:    all[offset:end],
		TotalCount: total,
		HasMore:    end < total,
		Offset:     offset,
		Limit:      limit,
	}
}

// FilterPaged scans all records and returns those matching the filter with pagination
func (t *Table[T]) FilterPaged(fn func(T) bool, offset, limit int) (*PagedResult[T], error) {
	t.db.lock.RLock()
	defer t.db.lock.RUnlock()

	var results []T
	totalCount := 0
	skipped := 0

	for id := range t.db.indexes[t.tableID] {
		record, err := t.Get(id)
		if err != nil {
			continue
		}

		if fn(*record) {
			totalCount++
			if skipped < offset {
				skipped++
				continue
			}
			if len(results) < limit {
				results = append(results, *record)
			}
		}
	}

	if results == nil {
		results = []T{}
	}

	hasMore := (skipped + len(results)) < totalCount

	return &PagedResult[T]{
		Records:    results,
		TotalCount: totalCount,
		HasMore:    hasMore,
		Offset:     offset,
		Limit:      limit,
	}, nil
}
