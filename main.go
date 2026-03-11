package embeddb

import (
	"fmt"
	"os"
	"reflect"
	"sync"

	"golang.org/x/exp/mmap"
)

// Record format description:
// [escCode,startMarker]2 bytes - Marks the beginning of a record.
// [id]4 bytes - The unique identifier for the record (uint32).
// [length]4 bytes - The length of the actual record data (uint32).
// [active]1 byte - Status of the record (1 for active, 0 for inactive).
// -- Actual record data bytes --
// [escCode,endMarker]2 bytes - Marks the end of a record.

// Database is a generic type that represents a database for storing records of type T.
// For multi-table support, use db.Table() to get typed table access.
type Database[T any] struct {
	header       DBHeader
	lock         sync.RWMutex
	writeLock    sync.Mutex // Serializes write operations
	file         *os.File
	mfile        *mmap.ReaderAt
	mlock        sync.RWMutex
	indexes      map[uint8]map[uint32]uint32 // tableID -> recordID -> file offset
	layout       *StructLayout               // Struct layout information using unsafe
	nextRecordID uint32
	nlock        sync.Mutex
	transaction  *Transaction     // Transaction manager for atomicity
	indexManager *IndexManager[T] // Manager for field indexes
	tableCatalog *TableCatalog    // Table catalog for multi-table support
	defaultTable string           // Default table name for single-table mode
}

type DBHeader struct {
	Version            string
	indexStart         uint32
	indexEnd           uint32
	nextRecordID       uint32
	nextOffset         uint32
	tocStart           uint32
	entryStart         uint32
	lgIndexStart       uint32 // Last good index start position
	lock               sync.Mutex
	indexCapacity      uint32 // Capacity allocated for the index
	tableCatalogOffset uint32 // Offset to table catalog
	tableCount         uint32 // Number of tables
}

// This type is no longer used and has been replaced by FieldOffset in field_offsets.go

const (
	headerSize                int          = 52
	chunkSize                 int          = 4096
	escCode                   byte         = 0x1B
	startMarker               byte         = 0x02
	endMarker                 byte         = 0x03
	valueStartMarker          byte         = 0x1E
	valueEndMarker            byte         = 0x1F
	embeddedStructType        reflect.Kind = reflect.Struct // Use reflect.Struct
	defaultIndexPreallocation uint32       = 10240          // 10KB preallocated for index by default
	Version                   string       = "0.1.0"
	defaultAutoIndexFields    bool         = false // Whether to auto-index tagged fields
)

// DB is a non-generic database that can store multiple table types.
// Note: For typed table access, use the generic Database[T] with db.Table() method.
// Example: db, _ := embeddb.New[User]("file.db", false, false); users, _ := db.Table()
type DB struct {
	*Database[any]
}

// Table returns a typed Table[T] for accessing records.
// If name is not provided or empty, the table name is auto-derived from the type name.
// Example: db.Table() or db.Table("users")
func (db *Database[T]) Table(name ...string) (*Table[T], error) {
	var instance T
	layout, err := ComputeStructLayout(instance)
	if err != nil {
		return nil, fmt.Errorf("failed to compute struct layout: %w", err)
	}

	tableName := reflect.TypeOf(instance).Name()
	if len(name) > 0 && name[0] != "" {
		tableName = name[0]
	}

	if db.tableCatalog == nil {
		db.tableCatalog = NewTableCatalog()
	}

	tableID := db.tableCatalog.AddTable(tableName, layout.Hash)
	indexManager := NewIndexManager(db, layout)

	return &Table[T]{
		db:           db,
		name:         tableName,
		tableID:      tableID,
		layout:       layout,
		indexManager: indexManager,
	}, nil
}

func (db *Database[T]) nextId() uint32 {
	db.nlock.Lock()
	defer db.nlock.Unlock()
	id := db.nextRecordID
	db.nextRecordID++
	return id
}

// LegacyIndex returns the index for table 0 (backward compatibility)
func (db *Database[T]) LegacyIndex() map[uint32]uint32 {
	return db.indexes[0]
}

// getTableIndex returns the index for a specific table, creating if needed
func (db *Database[T]) getTableIndex(tableID uint8) map[uint32]uint32 {
	if db.indexes == nil {
		db.indexes = make(map[uint8]map[uint32]uint32)
	}
	if db.indexes[tableID] == nil {
		db.indexes[tableID] = make(map[uint32]uint32)
	}
	return db.indexes[tableID]
}

// setRecordOffset sets the file offset for a record in a specific table
func (db *Database[T]) setRecordOffset(tableID uint8, recordID uint32, offset uint32) {
	db.getTableIndex(tableID)[recordID] = offset
}

// getRecordOffset gets the file offset for a record in a specific table
func (db *Database[T]) getRecordOffset(tableID uint8, recordID uint32) (uint32, bool) {
	if db.indexes == nil {
		return 0, false
	}
	idx, ok := db.indexes[tableID]
	if !ok {
		return 0, false
	}
	offset, ok := idx[recordID]
	return offset, ok
}

// hasRecord checks if a record exists in a specific table
func (db *Database[T]) hasRecord(tableID uint8, recordID uint32) bool {
	_, ok := db.getRecordOffset(tableID, recordID)
	return ok
}

// deleteRecord removes a record from a specific table's index
func (db *Database[T]) deleteRecord(tableID uint8, recordID uint32) {
	if db.indexes != nil {
		if idx, ok := db.indexes[tableID]; ok {
			delete(idx, recordID)
		}
	}
}

// tableRecordCount returns the number of records in a specific table
func (db *Database[T]) tableRecordCount(tableID uint8) int {
	if db.indexes == nil {
		return 0
	}
	if idx, ok := db.indexes[tableID]; ok {
		return len(idx)
	}
	return 0
}

// CreateIndex creates an index for the specified field
func (db *Database[T]) CreateIndex(fieldName string) error {
	if db.indexManager == nil {
		db.indexManager = NewIndexManager(db, db.layout)
	}
	return db.indexManager.CreateIndex(fieldName)
}

// DropIndex removes an index for the specified field
func (db *Database[T]) DropIndex(fieldName string) error {
	if db.indexManager == nil {
		return nil
	}
	return db.indexManager.DropIndex(fieldName)
}

// Close closes the database and any open indexes
func (db *Database[T]) Close() error {
	// Persist the index and header before closing
	if err := db.Sync(); err != nil {
		return fmt.Errorf("failed to sync before close: %w", err)
	}

	// Close indexes
	if db.indexManager != nil {
		if err := db.indexManager.Close(); err != nil {
			return err
		}
	}

	// Close memory mapping
	db.mlock.Lock()
	if db.mfile != nil {
		db.mfile.Close()
		db.mfile = nil
	}
	db.mlock.Unlock()

	// Close file
	return db.file.Close()
}

// Sync persists the in-memory index and header to disk.
// This should be called periodically for durability, and is automatically
// called by Close().
func (db *Database[T]) Sync() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Write the index to disk
	if err := db.writeIndexLocked(); err != nil {
		return fmt.Errorf("failed to write index: %w", err)
	}

	// Write the table catalog to disk (must be after index to avoid overwrite)
	if err := db.writeTableCatalogLocked(); err != nil {
		return fmt.Errorf("failed to write table catalog: %w", err)
	}

	// Write the header to disk
	if err := db.encodeHeaderLocked(); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Sync the file
	return db.file.Sync()
}

// Query finds all records that match a field value using an index
func (db *Database[T]) Query(fieldName string, value interface{}) ([]T, error) {
	if db.indexManager == nil || !db.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	// Find matching record IDs using the index
	recordIDs, err := db.indexManager.Query(fieldName, value)
	if err != nil {
		return nil, err
	}

	// Fetch the actual records
	results := make([]T, 0, len(recordIDs))
	for _, id := range recordIDs {
		record, err := db.Get(id)
		if err == nil && record != nil {
			results = append(results, *record)
		}
	}

	return results, nil
}

// QueryRangeGreaterThan finds all records where field > value (or >= if inclusive)
func (db *Database[T]) QueryRangeGreaterThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	if db.indexManager == nil || !db.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	return db.indexManager.QueryRangeGreaterThan(fieldName, value, inclusive)
}

// QueryRangeLessThan finds all records where field < value (or <= if inclusive)
func (db *Database[T]) QueryRangeLessThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	if db.indexManager == nil || !db.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	return db.indexManager.QueryRangeLessThan(fieldName, value, inclusive)
}

// QueryRangeBetween finds all records where min <= field <= max
func (db *Database[T]) QueryRangeBetween(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]T, error) {
	if db.indexManager == nil || !db.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	return db.indexManager.QueryRangeBetween(fieldName, min, max, inclusiveMin, inclusiveMax)
}

// FilterFunc is a function that returns true if a record matches the filter criteria
type FilterFunc[T any] func(record T) bool

// Filter scans all records and returns those that match the filter function.
// This performs an unindexed full table scan - use only when no index is available.
func (db *Database[T]) Filter(fn FilterFunc[T]) ([]T, error) {
	// Quick snapshot of index keys
	db.lock.RLock()
	var idx map[uint32]uint32
	var tableID uint8 = 0

	// Find non-empty index (prefer table 0 for legacy, but use any available)
	for tid, m := range db.indexes {
		if len(m) > 0 {
			idx = m
			tableID = tid
			break
		}
	}

	if idx == nil {
		db.lock.RUnlock()
		return nil, nil
	}

	ids := make([]uint32, 0, len(idx))
	for id := range idx {
		ids = append(ids, id)
	}
	db.lock.RUnlock()

	// Iterate without holding lock
	results := make([]T, 0)
	for _, id := range ids {
		offset, exists := idx[id]
		if !exists {
			continue
		}

		recordBytes, err := db.readRecordBytesAt(offset, tableID)
		if err != nil {
			continue
		}

		if !isActiveRecord(recordBytes) {
			continue
		}

		record, err := db.decodeRecord(recordBytes)
		if err != nil {
			continue
		}

		if fn(*record) {
			results = append(results, *record)
		}
	}

	return results, nil
}

// Scan iterates over all records, calling the callback for each.
// This performs an unindexed full table scan - use only when no index is available.
// The callback can return false to stop iteration early.
func (db *Database[T]) Scan(fn func(record T) bool) error {
	db.lock.RLock()
	defer db.lock.RUnlock()

	// For backward compatibility, use table 0 (legacy single-table mode)
	idx := db.indexes[0]
	for id := range idx {
		record, err := db.getLocked(id)
		if err != nil {
			continue // Skip records that can't be read
		}

		if !fn(*record) {
			break // Stop iteration if callback returns false
		}
	}

	return nil
}

// Count returns the total number of records in the database
func (db *Database[T]) Count() int {
	db.lock.RLock()
	defer db.lock.RUnlock()

	// Sum records across all tables
	total := 0
	for _, idx := range db.indexes {
		total += len(idx)
	}
	return total
}

// QueryPaged finds records that match a field value using an index with pagination
func (db *Database[T]) QueryPaged(fieldName string, value interface{}, offset, limit int) (*PagedResult[T], error) {
	if db.indexManager == nil || !db.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	return db.indexManager.QueryPaged(fieldName, value, offset, limit)
}

// QueryRangeGreaterThanPaged finds records where field > value with pagination
func (db *Database[T]) QueryRangeGreaterThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	if db.indexManager == nil || !db.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	return db.indexManager.QueryRangeGreaterThanPaged(fieldName, value, inclusive, offset, limit)
}

// QueryRangeLessThanPaged finds records where field < value with pagination
func (db *Database[T]) QueryRangeLessThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	if db.indexManager == nil || !db.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	return db.indexManager.QueryRangeLessThanPaged(fieldName, value, inclusive, offset, limit)
}

// QueryRangeBetweenPaged finds records where min <= field <= max with pagination
func (db *Database[T]) QueryRangeBetweenPaged(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool, offset, limit int) (*PagedResult[T], error) {
	if db.indexManager == nil || !db.indexManager.HasIndex(fieldName) {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}
	return db.indexManager.QueryRangeBetweenPaged(fieldName, min, max, inclusiveMin, inclusiveMax, offset, limit)
}

// FilterPaged scans all records and returns those that match the filter function with pagination
func (db *Database[T]) FilterPaged(fn FilterFunc[T], offset, limit int) (*PagedResult[T], error) {
	// Quick snapshot of index keys
	db.lock.RLock()
	var idx map[uint32]uint32
	var tableID uint8

	for tid, m := range db.indexes {
		if len(m) > 0 {
			idx = m
			tableID = tid
			break
		}
	}

	if idx == nil {
		db.lock.RUnlock()
		return &PagedResult[T]{Records: []T{}}, nil
	}

	ids := make([]uint32, 0, len(idx))
	for id := range idx {
		ids = append(ids, id)
	}
	db.lock.RUnlock()

	// Iterate without holding lock
	var results []T
	totalCount := 0
	skipped := 0

	for _, id := range ids {
		offsetVal, exists := idx[id]
		if !exists {
			continue
		}

		recordBytes, err := db.readRecordBytesAt(offsetVal, tableID)
		if err != nil {
			continue
		}

		if !isActiveRecord(recordBytes) {
			continue
		}

		record, err := db.decodeRecord(recordBytes)
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
