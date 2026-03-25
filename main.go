package embeddb

import (
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	embedcore "github.com/yay101/embeddbcore"
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
	header             DBHeader
	lock               sync.RWMutex
	writeLock          *sync.Mutex // Serializes writes per database file
	fileKey            string
	sharedState        *sharedFileState
	file               *os.File
	mfile              *mmap.ReaderAt
	mlock              sync.RWMutex
	indexes            map[uint8]map[uint32]uint32 // tableID -> recordID -> file offset
	layout             *embedcore.StructLayout     // Struct layout information using unsafe
	nextRecordID       uint32
	nlock              sync.Mutex
	transaction        *Transaction       // Transaction manager for atomicity
	indexManager       *IndexManager[T]   // Manager for field indexes
	tableCatalog       *TableCatalog      // Table catalog for multi-table support
	defaultTable       string             // Default table name for single-table mode
	tableIndexManagers []*IndexManager[T] // Track table index managers for cleanup

	autoVacuumEnabled       bool
	changesSinceVacuum      uint64
	tableDroppedSinceVacuum bool
	lastVacuumTime          time.Time
	vacuumStateLock         sync.Mutex
	regionStartHint         int64
	regionSafeUntil         int64
	regionCheckCounter      uint64
}

type DBHeader struct {
	Version                      string
	indexStart                   uint32
	indexEnd                     uint32
	nextRecordID                 uint32
	nextOffset                   uint32
	tocStart                     uint32
	entryStart                   uint32
	lgIndexStart                 uint32 // Last good index start position
	lock                         sync.Mutex
	indexCapacity                uint32 // Capacity allocated for the index
	tableCatalogOffset           uint32 // Offset to table catalog
	tableCount                   uint32 // Number of tables
	secondaryIndexRegionStart    uint32 // Embedded secondary index region start
	secondaryIndexRegionCapacity uint32 // Embedded secondary index region capacity
	secondaryIndexRegionUsed     uint32 // Embedded secondary index region used bytes
	secondaryIndexRegionFlags    uint32 // Embedded secondary index region flags/version
}

// This type is no longer used and has been replaced by FieldOffset in field_offsets.go

const (
	headerSize                int          = 64
	chunkSize                 int          = 4096
	escCode                   byte         = 0x1B
	startMarker               byte         = 0x02
	endMarker                 byte         = 0x03
	valueStartMarker          byte         = 0x1E
	valueEndMarker            byte         = 0x1F
	embeddedStructType        reflect.Kind = reflect.Struct // Use reflect.Struct
	defaultIndexPreallocation uint32       = 10240          // 10KB preallocated for index by default
	Version                   string       = "1.0.0"
	defaultAutoIndexFields    bool         = false // Whether to auto-index tagged fields
	autoVacuumInterval                     = 24 * time.Hour
	autoVacuumMinChanges      uint64       = 50000
	regionOverlapCheckEvery   uint64       = 1024
	regionOverlapGuardBytes   int64        = 64 * 1024
)

// Table returns a typed Table[T] for accessing records.
// If name is not provided or empty, the table name is auto-derived from the type name.
// Example: db.Table() or db.Table("users")
func (db *Database[T]) Table(name ...string) (*Table[T], error) {
	var instance T
	layout, err := embedcore.ComputeStructLayout(instance)
	if err != nil {
		return nil, fmt.Errorf("failed to compute struct layout: %w", err)
	}

	tableName := reflect.TypeOf(instance).Name()
	if len(name) > 0 && name[0] != "" {
		tableName = name[0]
	}

	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	if db.needsCrossHandleSync() {
		if err := db.syncStateFromDiskForWrite(); err != nil {
			return nil, fmt.Errorf("failed to sync database state for table access: %w", err)
		}
	}

	if db.tableCatalog == nil {
		db.tableCatalog = NewTableCatalog()
	}

	tableID, exists := db.tableCatalog.GetTableID(tableName)
	if !exists {
		tableID = db.tableCatalog.AddTable(tableName, layout.Hash)
		db.lock.Lock()
		if err := db.writeTableCatalogLocked(); err != nil {
			db.lock.Unlock()
			return nil, fmt.Errorf("failed to persist table catalog: %w", err)
		}
		db.lock.Unlock()
	}
	indexManager := NewIndexManager(db, layout, tableName)

	// Check for existing indexes on disk and load them
	if err := indexManager.CheckIndexes(); err != nil {
		fmt.Printf("Warning: failed to check indexes for table '%s': %v\n", tableName, err)
	}

	// Track the table's index manager for cleanup when database closes
	db.tableIndexManagers = append(db.tableIndexManagers, indexManager)

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

func (db *Database[T]) markMutation() {
	db.vacuumStateLock.Lock()
	db.changesSinceVacuum++
	db.vacuumStateLock.Unlock()
}

func (db *Database[T]) markTableDropped() {
	db.vacuumStateLock.Lock()
	db.tableDroppedSinceVacuum = true
	db.vacuumStateLock.Unlock()
}

func (db *Database[T]) markVacuumCompleted() {
	db.vacuumStateLock.Lock()
	db.lastVacuumTime = time.Now()
	db.changesSinceVacuum = 0
	db.tableDroppedSinceVacuum = false
	db.vacuumStateLock.Unlock()
}

func (db *Database[T]) shouldAutoVacuum() bool {
	if !db.autoVacuumEnabled {
		return false
	}
	db.vacuumStateLock.Lock()
	defer db.vacuumStateLock.Unlock()
	if time.Since(db.lastVacuumTime) < autoVacuumInterval {
		return false
	}
	return db.tableDroppedSinceVacuum || db.changesSinceVacuum >= autoVacuumMinChanges
}

func (db *Database[T]) maybeAutoVacuum() error {
	if !db.shouldAutoVacuum() {
		return nil
	}
	if err := db.Vacuum(); err != nil {
		return fmt.Errorf("auto vacuum failed: %w", err)
	}
	return nil
}

// CreateIndex creates an index for the specified field
// Note: When used on Database directly (not via Table), tableName is empty
// so indexes are stored without table prefix for backward compatibility
func (db *Database[T]) CreateIndex(fieldName string) error {
	if db.indexManager == nil {
		db.indexManager = NewIndexManager(db, db.layout, "")
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
	defer db.unregisterHandle()

	// Persist the index and header before closing
	if err := db.Sync(); err != nil {
		return fmt.Errorf("failed to sync before close: %w", err)
	}

	// Close database-level indexes
	if db.indexManager != nil {
		if err := db.indexManager.Close(); err != nil {
			return err
		}
	}

	// Close all table-level index managers
	for _, im := range db.tableIndexManagers {
		if err := im.Close(); err != nil {
			return err
		}
	}

	if useExperimentalRegionIndex() {
		db.lock.Lock()
		if err := syncRegionCatalogHeaderPointersFromFooter(db.file, &db.header); err != nil {
			db.lock.Unlock()
			return fmt.Errorf("failed final region header sync: %w", err)
		}
		if err := db.encodeHeaderLocked(); err != nil {
			db.lock.Unlock()
			return fmt.Errorf("failed final header write: %w", err)
		}
		if err := db.file.Sync(); err != nil {
			db.lock.Unlock()
			return fmt.Errorf("failed final file sync: %w", err)
		}
		db.lock.Unlock()
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

	// Write the index to disk
	if err := db.writeIndexLocked(); err != nil {
		db.lock.Unlock()
		return fmt.Errorf("failed to write index: %w", err)
	}

	// Write the table catalog to disk (must be after index to avoid overwrite)
	if err := db.writeTableCatalogLocked(); err != nil {
		db.lock.Unlock()
		return fmt.Errorf("failed to write table catalog: %w", err)
	}

	if useExperimentalRegionIndex() {
		if err := syncRegionCatalogHeaderPointersFromFooter(db.file, &db.header); err != nil {
			db.lock.Unlock()
			return fmt.Errorf("failed to sync region catalog header pointers: %w", err)
		}
	}

	// Write the header to disk
	if err := db.encodeHeaderLocked(); err != nil {
		db.lock.Unlock()
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Sync the file
	if err := db.file.Sync(); err != nil {
		db.lock.Unlock()
		return err
	}
	db.lock.Unlock()

	return db.maybeAutoVacuum()
}

// RebuildAllIndexes scans the database file to rebuild the primary index mapping
// and all secondary field indexes.
func (db *Database[T]) RebuildAllIndexes() error {
	// 1. Rebuild primary index (record ID -> offset)
	if err := db.RebuildPrimaryIndex(); err != nil {
		return fmt.Errorf("failed to rebuild primary index: %w", err)
	}

	// 2. Rebuild database-level field indexes
	if db.indexManager != nil {
		if err := db.indexManager.RebuildAll(); err != nil {
			return fmt.Errorf("failed to rebuild database field indexes: %w", err)
		}
	}

	// 3. Rebuild table-level field indexes
	// Note: We only rebuild already loaded index managers.
	for _, im := range db.tableIndexManagers {
		if err := im.RebuildAll(); err != nil {
			return fmt.Errorf("failed to rebuild table field indexes: %w", err)
		}
	}

	return nil
}

// SecondaryIndexStoreStats reports embedded secondary-index blob usage.
func (db *Database[T]) SecondaryIndexStoreStats() (*SecondaryIndexStoreStats, error) {
	if db == nil || db.file == nil {
		return nil, fmt.Errorf("database is not open")
	}
	return GetSecondaryIndexStoreStats(db.file.Name())
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

	return db.collectRecordsByIDs(recordIDs), nil
}

func (db *Database[T]) resolveQueryTableIDLocked() uint8 {
	tableID := uint8(0)
	if db.tableCatalog != nil && db.defaultTable != "" {
		if tid, ok := db.tableCatalog.GetTableID(db.defaultTable); ok {
			tableID = tid
		}
	}
	return tableID
}

func (db *Database[T]) collectRecordsByIDs(recordIDs []uint32) []T {
	if len(recordIDs) == 0 {
		return []T{}
	}

	results := make([]T, 0, len(recordIDs))
	var scratch []byte

	db.mlock.Lock()
	if err := db.ensureMmapSizeLocked(); err != nil {
		db.mlock.Unlock()
		return results
	}
	db.mlock.Unlock()

	db.lock.RLock()
	db.mlock.RLock()
	tableID := db.resolveQueryTableIDLocked()
	for _, id := range recordIDs {
		offset, exists := db.getRecordOffset(tableID, id)
		if !exists {
			continue
		}

		recordBytes, err := db.readRecordBytesAtIntoNoLock(offset, tableID, scratch)
		if err != nil {
			continue
		}
		scratch = recordBytes[:0]

		if !isActiveRecord(recordBytes) {
			continue
		}

		record, err := db.decodeRecord(recordBytes)
		if err == nil && record != nil {
			results = append(results, *record)
		}
	}
	db.mlock.RUnlock()
	db.lock.RUnlock()

	return results
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

// Scan iterates over all records, calling the callback for each.
// This performs an unindexed full table scan - use only when no index is available.
// The callback can return false to stop iteration early.
func (db *Database[T]) Scan(fn func(record T) bool) error {
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
		return nil
	}

	ids := make([]uint32, 0, len(idx))
	for id := range idx {
		ids = append(ids, id)
	}
	db.lock.RUnlock()

	// Iterate without holding lock
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

		if !fn(*record) {
			break
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
