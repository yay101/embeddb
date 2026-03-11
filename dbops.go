package embeddb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
)

func cleanupStaleIndexFiles(dbFileName string) error {
	pattern := filepath.Join(filepath.Dir(dbFileName), fmt.Sprintf("%s.*.idx", filepath.Base(dbFileName)))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, m := range matches {
		if err := os.Remove(m); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// TableCatalogEntry represents a single table in the database catalog.
//
// Deprecated: internal use only. This type will be made private in a future release.
type TableCatalogEntry struct {
	Name       string
	LayoutHash string
	NextID     uint32
	Dropped    bool
}

// TableCatalog manages the tables in the database.
//
// Deprecated: internal use only. This type will be made private in a future release.
type TableCatalog struct {
	tables   map[string]*TableCatalogEntry
	tableIDs map[uint8]string // tableID -> table name
	nextID   uint8
	lock     sync.RWMutex
}

// NewTableCatalog creates a table catalog.
//
// Deprecated: internal use only. This function will be made private in a future release.
func NewTableCatalog() *TableCatalog {
	return &TableCatalog{
		tables:   make(map[string]*TableCatalogEntry),
		tableIDs: make(map[uint8]string),
		nextID:   1, // Start at 1 (0 is reserved for legacy format)
	}
}

// AddTable adds a new table to the catalog
func (tc *TableCatalog) AddTable(name string, layoutHash string) uint8 {
	tc.lock.Lock()
	defer tc.lock.Unlock()

	// Check if table already exists
	for id, existingName := range tc.tableIDs {
		if existingName == name {
			return id
		}
	}

	// Assign new table ID
	id := tc.nextID
	tc.nextID++

	tc.tables[name] = &TableCatalogEntry{
		Name:       name,
		LayoutHash: layoutHash,
		NextID:     1,
	}
	tc.tableIDs[id] = name

	return id
}

// GetTableID returns the table ID for a table name
func (tc *TableCatalog) GetTableID(name string) (uint8, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()

	for id, tableName := range tc.tableIDs {
		if tableName == name {
			return id, true
		}
	}
	return 0, false
}

// GetTableName returns the table name for a table ID
func (tc *TableCatalog) GetTableName(id uint8) (string, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()

	name, ok := tc.tableIDs[id]
	return name, ok
}

// GetTable returns the table entry
func (tc *TableCatalog) GetTable(name string) (*TableCatalogEntry, bool) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()

	entry, ok := tc.tables[name]
	return entry, ok
}

// UpdateTableNextID updates the next ID for a table
func (tc *TableCatalog) UpdateTableNextID(name string, nextID uint32) {
	tc.lock.Lock()
	defer tc.lock.Unlock()

	if entry, ok := tc.tables[name]; ok {
		entry.NextID = nextID
	}
}

// DropTable marks a table as dropped (soft delete)
// The table's data will be cleaned up during Vacuum
func (tc *TableCatalog) DropTable(name string) {
	tc.lock.Lock()
	defer tc.lock.Unlock()

	if entry, ok := tc.tables[name]; ok {
		entry.Dropped = true
	}
}

// IsTableDropped checks if a table is marked as dropped
func (tc *TableCatalog) IsTableDropped(name string) bool {
	tc.lock.RLock()
	defer tc.lock.RUnlock()

	if entry, ok := tc.tables[name]; ok {
		return entry.Dropped
	}
	return false
}

// GetNextRecordID gets the next record ID for a table
func (tc *TableCatalog) GetNextRecordID(name string) uint32 {
	tc.lock.RLock()
	defer tc.lock.RUnlock()

	if entry, ok := tc.tables[name]; ok {
		return entry.NextID
	}
	return 1
}

// IncrementNextRecordID increments and returns the next record ID for a table
func (tc *TableCatalog) IncrementNextRecordID(name string) uint32 {
	tc.lock.Lock()
	defer tc.lock.Unlock()

	if entry, ok := tc.tables[name]; ok {
		id := entry.NextID
		entry.NextID++
		return id
	}
	return 1
}

// Count returns the number of tables
func (tc *TableCatalog) Count() int {
	tc.lock.RLock()
	defer tc.lock.RUnlock()

	return len(tc.tables)
}

// encodeTableCatalog encodes the table catalog to bytes
func (tc *TableCatalog) encode() ([]byte, error) {
	tc.lock.RLock()
	defer tc.lock.RUnlock()

	buf := make([]byte, 0, 4+len(tc.tables)*256) // Rough estimate

	// Write table count (including dropped tables so vacuum can clean them)
	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(tc.tables)))
	buf = append(buf, countBuf...)

	// Write each table
	// Format: [tableID:1][nameLen:1][name][hashLen:1][hash][nextID:4][dropped:1]
	for tableID, name := range tc.tableIDs {
		entry := tc.tables[name]

		// Table ID (1 byte)
		buf = append(buf, tableID)

		// Table name length (1 byte) + name
		nameLen := uint8(len(name))
		buf = append(buf, nameLen)
		buf = append(buf, name...)

		// Layout hash length (1 byte) + hash
		hashLen := uint8(len(entry.LayoutHash))
		buf = append(buf, hashLen)
		buf = append(buf, entry.LayoutHash...)

		// Next ID (4 bytes)
		idBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(idBuf, entry.NextID)
		buf = append(buf, idBuf...)

		// Dropped flag (1 byte: 0 = active, 1 = dropped)
		if entry.Dropped {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}

	return buf, nil
}

// decodeTableCatalog decodes the table catalog from bytes
func decodeTableCatalog(data []byte) (*TableCatalog, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("table catalog too short")
	}

	tc := NewTableCatalog()

	count := binary.BigEndian.Uint32(data[:4])
	offset := 4

	for i := uint32(0); i < count; i++ {
		if offset >= len(data) {
			return nil, fmt.Errorf("table catalog truncated at table %d", i)
		}

		// Read table ID (1 byte)
		tableID := uint8(data[offset])
		offset++

		// Read table name
		nameLen := int(data[offset])
		offset++
		if offset+nameLen > len(data) {
			return nil, fmt.Errorf("table name truncated at table %d", i)
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen

		// Read layout hash
		if offset >= len(data) {
			return nil, fmt.Errorf("table hash length missing at table %d", i)
		}
		hashLen := int(data[offset])
		offset++
		if offset+hashLen > len(data) {
			return nil, fmt.Errorf("table hash truncated at table %d", i)
		}
		hash := string(data[offset : offset+hashLen])
		offset += hashLen

		// Read next ID
		if offset+4 > len(data) {
			return nil, fmt.Errorf("table next ID truncated at table %d", i)
		}
		nextID := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		// Read dropped flag (optional - for backward compatibility)
		dropped := false
		if offset < len(data) {
			dropped = data[offset] == 1
			offset++
		}

		// Add to catalog
		entry := &TableCatalogEntry{
			Name:       name,
			LayoutHash: hash,
			NextID:     nextID,
			Dropped:    dropped,
		}
		tc.tables[name] = entry
		tc.tableIDs[tableID] = name

		// Update nextID to ensure we don't reuse IDs
		if tableID >= tc.nextID {
			tc.nextID = tableID + 1
		}
	}

	return tc, nil
}

// writeTableCatalog writes the table catalog to the database file (assumes lock is held)
func (db *Database[T]) writeTableCatalogLocked() error {
	if db.tableCatalog == nil {
		return nil // No tables to write
	}

	data, err := db.tableCatalog.encode()
	if err != nil {
		return fmt.Errorf("failed to encode table catalog: %w", err)
	}

	// Determine where to write the catalog
	// Write at the end of the current data (after records, before index)
	writeOffset := db.header.nextOffset

	// Write catalog with markers: [startMarker][length:4][data][endMarker]
	headerBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(headerBytes, uint32(len(data)))
	catalogData := append([]byte{startMarker}, headerBytes...)
	catalogData = append(catalogData, data...)
	catalogData = append(catalogData, endMarker)

	_, err = db.file.WriteAt(catalogData, int64(writeOffset))
	if err != nil {
		return fmt.Errorf("failed to write table catalog: %w", err)
	}

	// Update header
	db.header.tableCatalogOffset = writeOffset
	db.header.tableCount = uint32(db.tableCatalog.Count())
	db.header.nextOffset = writeOffset + uint32(len(catalogData))

	return nil
}

// writeTableCatalog writes the table catalog to the database file
func (db *Database[T]) writeTableCatalog() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.writeTableCatalogLocked()
}

// readTableCatalog reads the table catalog from the database file
func (db *Database[T]) readTableCatalog() error {
	if db.header.tableCatalogOffset == 0 {
		return nil // No catalog written yet
	}

	db.mlock.RLock()
	defer db.mlock.RUnlock()

	// Read catalog header
	headerBytes := make([]byte, 4)
	_, err := db.mfile.ReadAt(headerBytes, int64(db.header.tableCatalogOffset+1))
	if err != nil {
		return fmt.Errorf("failed to read table catalog header: %w", err)
	}

	length := binary.BigEndian.Uint32(headerBytes)

	// Check end marker position: startMarker(1) + length(4) + data(length) + endMarker(1)
	endPos := db.header.tableCatalogOffset + 6 + length
	if int(endPos) > db.mfile.Len() {
		return fmt.Errorf("table catalog extends beyond file: endPos=%d, fileLen=%d", endPos, db.mfile.Len())
	}

	// Read catalog data
	catalogData := make([]byte, length)
	_, err = db.mfile.ReadAt(catalogData, int64(db.header.tableCatalogOffset+5))
	if err != nil {
		return fmt.Errorf("failed to read table catalog data: %w", err)
	}

	// Decode
	db.tableCatalog, err = decodeTableCatalog(catalogData)
	if err != nil {
		return fmt.Errorf("failed to decode table catalog: %w", err)
	}

	return nil
}

// New creates a new database for storing records of type T.
// If autoIndex is true, fields with the "db:index" or "db:unique" tag will be automatically indexed
func New[T any](filename string, migrate bool, autoIndex bool) (*Database[T], error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Create an instance of T to analyze its structure
	var instance T

	// Compute struct layout using unsafe pointers for field offsets
	layout, err := ComputeStructLayout(instance)
	if err != nil {
		return nil, fmt.Errorf("failed to compute struct layout: %w", err)
	}

	// Derive default table name from type
	defaultTableName := reflect.TypeOf(instance).Name()

	db := &Database[T]{
		file:              file,
		indexes:           make(map[uint8]map[uint32]uint32),
		nextRecordID:      1, // Start with ID 1
		layout:            layout,
		defaultTable:      defaultTableName,
		autoVacuumEnabled: true,
		lastVacuumTime:    time.Now(),
	}

	// Create the index manager
	db.indexManager = NewIndexManager(db, layout)

	// Check if the file is new or existing
	fileInfo, err := file.Stat()
	if err != nil {
		db.Close() // Close file on error.
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	// For a new file, initialize the header and index
	if fileInfo.Size() == 0 {
		if err := cleanupStaleIndexFiles(file.Name()); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to clean stale index files: %w", err)
		}

		// Initialize the header for a new file
		db.header = DBHeader{
			Version:            Version,
			nextRecordID:       1,
			nextOffset:         uint32(headerSize), // Start after the header
			indexStart:         0,
			indexEnd:           0,
			indexCapacity:      defaultIndexPreallocation,
			tableCatalogOffset: 0,
			tableCount:         0,
		}

		// Initialize table catalog for the default table
		db.tableCatalog = NewTableCatalog()
		db.tableCatalog.AddTable(defaultTableName, layout.Hash)

		// Write the empty header to disk
		if err := db.encodeHeader(); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to write header: %w", err)
		}

		// Write the table catalog to disk
		if err := db.writeTableCatalog(); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to write table catalog: %w", err)
		}

		// Auto-index fields if requested (for new files)
		if autoIndex {
			if err := db.autoIndexFields(); err != nil {
				fmt.Printf("Warning: failed to auto-index fields: %v\n", err)
			}
		}
	} else {
		// Read the existing header
		if err := db.decodeHeader(); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to decode header: %w", err)
		}

		// Read the existing index
		if err := db.ReadIndex(); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to read index: %w", err)
		}

		// Read the existing table catalog
		if err := db.readTableCatalog(); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to read table catalog: %w", err)
		}

		// No legacy schema initialization needed

		// Try to load any existing indexes
		if err := db.indexManager.CheckIndexes(); err != nil {
			// Just log the error, don't fail
			fmt.Printf("Warning: failed to check indexes: %v\n", err)
		}

		// Try to extract indexes from the database file
		if err := db.indexManager.ExtractIndexesFromDatabase(); err != nil {
			// Just log the error, don't fail
			fmt.Printf("Warning: failed to extract indexes: %v\n", err)
		}

		// Check if we need to migrate the database
		if migrate {
			// Try to read a record from any table to verify schema compatibility
			for tableID, idx := range db.indexes {
				if len(idx) == 0 {
					continue
				}
				for id := range idx {
					offset, _ := db.getRecordOffset(tableID, id)
					recordBytes, err := db.readRecordBytesAt(offset, tableID)
					if err != nil || !isActiveRecord(recordBytes) {
						continue
					}
					existingRecord, err := db.decodeRecord(recordBytes)
					if err == nil && existingRecord != nil {
						oldLayout, err := ComputeStructLayout(*existingRecord)
						if err != nil {
							db.Close()
							return nil, fmt.Errorf("failed to compute layout of existing record: %w", err)
						}
						if oldLayout.Hash != layout.Hash {
							if err := db.Migrate(oldLayout); err != nil {
								db.Close()
								return nil, fmt.Errorf("failed to migrate database: %w", err)
							}
						}
						break
					}
				}
				break // Only check first table
			}
		}

		// Build any pending indexes
		if err := db.indexManager.BuildPendingIndexes(); err != nil {
			// Just log the error, don't fail
			fmt.Printf("Warning: failed to build pending indexes: %v\n", err)
		}

		// Auto-index fields if requested
		if autoIndex {
			if err := db.autoIndexFields(); err != nil {
				fmt.Printf("Warning: failed to auto-index fields: %v\n", err)
			}
		}
	}

	return db, nil
}

// Close function is now defined in main.go

// Open opens an existing database file and returns a non-generic DB handle.
// This is useful when you want to work with multiple tables of different types.
// Use Table[T](db) or Table[T](db, "name") to get typed table access.
func Open(filename string) (*DB, error) {
	db, err := New[any](filename, false, false)
	if err != nil {
		return nil, err
	}
	return &DB{Database: db}, nil
}

// Schema initialization is now handled via the StructLayout using unsafe

// autoIndexFields creates indexes for fields with the db:index or db:unique tag
func (db *Database[T]) autoIndexFields() error {
	// Get the type of T
	t := reflect.TypeOf((*T)(nil)).Elem()

	// Function to recursively check fields
	var checkFields func(t reflect.Type, prefix string) error
	checkFields = func(t reflect.Type, prefix string) error {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)

			// Skip unexported fields
			if !field.IsExported() {
				continue
			}

			// Build the full field name
			fieldName := field.Name
			if prefix != "" {
				fieldName = prefix + "." + fieldName
			}

			// Check for index tags
			tag := field.Tag.Get("db")
			if tag == "index" || tag == "unique" || strings.Contains(tag, "index") {
				// Create an index for this field
				if err := db.CreateIndex(fieldName); err != nil {
					return fmt.Errorf("failed to create index for field %s: %w", fieldName, err)
				}
			}

			// Recursively check embedded structs
			if field.Type.Kind() == reflect.Struct {
				if err := checkFields(field.Type, fieldName); err != nil {
					return err
				}
			}
		}
		return nil
	}

	return checkFields(t, "")
}

// Insert inserts a new record into the database.
func (db *Database[T]) Insert(record *T) (uint32, error) {
	return db.InsertToTable(record, db.defaultTable)
}

// InsertToTable inserts a new record into a specific table
func (db *Database[T]) InsertToTable(record *T, tableName string) (uint32, error) {
	// Serialize write operations
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	// Get table ID and next record ID
	tableID := uint8(0)
	var newRecordId uint32
	if db.tableCatalog != nil && tableName != "" {
		var ok bool
		tableID, ok = db.tableCatalog.GetTableID(tableName)
		if !ok {
			return 0, fmt.Errorf("table '%s' not found", tableName)
		}
		newRecordId = db.tableCatalog.IncrementNextRecordID(tableName)
	} else {
		// Legacy single-table mode
		newRecordId = db.nextId()
	}

	// 1. Encode the record into the binary format (no lock needed).
	encoded, err := db.encodeRecord(record)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}

	// Lock for the critical section of writing to file and updating index
	db.lock.Lock()

	// 2. Write the record header (escCode, startMarker, tableID, ID, length, active)
	// New format: 12 bytes header instead of 11
	headerBytes := make([]byte, 12)
	headerBytes[0] = escCode
	headerBytes[1] = startMarker
	headerBytes[2] = tableID

	// Write the ID
	binary.BigEndian.PutUint32(headerBytes[3:7], newRecordId)

	// Write the length
	recordLength := uint32(len(encoded))
	binary.BigEndian.PutUint32(headerBytes[7:11], recordLength)

	// Set active flag
	headerBytes[11] = 1 // 1 = active

	// Add end marker
	footerBytes := []byte{escCode, endMarker}

	// Combine header, encoded data, and footer
	completeRecord := make([]byte, 0, len(headerBytes)+len(encoded)+len(footerBytes))
	completeRecord = append(completeRecord, headerBytes...)
	completeRecord = append(completeRecord, encoded...)
	completeRecord = append(completeRecord, footerBytes...)

	// 3. Write the data to the file at the next available offset
	nextOffset := db.header.nextOffset
	if err := db.writeData(completeRecord, int64(nextOffset)); err != nil {
		return 0, fmt.Errorf("failed to write record: %w", err)
	}

	// 4. Update the index with the new record
	db.setRecordOffset(tableID, newRecordId, nextOffset)

	// 5. Update the next offset
	db.header.nextOffset += uint32(len(completeRecord))

	// 6. Index is kept in memory - will be persisted on Close() or Sync()
	// This avoids O(n^2) file growth from writing the full index on every insert

	// 7. Update any indexes (table-aware)
	if db.indexManager != nil {
		if err := db.indexManager.InsertIntoIndexes(record, newRecordId); err != nil {
			db.lock.Unlock()
			return 0, fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	// Release lock
	db.lock.Unlock()
	db.markMutation()

	return newRecordId, nil
}

func (db *Database[T]) encodeRecord(record *T) ([]byte, error) {
	// Use the new layout-based encoding
	return db.encodeRecordWithLayout(record, db.layout)
}

// encodeEmbeddedStruct handles the encoding of an embedded struct.
// This method is no longer needed and is only kept for API compatibility
func (db *Database[T]) encodeEmbeddedStruct(fieldValue reflect.Value) ([]byte, error) {
	// This method is no longer needed since we use StructLayout for all operations
	return nil, fmt.Errorf("embedded struct encoding is now handled through StructLayout")
}

// Get retrieves a record from the database by its ID.
// This uses the index for fast lookup and the layout for proper decoding
func (db *Database[T]) Get(id uint32) (*T, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.getLocked(id)
}

// getLocked retrieves a record without acquiring locks.
// IMPORTANT: Caller must hold db.lock (read or write) before calling this method.
func (db *Database[T]) getLocked(id uint32) (*T, error) {
	// Prefer the default table in multi-table mode.
	if db.tableCatalog != nil && db.defaultTable != "" {
		if tableID, ok := db.tableCatalog.GetTableID(db.defaultTable); ok {
			return db.getLockedForTable(id, tableID)
		}
	}

	// Legacy single-table fallback.
	return db.getLockedForTable(id, 0)
}

// getLockedForTable retrieves a record from a specific table without acquiring locks.
func (db *Database[T]) getLockedForTable(id uint32, tableID uint8) (*T, error) {
	// 1. Look up the record offset in the table's index
	offset, exists := db.getRecordOffset(tableID, id)
	if !exists {
		return nil, fmt.Errorf("record with id %d not found", id)
	}

	// 2. Read the record bytes with table context
	recordBytes, err := db.readRecordBytesAt(offset, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to read record bytes: %w", err)
	}

	// 3. Check if the record is active
	if !isActiveRecord(recordBytes) {
		return nil, fmt.Errorf("record with id %d is not active", id)
	}

	// 4. Decode the data into a struct of type T
	record, err := db.decodeRecord(recordBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode record: %w", err)
	}

	return record, nil
}

// These backward compatibility methods have been removed

// Delete marks a record as inactive in the database
func (db *Database[T]) Delete(id uint32) error {
	// For backward compatibility, use table 0
	return db.DeleteFromTable(id, 0)
}

// DeleteFromTable marks a record as inactive in a specific table
func (db *Database[T]) DeleteFromTable(id uint32, tableID uint8) error {
	// Serialize write operations
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	db.lock.Lock()

	// Check if the record exists
	offset, exists := db.getRecordOffset(tableID, id)
	if !exists {
		db.lock.Unlock()
		return fmt.Errorf("record with id %d not found", id)
	}

	// Check if the record is already inactive
	recordBytes, err := db.readRecordBytesAt(offset, tableID)
	if err != nil {
		return fmt.Errorf("failed to read record: %w", err)
	}

	if !isActiveRecord(recordBytes) {
		// Record is already deleted
		db.lock.Unlock()
		return nil
	}

	// Get the record for index updates before marking it inactive
	// Use getLocked since we already hold the lock
	record, err := db.getLocked(id)
	if err != nil {
		db.lock.Unlock()
		return fmt.Errorf("failed to get record for index updates: %w", err)
	}

	// The active flag is at offset + 11 (new format with tableID)
	inactiveFlag := byte(0)

	// Write the inactive flag
	_, err = db.file.WriteAt([]byte{inactiveFlag}, int64(offset+11))
	if err != nil {
		db.lock.Unlock()
		return fmt.Errorf("failed to mark record as deleted: %w", err)
	}

	// We keep the record in the index so we know it exists but is deleted
	// Alternative approach could be to remove it from the index

	// Write the updated index
	if err := db.writeIndexLocked(); err != nil {
		db.lock.Unlock()
		return fmt.Errorf("failed to update index: %w", err)
	}

	// Update any indexes
	if db.indexManager != nil && record != nil {
		if err := db.indexManager.RemoveFromIndexes(record, id); err != nil {
			db.lock.Unlock()
			return fmt.Errorf("failed to update indexes: %w", err)
		}
	}
	db.lock.Unlock()
	db.markMutation()

	return nil
}

// The readEncodedData function has been replaced by readRecordBytes

// decodeRecord decodes the binary data into a struct of type T.
func (db *Database[T]) decodeRecord(data []byte) (*T, error) {
	// Use the new layout-based decoding
	return db.decodeRecordWithLayout(data, db.layout)
}

// The decodeEmbeddedStruct method has been removed in favor of the layout-based approach

// Update updates an existing record in the database
// This implementation uses atomic operations to ensure data integrity
func (db *Database[T]) Update(id uint32, record *T) error {
	// For backward compatibility, use table 0
	return db.UpdateInTable(id, record, 0)
}

// UpdateInTable updates an existing record in a specific table
func (db *Database[T]) UpdateInTable(id uint32, record *T, tableID uint8) error {
	// Serialize write operations
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	db.lock.Lock()

	// Check if the record exists
	oldOffset, exists := db.getRecordOffset(tableID, id)
	if !exists {
		db.lock.Unlock()
		return fmt.Errorf("record with id %d not found", id)
	}

	// Read old record bytes to verify it's active
	oldBytes, err := db.readRecordBytesAt(oldOffset, tableID)
	if err != nil {
		return fmt.Errorf("failed to read old record: %w", err)
	}

	if !isActiveRecord(oldBytes) {
		db.lock.Unlock()
		return fmt.Errorf("cannot update inactive record with id %d", id)
	}

	// Get the old record for index updates
	oldRecord, err := db.getLockedForTable(id, tableID)
	if err != nil {
		db.lock.Unlock()
		return fmt.Errorf("failed to get old record for index updates: %w", err)
	}

	// Mark the old record as inactive (soft delete)
	// This modifies just the active byte at offset+11 (new format with tableID)
	_, err = db.file.WriteAt([]byte{0}, int64(oldOffset+11))
	if err != nil {
		db.lock.Unlock()
		return fmt.Errorf("failed to mark old record as inactive: %w", err)
	}

	// Encode the updated record
	encoded, err := db.encodeRecord(record)
	if err != nil {
		db.lock.Unlock()
		return fmt.Errorf("failed to encode record: %w", err)
	}

	// Write the record header (escCode, startMarker, tableID, ID, length, active)
	// New format: 12 bytes header
	headerBytes := make([]byte, 12)
	headerBytes[0] = escCode
	headerBytes[1] = startMarker
	headerBytes[2] = tableID

	// Write the ID (reuse the same ID)
	binary.BigEndian.PutUint32(headerBytes[3:7], id)

	// Write the length
	recordLength := uint32(len(encoded))
	binary.BigEndian.PutUint32(headerBytes[7:11], recordLength)

	// Set active flag
	headerBytes[11] = 1 // 1 = active

	// Add end marker
	footerBytes := []byte{escCode, endMarker}

	// Combine header, encoded data, and footer
	completeRecord := make([]byte, 0, len(headerBytes)+len(encoded)+len(footerBytes))
	completeRecord = append(completeRecord, headerBytes...)
	completeRecord = append(completeRecord, encoded...)
	completeRecord = append(completeRecord, footerBytes...)

	// Write the updated record at the next available offset
	nextOffset := db.header.nextOffset
	if err := db.writeData(completeRecord, int64(nextOffset)); err != nil {
		return fmt.Errorf("failed to write updated record: %w", err)
	}

	// Update the index with the new record location
	db.setRecordOffset(tableID, id, nextOffset)

	// Update the next offset
	db.header.nextOffset += uint32(len(completeRecord))

	// Update any indexes
	if db.indexManager != nil && oldRecord != nil {
		if err := db.indexManager.UpdateInIndexes(oldRecord, record, id); err != nil {
			db.lock.Unlock()
			return fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	// Release lock
	db.lock.Unlock()
	db.markMutation()

	return nil
}

// Compact removes deleted records and reorganizes the database file
// This is a more thorough version of Vacuum that rebuilds the entire file
func (db *Database[T]) Compact() error {
	// Just use our Vacuum implementation
	return db.Vacuum()
}
