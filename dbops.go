package embeddb

import (
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"strings"
)

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

	db := &Database[T]{
		file:         file,
		index:        make(map[uint32]uint32),
		nextRecordID: 1, // Start with ID 1
		layout:       layout,
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
		// Initialize the header for a new file
		db.header = DBHeader{
			Version:       Version,
			nextRecordID:  1,
			nextOffset:    uint32(headerSize), // Start after the header
			indexStart:    0,
			indexEnd:      0,
			indexCapacity: defaultIndexPreallocation,
		}

		// Write the empty header to disk
		if err := db.encodeHeader(); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to write header: %w", err)
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
			// Create a record to check if the schema matches

			// Try to read a record to verify schema compatibility
			for id := range db.index {
				existingRecord, err := db.Get(id)
				if err == nil && existingRecord != nil {
					// Found a valid record, check if migration is needed
					oldLayout, err := ComputeStructLayout(*existingRecord)
					if err != nil {
						db.Close()
						return nil, fmt.Errorf("failed to compute layout of existing record: %w", err)
					}

					// If layouts don't match, migrate
					if oldLayout.Hash != layout.Hash {
						if err := db.Migrate(oldLayout); err != nil {
							db.Close()
							return nil, fmt.Errorf("failed to migrate database: %w", err)
						}
					}
					break
				}
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
	// Begin a transaction to ensure atomicity
	if err := db.beginTransaction(); err != nil {
		return 0, fmt.Errorf("failed to begin insert transaction: %w", err)
	}

	// Use defer to ensure we either commit or rollback
	var newRecordId uint32
	committed := false
	defer func() {
		if !committed {
			// Only rollback if not committed
			if err := db.rollbackTransaction(); err != nil {
				// Just log the error, we can't return it from defer
				fmt.Printf("Error rolling back transaction: %v\n", err)
			}
		}
	}()

	// Lock for thread safety
	db.lock.Lock()
	defer db.lock.Unlock()

	// Get the next record ID
	newRecordId = db.nextId()

	// 1. Encode the record into the binary format.
	encoded, err := db.encodeRecord(record)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}

	// 2. Write the record header (escCode, startMarker, ID, length, active)
	headerBytes := make([]byte, 11)
	headerBytes[0] = escCode
	headerBytes[1] = startMarker

	// Write the ID
	binary.BigEndian.PutUint32(headerBytes[2:6], newRecordId)

	// Write the length
	recordLength := uint32(len(encoded))
	binary.BigEndian.PutUint32(headerBytes[6:10], recordLength)

	// Set active flag
	headerBytes[10] = 1 // 1 = active

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
	db.index[newRecordId] = nextOffset

	// 5. Update the next offset
	db.header.nextOffset += uint32(len(completeRecord))

	// 6. Write the updated index
	if err := db.WriteIndex(); err != nil {
		return 0, fmt.Errorf("failed to update index: %w", err)
	}

	// 7. Update any indexes
	if db.indexManager != nil {
		if err := db.indexManager.InsertIntoIndexes(record, newRecordId); err != nil {
			return 0, fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	// Commit the transaction
	if err := db.commitTransaction(); err != nil {
		return 0, fmt.Errorf("failed to commit insert transaction: %w", err)
	}
	committed = true

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

	// 1. Look up the record offset in the index
	offset, exists := db.index[id]
	if !exists {
		return nil, fmt.Errorf("record with id %d not found", id)
	}

	// 2. Read the record bytes
	recordBytes, err := db.readRecordBytes(offset)
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
	// Begin a transaction to ensure atomicity
	if err := db.beginTransaction(); err != nil {
		return fmt.Errorf("failed to begin delete transaction: %w", err)
	}

	// Use defer to ensure we either commit or rollback
	committed := false
	defer func() {
		if !committed {
			// Only rollback if not committed
			if err := db.rollbackTransaction(); err != nil {
				// Just log the error, we can't return it from defer
				fmt.Printf("Error rolling back transaction: %v\n", err)
			}
		}
	}()

	db.lock.Lock()
	defer db.lock.Unlock()

	// Check if the record exists
	offset, exists := db.index[id]
	if !exists {
		return fmt.Errorf("record with id %d not found", id)
	}

	// Check if the record is already inactive
	recordBytes, err := db.readRecordBytes(offset)
	if err != nil {
		return fmt.Errorf("failed to read record: %w", err)
	}

	if !isActiveRecord(recordBytes) {
		// Record is already deleted
		return nil
	}

	// Get the record for index updates before marking it inactive
	record, err := db.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get record for index updates: %w", err)
	}

	// The active flag is at offset + 10
	inactiveFlag := byte(0)

	// Write the inactive flag
	_, err = db.file.WriteAt([]byte{inactiveFlag}, int64(offset+10))
	if err != nil {
		return fmt.Errorf("failed to mark record as deleted: %w", err)
	}

	// We keep the record in the index so we know it exists but is deleted
	// Alternative approach could be to remove it from the index

	// Write the updated index
	if err := db.WriteIndex(); err != nil {
		return fmt.Errorf("failed to update index: %w", err)
	}

	// Update any indexes
	if db.indexManager != nil && record != nil {
		if err := db.indexManager.RemoveFromIndexes(record, id); err != nil {
			return fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	// Commit the transaction
	if err := db.commitTransaction(); err != nil {
		return fmt.Errorf("failed to commit delete transaction: %w", err)
	}
	committed = true

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
	// Begin a transaction to ensure atomicity
	if err := db.beginTransaction(); err != nil {
		return fmt.Errorf("failed to begin update transaction: %w", err)
	}

	// Use defer to ensure we either commit or rollback
	committed := false
	defer func() {
		if !committed {
			// Only rollback if not committed
			if err := db.rollbackTransaction(); err != nil {
				// Just log the error, we can't return it from defer
				fmt.Printf("Error rolling back transaction: %v\n", err)
			}
		}
	}()

	db.lock.Lock()
	defer db.lock.Unlock()

	// Check if the record exists
	oldOffset, exists := db.index[id]
	if !exists {
		return fmt.Errorf("record with id %d not found", id)
	}

	// Read old record bytes to verify it's active
	oldBytes, err := db.readRecordBytes(oldOffset)
	if err != nil {
		return fmt.Errorf("failed to read old record: %w", err)
	}

	if !isActiveRecord(oldBytes) {
		return fmt.Errorf("cannot update inactive record with id %d", id)
	}

	// Get the old record for index updates
	oldRecord, err := db.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get old record for index updates: %w", err)
	}

	// Mark the old record as inactive (soft delete)
	// This modifies just the active byte at offset+10
	_, err = db.file.WriteAt([]byte{0}, int64(oldOffset+10))
	if err != nil {
		return fmt.Errorf("failed to mark old record as inactive: %w", err)
	}

	// Encode the updated record
	encoded, err := db.encodeRecord(record)
	if err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}

	// Write the record header (escCode, startMarker, ID, length, active)
	headerBytes := make([]byte, 11)
	headerBytes[0] = escCode
	headerBytes[1] = startMarker

	// Write the ID (reuse the same ID)
	binary.BigEndian.PutUint32(headerBytes[2:6], id)

	// Write the length
	recordLength := uint32(len(encoded))
	binary.BigEndian.PutUint32(headerBytes[6:10], recordLength)

	// Set active flag
	headerBytes[10] = 1 // 1 = active

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
	db.index[id] = nextOffset

	// Update the next offset
	db.header.nextOffset += uint32(len(completeRecord))

	// Write the updated index
	if err := db.WriteIndex(); err != nil {
		return fmt.Errorf("failed to update index: %w", err)
	}

	// Update any indexes
	if db.indexManager != nil && oldRecord != nil {
		if err := db.indexManager.UpdateInIndexes(oldRecord, record, id); err != nil {
			return fmt.Errorf("failed to update indexes: %w", err)
		}
	}

	// Commit the transaction
	if err := db.commitTransaction(); err != nil {
		return fmt.Errorf("failed to commit update transaction: %w", err)
	}
	committed = true

	return nil
}

// Compact removes deleted records and reorganizes the database file
// This is a more thorough version of Vacuum that rebuilds the entire file
func (db *Database[T]) Compact() error {
	// Just use our Vacuum implementation
	return db.Vacuum()
}
