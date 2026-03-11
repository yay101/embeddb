package embeddb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"time"
	"unsafe"
)

// MigrationOptions contains options for schema migration
type MigrationOptions struct {
	// BatchSize is the number of records to migrate in a single transaction
	BatchSize int
	// Logger is an optional function for logging migration progress
	Logger func(message string)
}

// DefaultMigrationOptions returns the default migration options
func DefaultMigrationOptions() MigrationOptions {
	return MigrationOptions{
		BatchSize: 100,
		Logger:    nil,
	}
}

// Migrate performs a schema migration when the struct layout has changed
// It reads all records using the old layout and writes them back using the new layout
// This operation is atomic - either all records are migrated or none
func (db *Database[T]) Migrate(oldLayout *StructLayout) error {
	return db.MigrateWithOptions(oldLayout, DefaultMigrationOptions())
}

// MigrateWithOptions performs a schema migration with custom options
func (db *Database[T]) MigrateWithOptions(oldLayout *StructLayout, opts MigrationOptions) error {
	if db.layout.Hash == oldLayout.Hash {
		// No migration needed if the layouts are identical
		return nil
	}

	logMessage := func(msg string) {
		if opts.Logger != nil {
			opts.Logger(msg)
		}
	}

	logMessage("Starting database migration")

	// Create a temporary map for the migrated records
	migratedRecords := make(map[uint32]uint32) // id -> new offset

	// Use a transaction to ensure atomicity
	if err := db.beginTransaction(); err != nil {
		return fmt.Errorf("failed to begin migration transaction: %w", err)
	}

	// We need to defer either a commit or rollback
	var migrationSuccessful bool = false
	defer func() {
		if migrationSuccessful {
			if err := db.commitTransaction(); err != nil {
				logMessage(fmt.Sprintf("Error committing migration transaction: %v", err))
			}
		} else {
			if err := db.rollbackTransaction(); err != nil {
				logMessage(fmt.Sprintf("Error rolling back migration transaction: %v", err))
			}
		}
	}()

	// Create a temporary backup of the index
	oldIndex := make(map[uint32]uint32)
	for k, v := range db.index {
		oldIndex[k] = v
	}

	// Get all record IDs
	var recordIDs []uint32
	for id := range db.index {
		recordIDs = append(recordIDs, id)
	}

	logMessage(fmt.Sprintf("Found %d records to migrate", len(recordIDs)))

	// Process records in batches
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultMigrationOptions().BatchSize
	}

	// Track the total migrated records for logging
	totalMigrated := 0

	// Create a mutex to protect the batch operations
	var batchMutex sync.Mutex

	for i := 0; i < len(recordIDs); i += batchSize {
		end := i + batchSize
		if end > len(recordIDs) {
			end = len(recordIDs)
		}

		batch := recordIDs[i:end]
		logMessage(fmt.Sprintf("Migrating batch %d/%d (records %d-%d)",
			(i/batchSize)+1, (len(recordIDs)+batchSize-1)/batchSize, i+1, end))

		// Process each record in the batch
		for _, id := range batch {
			// Get the old record bytes at the old offset
			oldOffset := db.index[id]

			// Read the record bytes
			recordBytes, err := db.readRecordBytes(oldOffset)
			if err != nil {
				return fmt.Errorf("failed to read record %d during migration: %w", id, err)
			}

			// Skip deleted/inactive records
			if !isActiveRecord(recordBytes) {
				migratedRecords[id] = oldOffset // Keep the same offset for inactive records
				continue
			}

			// Decode the record using the old layout
			record, err := db.decodeRecordWithLayout(recordBytes, oldLayout)
			if err != nil {
				return fmt.Errorf("failed to decode record %d during migration: %w", id, err)
			}

			// Encode the record using the new layout
			newBytes, err := db.encodeRecordWithLayout(record, db.layout)
			if err != nil {
				return fmt.Errorf("failed to encode record %d during migration: %w", id, err)
			}

			// Write the record with the new layout
			newOffset, err := db.writeRecordBytes(id, newBytes)
			if err != nil {
				return fmt.Errorf("failed to write migrated record %d: %w", id, err)
			}

			// Update our migration tracking
			batchMutex.Lock()
			migratedRecords[id] = newOffset
			totalMigrated++
			batchMutex.Unlock()
		}

		logMessage(fmt.Sprintf("Migrated %d records so far", totalMigrated))
	}

	// Update the index with the migrated offsets
	db.lock.Lock()
	for id, offset := range migratedRecords {
		db.index[id] = offset
	}
	db.lock.Unlock()

	// Write the updated index
	if err := db.WriteIndex(); err != nil {
		return fmt.Errorf("failed to write updated index after migration: %w", err)
	}

	logMessage("Migration completed successfully")
	migrationSuccessful = true
	return nil
}

// readRecordBytes reads the raw bytes of a record at the given offset
func (db *Database[T]) readRecordBytes(offset uint32) ([]byte, error) {
	return db.readRecordBytesAt(offset, 0)
}

// readRecordBytesAt reads the raw bytes of a record at the given offset with optional tableID
func (db *Database[T]) readRecordBytesAt(offset uint32, expectedTableID uint8) ([]byte, error) {
	// Check if mmap needs to be loaded before acquiring read lock
	// (ReloadMMap acquires write lock, so we can't call it while holding read lock)
	if db.mfile == nil {
		if err := db.ReloadMMap(); err != nil {
			return nil, err
		}
	}

	db.mlock.RLock()
	defer db.mlock.RUnlock()

	// First, determine the length of the record
	// Read the length bytes (4 bytes at offset+7 for new format)
	// We check the format by looking at the first byte after markers
	lengthOffset := int64(offset + 7) // new format: offset+2(tableID) + offset+3-6(id) = offset+7 for length
	lengthBytes := make([]byte, 4)
	_, err := db.mfile.ReadAt(lengthBytes, lengthOffset)
	if err != nil {
		return nil, err
	}

	// Parse the record length
	recordLength := binary.BigEndian.Uint32(lengthBytes)

	// Read the entire record including the header and markers
	// Header is 12 bytes: [escCode,startMarker](2) + tableID(1) + id(4) + length(4) + active(1)
	// The footer is 2 bytes: [escCode,endMarker]
	totalLength := 12 + recordLength + 2

	recordBytes := make([]byte, totalLength)
	_, err = db.mfile.ReadAt(recordBytes, int64(offset))
	if err != nil {
		return nil, err
	}

	// Validate the record starts and ends correctly
	if recordBytes[0] != escCode || recordBytes[1] != startMarker {
		return nil, errors.New("invalid record: missing start marker")
	}

	if recordBytes[totalLength-2] != escCode || recordBytes[totalLength-1] != endMarker {
		return nil, errors.New("invalid record: missing end marker")
	}

	// Verify tableID if specified
	if expectedTableID != 0 && recordBytes[2] != expectedTableID {
		return nil, fmt.Errorf("record table mismatch: expected %d, got %d", expectedTableID, recordBytes[2])
	}

	return recordBytes, nil
}

// writeRecordBytes writes the raw bytes of a record and returns the new offset
func (db *Database[T]) writeRecordBytes(id uint32, recordBytes []byte) (uint32, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Determine the next available offset for writing
	nextOffset := db.header.nextOffset

	// Write the record bytes to the file
	_, err := db.file.WriteAt(recordBytes, int64(nextOffset))
	if err != nil {
		return 0, err
	}

	// Update the next offset
	db.header.nextOffset += uint32(len(recordBytes))

	return nextOffset, nil
}

// isActiveRecord checks if a record is active by examining its active byte
func isActiveRecord(recordBytes []byte) bool {
	// The active byte is at offset 11 in the record header (after escCode, startMarker, tableID, id)
	return len(recordBytes) > 11 && recordBytes[11] == 1
}

// decodeRecordWithLayout decodes a record using the specified struct layout
func (db *Database[T]) decodeRecordWithLayout(data []byte, layout *StructLayout) (*T, error) {
	// Create a new instance of T
	record := new(T)

	// Extract the ID from the header before skipping it
	// Header: [escCode,startMarker](2) + tableID(1) + id(4) + length(4) + active(1)
	recordID := binary.BigEndian.Uint32(data[3:7])

	// If there's a primary key field, set the ID
	if layout.PrimaryKey < 255 {
		if pkField, exists := layout.FieldOffsets[layout.PrimaryKey]; exists {
			// Set the primary key field value
			// Pass as uint32 directly since that's what we read from the header
			if err := SetFieldValue(record, pkField, uint32(recordID)); err != nil {
				return nil, fmt.Errorf("failed to set primary key field: %w", err)
			}
		}
	}

	// Skip the header (first 12 bytes: escCode + startMarker + tableID + id + length + active)
	data = data[12:]

	// Remove the end marker (last 2 bytes)
	data = data[:len(data)-2]

	// Process each field in the record
	for len(data) > 0 {
		// Get the field key (byte identifier)
		if len(data) < 2 {
			return nil, errors.New("invalid record format: truncated field data")
		}

		fieldKey := data[0]
		if data[1] != valueStartMarker {
			return nil, fmt.Errorf("invalid record format: missing value start marker for field %d", fieldKey)
		}

		// Skip the field key and start marker
		data = data[2:]

		// Get the field offset information
		fieldOffset, exists := layout.FieldOffsets[fieldKey]
		if !exists {
			// Skip unknown fields (might be from a newer schema)
			// Find the value end marker
			endIdx := bytes.IndexByte(data, valueEndMarker)
			if endIdx == -1 {
				return nil, errors.New("invalid record format: missing value end marker")
			}
			data = data[endIdx+1:]
			continue
		}

		// Skip primary key field - it's already set from the record header
		if fieldOffset.Primary {
			// Find the value end marker and skip
			endIdx := bytes.IndexByte(data, valueEndMarker)
			if endIdx == -1 {
				return nil, errors.New("invalid record format: missing value end marker for primary key")
			}
			data = data[endIdx+1:]
			continue
		}

		// Decode the field value based on its type
		var value interface{}
		var err error

		switch fieldOffset.Type {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			value, data, err = decodeVarint(data)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			value, data, err = decodeUvarint(data)
		case reflect.Float32, reflect.Float64:
			bits, remaining, decodeErr := decodeUvarint(data)
			if decodeErr != nil {
				err = decodeErr
				break
			}
			value = math.Float64frombits(bits.(uint64))
			data = remaining
		case reflect.String:
			value, data, err = decodeString(data)
		case reflect.Bool:
			value, data, err = decodeBool(data)
		case reflect.Struct:
			// For embedded structs, we need to handle them differently
			// Check if this is a time.Time field
			if fieldOffset.IsTime {
				// time.Time is stored as int64 (Unix nanoseconds)
				unixNanoVal, remaining, decodeErr := decodeVarint(data)
				if decodeErr != nil {
					err = decodeErr
					break
				}
				unixNano, ok := unixNanoVal.(int64)
				if !ok {
					err = fmt.Errorf("invalid time value: expected int64")
					break
				}
				value = time.Unix(0, unixNano)
				data = remaining
			} else if fieldOffset.IsStruct {
				// Read the length of the embedded struct
				length, remaining, decodeErr := decodeUvarint(data)
				if decodeErr != nil {
					err = decodeErr
					break
				}

				// Extract the embedded struct data
				if len(remaining) < int(length.(uint64)) {
					err = errors.New("invalid record: embedded struct data too short")
					break
				}

				// Skip processing the embedded struct for now
				// In a real implementation, you'd recursively decode the struct
				data = remaining[int(length.(uint64)):]

				// Skip the value end marker
				if len(data) > 0 && data[0] == valueEndMarker {
					data = data[1:]
				} else {
					err = errors.New("invalid record: missing value end marker after embedded struct")
				}
				continue
			} else {
				err = fmt.Errorf("field %s is marked as struct but IsStruct is false", fieldOffset.Name)
				break
			}
		case reflect.Slice:
			lengthVal, remaining, decodeErr := decodeUvarint(data)
			if decodeErr != nil {
				err = decodeErr
				break
			}
			length := int(lengthVal.(uint64))

			elemType := fieldOffset.SliceElem
			slice := make([]interface{}, length)

			for i := 0; i < length; i++ {
				var elem interface{}
				switch elemType.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					elem, remaining, err = decodeVarint(remaining)
					if err != nil {
						break
					}
					intVal := elem.(int64)
					switch elemType.Kind() {
					case reflect.Int:
						slice[i] = int(intVal)
					case reflect.Int8:
						slice[i] = int8(intVal)
					case reflect.Int16:
						slice[i] = int16(intVal)
					case reflect.Int32:
						slice[i] = int32(intVal)
					case reflect.Int64:
						slice[i] = intVal
					}
					continue
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					elem, remaining, err = decodeUvarint(remaining)
					if err != nil {
						break
					}
					uintVal := elem.(uint64)
					switch elemType.Kind() {
					case reflect.Uint:
						slice[i] = uint(uintVal)
					case reflect.Uint8:
						slice[i] = uint8(uintVal)
					case reflect.Uint16:
						slice[i] = uint16(uintVal)
					case reflect.Uint32:
						slice[i] = uint32(uintVal)
					case reflect.Uint64:
						slice[i] = uintVal
					}
					continue
				case reflect.Float32, reflect.Float64:
					elem, remaining, decodeErr = decodeUvarint(remaining)
					if decodeErr != nil {
						err = decodeErr
						break
					}
					floatVal := math.Float64frombits(elem.(uint64))
					if elemType.Kind() == reflect.Float32 {
						slice[i] = float32(floatVal)
					} else {
						slice[i] = floatVal
					}
					continue
				case reflect.String:
					elem, remaining, err = decodeString(remaining)
					if err != nil {
						break
					}
					slice[i] = elem.(string)
					continue
				case reflect.Bool:
					elem, remaining, err = decodeBool(remaining)
					if err != nil {
						break
					}
					slice[i] = elem.(bool)
					continue
				default:
					err = fmt.Errorf("unsupported slice element type: %v", elemType.Kind())
					break
				}
			}
			value = slice
			data = remaining
		default:
			err = fmt.Errorf("unsupported field type: %v", fieldOffset.Type)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to decode field %s: %w", fieldOffset.Name, err)
		}

		// Set the field value using the unsafe pointer
		// For time.Time fields (which are structs with IsTime=true), we need to set the value too
		if fieldOffset.Type != reflect.Struct || fieldOffset.IsTime {
			err = SetFieldValue(record, fieldOffset, value)
			if err != nil {
				return nil, fmt.Errorf("failed to set field %s: %w", fieldOffset.Name, err)
			}
		}

		// Skip the value end marker
		if len(data) > 0 && data[0] == valueEndMarker {
			data = data[1:]
		} else {
			return nil, fmt.Errorf("invalid record: missing value end marker for field %s", fieldOffset.Name)
		}
	}

	return record, nil
}

// encodeRecordWithLayout encodes a record using the specified struct layout
// This only encodes the field data, NOT the record wrapper (header/footer).
// The caller (Insert/Update) is responsible for adding the record header and footer.
func (db *Database[T]) encodeRecordWithLayout(record *T, layout *StructLayout) ([]byte, error) {
	var buffer []byte

	// Get sorted keys for deterministic field order
	keys := make([]byte, 0, len(layout.FieldOffsets))
	for key := range layout.FieldOffsets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// Encode each field in sorted key order
	for _, key := range keys {
		fieldOffset := layout.FieldOffsets[key]
		// Skip struct container fields - we only encode the leaf fields
		// The nested struct's fields are already expanded in the layout
		// Exception: time.Time fields need to be encoded
		if fieldOffset.IsStruct && !fieldOffset.IsTime {
			continue
		}

		// Skip primary key field - it's stored in the record header, not the data
		if fieldOffset.Primary {
			continue
		}

		// Add the field key and start marker
		buffer = append(buffer, key, valueStartMarker)

		// Get the field value using the unsafe pointer
		value, err := GetFieldValue(record, fieldOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to get field %s: %w", fieldOffset.Name, err)
		}

		// Encode the value based on the field type
		switch fieldOffset.Type {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			// Convert to int64 regardless of original int type
			intValue := reflect.ValueOf(value).Int()
			buffer = encodeVarint(buffer, intValue)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// Convert to uint64 regardless of original uint type
			uintValue := reflect.ValueOf(value).Uint()
			buffer = encodeUvarint(buffer, uintValue)
		case reflect.Float32, reflect.Float64:
			// Always store as float64 for simplicity
			floatValue := reflect.ValueOf(value).Float()
			bits := math.Float64bits(floatValue)
			buffer = encodeUvarint(buffer, bits)
		case reflect.String:
			strValue, ok := value.(string)
			if !ok {
				strValue = ""
			}
			buffer = encodeString(buffer, strValue)
		case reflect.Bool:
			boolValue, ok := value.(bool)
			if !ok {
				boolValue = false
			}
			buffer = encodeBool(buffer, boolValue)
		case reflect.Struct:
			// Check if this is a time.Time field
			if fieldOffset.IsTime {
				timeValue, ok := value.(time.Time)
				if !ok {
					return nil, fmt.Errorf("field %s is marked as time.Time but value is not time.Time", fieldOffset.Name)
				}
				// Store as int64 (Unix nanoseconds)
				buffer = encodeVarint(buffer, timeValue.UnixNano())
			} else {
				return nil, fmt.Errorf("unsupported struct field type: %s", fieldOffset.Name)
			}
		case reflect.Slice:
			slicePtr := value.(unsafe.Pointer)
			sliceHeader := (*reflect.SliceHeader)(slicePtr)
			length := sliceHeader.Len
			buffer = encodeUvarint(buffer, uint64(length))
			if length == 0 || sliceHeader.Data == 0 {
				break
			}

			elemType := fieldOffset.SliceElem
			dataPtr := sliceHeader.Data
			elemSize := elemType.Size()

			for i := 0; i < length; i++ {
				elemPtr := unsafe.Pointer(dataPtr + uintptr(i)*elemSize)
				switch elemType.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					var intVal int64
					switch elemType.Kind() {
					case reflect.Int:
						intVal = int64(*(*int)(elemPtr))
					case reflect.Int8:
						intVal = int64(*(*int8)(elemPtr))
					case reflect.Int16:
						intVal = int64(*(*int16)(elemPtr))
					case reflect.Int32:
						intVal = int64(*(*int32)(elemPtr))
					case reflect.Int64:
						intVal = *(*int64)(elemPtr)
					}
					buffer = encodeVarint(buffer, intVal)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					var uintVal uint64
					switch elemType.Kind() {
					case reflect.Uint:
						uintVal = uint64(*(*uint)(elemPtr))
					case reflect.Uint8:
						uintVal = uint64(*(*uint8)(elemPtr))
					case reflect.Uint16:
						uintVal = uint64(*(*uint16)(elemPtr))
					case reflect.Uint32:
						uintVal = uint64(*(*uint32)(elemPtr))
					case reflect.Uint64:
						uintVal = *(*uint64)(elemPtr)
					}
					buffer = encodeUvarint(buffer, uintVal)
				case reflect.Float32, reflect.Float64:
					var floatVal float64
					if elemType.Kind() == reflect.Float32 {
						floatVal = float64(*(*float32)(elemPtr))
					} else {
						floatVal = *(*float64)(elemPtr)
					}
					bits := math.Float64bits(floatVal)
					buffer = encodeUvarint(buffer, bits)
				case reflect.String:
					strHdr := (*reflect.StringHeader)(elemPtr)
					strVal := unsafe.String((*byte)(unsafe.Pointer(strHdr.Data)), strHdr.Len)
					buffer = encodeString(buffer, strVal)
				case reflect.Bool:
					boolVal := *(*bool)(elemPtr)
					buffer = encodeBool(buffer, boolVal)
				default:
					return nil, fmt.Errorf("unsupported slice element type: %v", elemType.Kind())
				}
			}
		default:
			return nil, fmt.Errorf("unsupported field type: %v", fieldOffset.Type)
		}

		// Add the value end marker
		buffer = append(buffer, valueEndMarker)
	}

	return buffer, nil
}

// Utility functions for encoding/decoding primitive types

func encodeVarint(buffer []byte, value int64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], value)
	return append(buffer, tmp[:n]...)
}

func encodeUvarint(buffer []byte, value uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], value)
	return append(buffer, tmp[:n]...)
}

func encodeString(buffer []byte, value string) []byte {
	// Encode string length as varint
	buffer = encodeUvarint(buffer, uint64(len(value)))
	// Append string bytes
	return append(buffer, value...)
}

func encodeBool(buffer []byte, value bool) []byte {
	if value {
		return append(buffer, 1)
	}
	return append(buffer, 0)
}

func decodeVarint(data []byte) (interface{}, []byte, error) {
	value, n := binary.Varint(data)
	if n <= 0 {
		return nil, data, errors.New("invalid varint encoding")
	}
	return value, data[n:], nil
}

func decodeUvarint(data []byte) (interface{}, []byte, error) {
	value, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, data, errors.New("invalid uvarint encoding")
	}
	return value, data[n:], nil
}

func decodeString(data []byte) (interface{}, []byte, error) {
	// First decode the string length
	lengthValue, remaining, err := decodeUvarint(data)
	if err != nil {
		return nil, data, err
	}

	length := int(lengthValue.(uint64))
	if len(remaining) < length {
		return nil, data, errors.New("string data too short")
	}

	// Extract the string
	value := string(remaining[:length])
	return value, remaining[length:], nil
}

func decodeBool(data []byte) (interface{}, []byte, error) {
	if len(data) < 1 {
		return nil, data, errors.New("bool data too short")
	}
	value := data[0] != 0
	return value, data[1:], nil
}
