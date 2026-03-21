package embeddb

import (
	"fmt"
	"reflect"
	"sync"
	"time"
	"unsafe"

	embedcore "github.com/yay101/embeddbcore"
)

// PagedResult represents a paginated query result
type PagedResult[T any] struct {
	Records    []T  // The records for the current page
	TotalCount int  // Total number of matching records
	HasMore    bool // Whether there are more records beyond this page
	Offset     int  // The offset used for this query
	Limit      int  // The limit used for this query
}

// IndexManager handles the creation, management, and querying of indexes for a database.
//
// Deprecated: internal use only. This type will be made private in a future release.
type IndexManager[T any] struct {
	db               *Database[T]                     // Reference to the parent database
	tableName        string                           // Table name for multi-table support
	indexes          map[string]*BTreeIndex           // Map of field name to index
	layout           *embedcore.StructLayout          // Struct layout for fast field access
	fieldOffsetCache map[string]embedcore.FieldOffset // Cached field offsets for O(1) lookup
	lock             sync.RWMutex                     // Lock for concurrent access
	pendingIndexes   map[string]struct{}              // Set of indexes that need to be rebuilt
}

// NewIndexManager creates a new index manager for a database.
// The tableName parameter is used for multi-table support to namespace index files.
//
// Deprecated: internal use only. This function will be made private in a future release.
func NewIndexManager[T any](db *Database[T], layout *embedcore.StructLayout, tableName string) *IndexManager[T] {
	// Build field offset cache for O(1) lookup
	fieldOffsetCache := make(map[string]embedcore.FieldOffset, len(layout.FieldOffsets))
	for _, fo := range layout.FieldOffsets {
		fieldOffsetCache[fo.Name] = fo
	}

	return &IndexManager[T]{
		db:               db,
		tableName:        tableName,
		indexes:          make(map[string]*BTreeIndex),
		layout:           layout,
		fieldOffsetCache: fieldOffsetCache,
		pendingIndexes:   make(map[string]struct{}),
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

	// Find the field in the layout using cached map - O(1) lookup
	fieldOffset, fieldFound := im.fieldOffsetCache[fieldName]
	if !fieldFound {
		return fmt.Errorf("field '%s' not found in struct", fieldName)
	}

	indexFieldType := fieldOffset.Type
	if fieldOffset.IsSlice && fieldOffset.SliceElem != nil {
		indexFieldType = fieldOffset.SliceElem.Kind()
	}

	// Create the B-tree index
	index, err := NewBTreeIndex(im.db.file.Name(), im.tableName, fieldName, fieldOffset.Offset, indexFieldType, fieldOffset.IsTime)
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

	// Find the field offset using cached map - O(1) lookup
	fieldOffset, fieldFound := im.fieldOffsetCache[fieldName]
	if !fieldFound {
		im.lock.Unlock()
		return fmt.Errorf("field '%s' not found in struct", fieldName)
	}
	im.lock.Unlock()

	// Get all record IDs from the database (first table with data)
	im.db.lock.RLock()
	var recordIDs []uint32
	for _, idx := range im.db.indexes {
		for id := range idx {
			recordIDs = append(recordIDs, id)
		}
		break // Just get first table for now
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
			// Important: preserve the exact type to match B-tree field type expectations
			switch fieldOffset.Type {
			case reflect.String:
				fieldValue = *(*string)(fieldPtr)
			case reflect.Int:
				fieldValue = *(*int)(fieldPtr)
			case reflect.Int8:
				fieldValue = *(*int8)(fieldPtr)
			case reflect.Int16:
				fieldValue = *(*int16)(fieldPtr)
			case reflect.Int32:
				fieldValue = *(*int32)(fieldPtr)
			case reflect.Int64:
				fieldValue = *(*int64)(fieldPtr)
			case reflect.Uint:
				fieldValue = *(*uint)(fieldPtr)
			case reflect.Uint8:
				fieldValue = *(*uint8)(fieldPtr)
			case reflect.Uint16:
				fieldValue = *(*uint16)(fieldPtr)
			case reflect.Uint32:
				fieldValue = *(*uint32)(fieldPtr)
			case reflect.Uint64:
				fieldValue = *(*uint64)(fieldPtr)
			case reflect.Float32:
				fieldValue = *(*float32)(fieldPtr)
			case reflect.Float64:
				fieldValue = *(*float64)(fieldPtr)
			case reflect.Bool:
				fieldValue = *(*bool)(fieldPtr)
			case reflect.Struct:
				// Check if this is a time.Time field
				if fieldOffset.IsTime {
					fieldValue = *(*time.Time)(fieldPtr)
				} else {
					continue // Skip unsupported struct types
				}
			case reflect.Slice:
				sliceHeader := (*reflect.SliceHeader)(fieldPtr)
				if sliceHeader.Len == 0 {
					continue
				}

				elemType := fieldOffset.SliceElem
				dataPtr := sliceHeader.Data
				elemSize := elemType.Size()

				for i := 0; i < sliceHeader.Len; i++ {
					elemPtr := unsafe.Pointer(dataPtr + uintptr(i)*elemSize)
					var elem interface{}
					switch elemType.Kind() {
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						switch elemType.Kind() {
						case reflect.Int:
							elem = *(*int)(elemPtr)
						case reflect.Int8:
							elem = *(*int8)(elemPtr)
						case reflect.Int16:
							elem = *(*int16)(elemPtr)
						case reflect.Int32:
							elem = *(*int32)(elemPtr)
						case reflect.Int64:
							elem = *(*int64)(elemPtr)
						}
					case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
						switch elemType.Kind() {
						case reflect.Uint:
							elem = *(*uint)(elemPtr)
						case reflect.Uint8:
							elem = *(*uint8)(elemPtr)
						case reflect.Uint16:
							elem = *(*uint16)(elemPtr)
						case reflect.Uint32:
							elem = *(*uint32)(elemPtr)
						case reflect.Uint64:
							elem = *(*uint64)(elemPtr)
						}
					case reflect.Float32, reflect.Float64:
						if elemType.Kind() == reflect.Float32 {
							elem = *(*float32)(elemPtr)
						} else {
							elem = *(*float64)(elemPtr)
						}
					case reflect.String:
						strHdr := (*reflect.StringHeader)(elemPtr)
						elem = unsafe.String((*byte)(unsafe.Pointer(strHdr.Data)), strHdr.Len)
					case reflect.Bool:
						elem = *(*bool)(elemPtr)
					default:
						continue
					}
					if err := index.Insert(elem, id); err != nil {
						return fmt.Errorf("failed to index slice element: %w", err)
					}
				}
				continue
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
		// Find the field offset using cached map - O(1) lookup
		fieldOffset, fieldFound := im.fieldOffsetCache[fieldName]
		if !fieldFound {
			continue // Skip this index
		}

		// Extract the field value
		fieldPtr := unsafe.Pointer(uintptr(unsafe.Pointer(record)) + fieldOffset.Offset)
		var fieldValue interface{}

		// Get the field value based on its type
		// Important: preserve the exact type to match B-tree field type expectations
		switch fieldOffset.Type {
		case reflect.String:
			fieldValue = *(*string)(fieldPtr)
		case reflect.Int:
			fieldValue = *(*int)(fieldPtr)
		case reflect.Int8:
			fieldValue = *(*int8)(fieldPtr)
		case reflect.Int16:
			fieldValue = *(*int16)(fieldPtr)
		case reflect.Int32:
			fieldValue = *(*int32)(fieldPtr)
		case reflect.Int64:
			fieldValue = *(*int64)(fieldPtr)
		case reflect.Uint:
			fieldValue = *(*uint)(fieldPtr)
		case reflect.Uint8:
			fieldValue = *(*uint8)(fieldPtr)
		case reflect.Uint16:
			fieldValue = *(*uint16)(fieldPtr)
		case reflect.Uint32:
			fieldValue = *(*uint32)(fieldPtr)
		case reflect.Uint64:
			fieldValue = *(*uint64)(fieldPtr)
		case reflect.Float32:
			fieldValue = *(*float32)(fieldPtr)
		case reflect.Float64:
			fieldValue = *(*float64)(fieldPtr)
		case reflect.Bool:
			fieldValue = *(*bool)(fieldPtr)
		case reflect.Struct:
			// Check if this is a time.Time field
			if fieldOffset.IsTime {
				fieldValue = *(*time.Time)(fieldPtr)
			} else {
				continue // Skip unsupported struct types
			}
		case reflect.Slice:
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			if sliceHeader.Len == 0 {
				continue
			}

			elemType := fieldOffset.SliceElem
			dataPtr := sliceHeader.Data
			elemSize := elemType.Size()

			for i := 0; i < sliceHeader.Len; i++ {
				elemPtr := unsafe.Pointer(dataPtr + uintptr(i)*elemSize)
				var elem interface{}
				switch elemType.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					switch elemType.Kind() {
					case reflect.Int:
						elem = *(*int)(elemPtr)
					case reflect.Int8:
						elem = *(*int8)(elemPtr)
					case reflect.Int16:
						elem = *(*int16)(elemPtr)
					case reflect.Int32:
						elem = *(*int32)(elemPtr)
					case reflect.Int64:
						elem = *(*int64)(elemPtr)
					}
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					switch elemType.Kind() {
					case reflect.Uint:
						elem = *(*uint)(elemPtr)
					case reflect.Uint8:
						elem = *(*uint8)(elemPtr)
					case reflect.Uint16:
						elem = *(*uint16)(elemPtr)
					case reflect.Uint32:
						elem = *(*uint32)(elemPtr)
					case reflect.Uint64:
						elem = *(*uint64)(elemPtr)
					}
				case reflect.Float32, reflect.Float64:
					if elemType.Kind() == reflect.Float32 {
						elem = *(*float32)(elemPtr)
					} else {
						elem = *(*float64)(elemPtr)
					}
				case reflect.String:
					strHdr := (*reflect.StringHeader)(elemPtr)
					elem = unsafe.String((*byte)(unsafe.Pointer(strHdr.Data)), strHdr.Len)
				case reflect.Bool:
					elem = *(*bool)(elemPtr)
				default:
					continue
				}
				if err := index.Insert(elem, recordID); err != nil {
					return fmt.Errorf("failed to index slice element: %w", err)
				}
			}
			continue
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
		// Find the field offset using cached map - O(1) lookup
		fieldOffset, fieldFound := im.fieldOffsetCache[fieldName]
		if !fieldFound {
			continue // Skip this index
		}

		// Extract the field value
		fieldPtr := unsafe.Pointer(uintptr(unsafe.Pointer(record)) + fieldOffset.Offset)
		var fieldValue interface{}

		// Get the field value based on its type
		// Important: preserve the exact type to match B-tree field type expectations
		switch fieldOffset.Type {
		case reflect.String:
			fieldValue = *(*string)(fieldPtr)
		case reflect.Int:
			fieldValue = *(*int)(fieldPtr)
		case reflect.Int8:
			fieldValue = *(*int8)(fieldPtr)
		case reflect.Int16:
			fieldValue = *(*int16)(fieldPtr)
		case reflect.Int32:
			fieldValue = *(*int32)(fieldPtr)
		case reflect.Int64:
			fieldValue = *(*int64)(fieldPtr)
		case reflect.Uint:
			fieldValue = *(*uint)(fieldPtr)
		case reflect.Uint8:
			fieldValue = *(*uint8)(fieldPtr)
		case reflect.Uint16:
			fieldValue = *(*uint16)(fieldPtr)
		case reflect.Uint32:
			fieldValue = *(*uint32)(fieldPtr)
		case reflect.Uint64:
			fieldValue = *(*uint64)(fieldPtr)
		case reflect.Float32:
			fieldValue = *(*float32)(fieldPtr)
		case reflect.Float64:
			fieldValue = *(*float64)(fieldPtr)
		case reflect.Bool:
			fieldValue = *(*bool)(fieldPtr)
		case reflect.Struct:
			// Check if this is a time.Time field
			if fieldOffset.IsTime {
				fieldValue = *(*time.Time)(fieldPtr)
			} else {
				continue // Skip unsupported struct types
			}
		case reflect.Slice:
			// For slice fields, get the slice header and iterate over elements
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			if sliceHeader.Len == 0 {
				continue // Skip empty slices
			}
			sliceVal := reflect.NewAt(
				reflect.SliceOf(fieldOffset.SliceElem),
				unsafe.Pointer(sliceHeader.Data),
			).Elem()

			// Remove each element from the index
			for i := 0; i < sliceVal.Len(); i++ {
				elem := sliceVal.Index(i).Interface()
				if err := index.Remove(elem, recordID); err != nil {
					return fmt.Errorf("failed to remove slice element from index: %w", err)
				}
			}
			continue
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

	// Flush pending writes before querying to ensure all data is in the index
	if err := index.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush index before query: %w", err)
	}

	// Find all matching records
	return index.Find(value)
}

// QueryRangeGreaterThan finds records where field > value (or >= if inclusive)
func (im *IndexManager[T]) QueryRangeGreaterThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	im.lock.RLock()
	index, exists := im.indexes[fieldName]
	im.lock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	if err := index.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush index before query: %w", err)
	}

	recordIDs, err := index.FindGreaterThan(value, inclusive)
	if err != nil {
		return nil, err
	}

	return im.collectRecordsByID(recordIDs), nil
}

// QueryRangeLessThan finds records where field < value (or <= if inclusive)
func (im *IndexManager[T]) QueryRangeLessThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	im.lock.RLock()
	index, exists := im.indexes[fieldName]
	im.lock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	if err := index.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush index before query: %w", err)
	}

	recordIDs, err := index.FindLessThan(value, inclusive)
	if err != nil {
		return nil, err
	}

	return im.collectRecordsByID(recordIDs), nil
}

// QueryRangeBetween finds records where min <= field <= max
func (im *IndexManager[T]) QueryRangeBetween(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]T, error) {
	im.lock.RLock()
	index, exists := im.indexes[fieldName]
	im.lock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	if err := index.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush index before query: %w", err)
	}

	recordIDs, err := index.FindBetween(min, max, inclusiveMin, inclusiveMax)
	if err != nil {
		return nil, err
	}

	return im.collectRecordsByID(recordIDs), nil
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

// RebuildAll rebuilds all indexes managed by this manager
func (im *IndexManager[T]) RebuildAll() error {
	im.lock.Lock()
	fields := make([]string, 0, len(im.indexes))
	for fieldName := range im.indexes {
		fields = append(fields, fieldName)
	}
	im.lock.Unlock()

	for _, fieldName := range fields {
		im.lock.Lock()
		index, exists := im.indexes[fieldName]
		if exists {
			// Re-initialize the index file to clear it
			if err := index.initializeFile(); err != nil {
				im.lock.Unlock()
				return fmt.Errorf("failed to re-initialize index for field '%s': %w", fieldName, err)
			}
		}
		im.lock.Unlock()

		// Rebuild the index by scanning the database
		if err := im.BuildIndex(fieldName); err != nil {
			return fmt.Errorf("failed to rebuild index for field '%s': %w", fieldName, err)
		}
	}

	return nil
}

// CheckIndexes checks if all indexes exist and creates any missing ones
func (im *IndexManager[T]) CheckIndexes() error {
	im.lock.Lock()
	defer im.lock.Unlock()

	if useExperimentalRegionIndex() {
		for _, offset := range im.layout.FieldOffsets {
			fieldName := offset.Name

			if _, exists := im.indexes[fieldName]; exists {
				continue
			}

			existsInRegion, err := regionCatalogEntryExists(im.db.file, im.tableName, fieldName)
			if err != nil {
				return fmt.Errorf("failed to inspect region catalog for field '%s': %w", fieldName, err)
			}
			if !existsInRegion {
				continue
			}

			indexFieldType := offset.Type
			if offset.IsSlice && offset.SliceElem != nil {
				indexFieldType = offset.SliceElem.Kind()
			}

			index, err := NewBTreeIndex(im.db.file.Name(), im.tableName, fieldName, offset.Offset, indexFieldType, offset.IsTime)
			if err != nil {
				im.pendingIndexes[fieldName] = struct{}{}
				continue
			}

			im.indexes[fieldName] = index
		}

		return nil
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

// QueryPaged finds records that match a field value with pagination
func (im *IndexManager[T]) QueryPaged(fieldName string, value interface{}, offset, limit int) (*PagedResult[T], error) {
	im.lock.RLock()
	index, exists := im.indexes[fieldName]
	im.lock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	if err := index.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush index before query: %w", err)
	}

	allIDs, err := index.Find(value)
	if err != nil {
		return nil, err
	}
	totalCount := len(allIDs)

	end := offset + limit
	if offset >= len(allIDs) {
		return &PagedResult[T]{
			Records:    []T{},
			TotalCount: totalCount,
			HasMore:    false,
			Offset:     offset,
			Limit:      limit,
		}, nil
	}

	if end > len(allIDs) {
		end = len(allIDs)
	}

	pagedIDs := allIDs[offset:end]

	results := im.collectRecordsByID(pagedIDs)

	return &PagedResult[T]{
		Records:    results,
		TotalCount: totalCount,
		HasMore:    end < len(allIDs),
		Offset:     offset,
		Limit:      limit,
	}, nil
}

// QueryRangeGreaterThanPaged finds records where field > value with pagination
func (im *IndexManager[T]) QueryRangeGreaterThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	im.lock.RLock()
	index, exists := im.indexes[fieldName]
	im.lock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	if err := index.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush index before query: %w", err)
	}

	allIDs, err := index.FindGreaterThan(value, inclusive)
	if err != nil {
		return nil, err
	}
	totalCount := len(allIDs)

	end := offset + limit
	if offset >= len(allIDs) {
		return &PagedResult[T]{
			Records:    []T{},
			TotalCount: totalCount,
			HasMore:    false,
			Offset:     offset,
			Limit:      limit,
		}, nil
	}

	if end > len(allIDs) {
		end = len(allIDs)
	}

	pagedIDs := allIDs[offset:end]

	results := im.collectRecordsByID(pagedIDs)

	return &PagedResult[T]{
		Records:    results,
		TotalCount: totalCount,
		HasMore:    end < len(allIDs),
		Offset:     offset,
		Limit:      limit,
	}, nil
}

// QueryRangeLessThanPaged finds records where field < value with pagination
func (im *IndexManager[T]) QueryRangeLessThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	im.lock.RLock()
	index, exists := im.indexes[fieldName]
	im.lock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	if err := index.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush index before query: %w", err)
	}

	allIDs, err := index.FindLessThan(value, inclusive)
	if err != nil {
		return nil, err
	}
	totalCount := len(allIDs)

	end := offset + limit
	if offset >= len(allIDs) {
		return &PagedResult[T]{
			Records:    []T{},
			TotalCount: totalCount,
			HasMore:    false,
			Offset:     offset,
			Limit:      limit,
		}, nil
	}

	if end > len(allIDs) {
		end = len(allIDs)
	}

	pagedIDs := allIDs[offset:end]

	results := im.collectRecordsByID(pagedIDs)

	return &PagedResult[T]{
		Records:    results,
		TotalCount: totalCount,
		HasMore:    end < len(allIDs),
		Offset:     offset,
		Limit:      limit,
	}, nil
}

// QueryRangeBetweenPaged finds records where min <= field <= max with pagination
func (im *IndexManager[T]) QueryRangeBetweenPaged(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool, offset, limit int) (*PagedResult[T], error) {
	im.lock.RLock()
	index, exists := im.indexes[fieldName]
	im.lock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	if err := index.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush index before query: %w", err)
	}

	allIDs, err := index.FindBetween(min, max, inclusiveMin, inclusiveMax)
	if err != nil {
		return nil, err
	}
	totalCount := len(allIDs)

	end := offset + limit
	if offset >= len(allIDs) {
		return &PagedResult[T]{
			Records:    []T{},
			TotalCount: totalCount,
			HasMore:    false,
			Offset:     offset,
			Limit:      limit,
		}, nil
	}

	if end > len(allIDs) {
		end = len(allIDs)
	}

	pagedIDs := allIDs[offset:end]

	results := im.collectRecordsByID(pagedIDs)

	return &PagedResult[T]{
		Records:    results,
		TotalCount: totalCount,
		HasMore:    end < len(allIDs),
		Offset:     offset,
		Limit:      limit,
	}, nil
}

func (im *IndexManager[T]) collectRecordsByID(recordIDs []uint32) []T {
	return im.db.collectRecordsByIDs(recordIDs)
}
