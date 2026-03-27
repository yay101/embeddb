package embeddb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	embedcore "github.com/yay101/embeddbcore"
)

func encodePKForIndex(tableID uint8, pkValue any) []byte {
	var key []byte
	key = append(key, tableID)
	switch v := pkValue.(type) {
	case int:
		key = embedcore.EncodeUvarint(key, uint64(int64(v)))
	case int8:
		key = embedcore.EncodeUvarint(key, uint64(int64(v)))
	case int16:
		key = embedcore.EncodeUvarint(key, uint64(int64(v)))
	case int32:
		key = embedcore.EncodeUvarint(key, uint64(int64(v)))
	case int64:
		key = embedcore.EncodeUvarint(key, uint64(v))
	case uint:
		key = embedcore.EncodeUvarint(key, uint64(v))
	case uint8:
		key = embedcore.EncodeUvarint(key, uint64(v))
	case uint16:
		key = embedcore.EncodeUvarint(key, uint64(v))
	case uint32:
		key = embedcore.EncodeUvarint(key, uint64(v))
	case uint64:
		key = embedcore.EncodeUvarint(key, v)
	case string:
		key = embedcore.EncodeString(key, v)
	}
	return key
}

type OpenOptions struct {
	Migrate   bool
	AutoIndex bool
}

type DB struct {
	filename  string
	migrate   bool
	autoIndex bool
	lock      sync.Mutex
	closed    bool
	database  *database
	tables    map[string]*database
}

func Open(filename string, opts ...OpenOptions) (*DB, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename is required")
	}

	config := OpenOptions{}
	if len(opts) > 0 {
		config = opts[0]
	}

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("failed to close file handle: %w", err)
	}

	return &DB{
		filename:  filename,
		migrate:   config.Migrate,
		autoIndex: config.AutoIndex,
		tables:    make(map[string]*database),
	}, nil
}

func Use[T any](db *DB, name ...string) (*TypedTable[T], error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}

	tableName := resolveTableName[T](name...)

	db.lock.Lock()
	defer db.lock.Unlock()

	if db.closed {
		return nil, fmt.Errorf("database is closed")
	}

	if existing, ok := db.tables[tableName]; ok {
		existingTable, ok := existing.tableCat[tableName]
		if !ok {
			return nil, fmt.Errorf("table not found")
		}
		layout := computeLayout[T]()
		return &TypedTable[T]{
			db:      existing,
			name:    tableName,
			tableID: existingTable.ID,
			layout:  layout,
		}, nil
	}

	var typedDB *database
	if db.database != nil {
		typedDB = db.database
	} else {
		var err error
		typedDB, err = OpenDatabase(db.filename, false)
		if err != nil {
			return nil, err
		}
		db.database = typedDB
	}

	layout := computeLayout[T]()

	typedDB.mu.Lock()
	table, exists := typedDB.tableCat[tableName]
	if !exists {
		var maxID uint8
		for _, entry := range typedDB.tableCat {
			if entry.ID > maxID && !entry.Dropped {
				maxID = entry.ID
			}
		}
		maxID++
		table = &tableCatalogEntry{
			ID:           maxID,
			Name:         tableName,
			LayoutHash:   layout.Hash,
			NextRecordID: 1,
		}
		typedDB.tableCat[tableName] = table
	}
	typedDB.mu.Unlock()

	typedTable := &TypedTable[T]{
		db:      typedDB,
		name:    tableName,
		tableID: table.ID,
		layout:  layout,
	}

	if db.autoIndex {
		for _, field := range layout.FieldOffsets {
			if field.Name != "" && !field.Primary && field.Offset > 0 {
				_ = typedTable.CreateIndex(field.Name)
			}
		}
	}

	db.tables[tableName] = typedDB

	return typedTable, nil
}

func (db *DB) Close() error {
	if db == nil {
		return nil
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	if db.closed {
		return nil
	}

	var closeErr error
	if db.database != nil {
		if err := db.database.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
		db.database = nil
	}
	db.tables = make(map[string]*database)
	db.closed = true

	return closeErr
}

func (db *DB) Begin() *Transaction {
	if db == nil || db.database == nil {
		return nil
	}
	return db.database.Begin()
}

func (db *DB) Commit() error {
	if db == nil || db.database == nil {
		return fmt.Errorf("database not initialized")
	}
	if db.database.tx == nil {
		return fmt.Errorf("no active transaction")
	}
	return db.database.tx.Commit()
}

func (db *DB) Rollback() error {
	if db == nil || db.database == nil {
		return fmt.Errorf("database not initialized")
	}
	if db.database.tx == nil {
		return fmt.Errorf("no active transaction")
	}
	return db.database.tx.Rollback()
}

func (db *DB) Sync() error {
	if db == nil {
		return nil
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	if db.closed {
		return fmt.Errorf("database is closed")
	}

	for _, t := range db.tables {
		return t.Flush()
	}
	return nil
}

func (db *DB) Vacuum() error {
	if db == nil {
		return nil
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	if db.closed {
		return fmt.Errorf("database is closed")
	}

	for _, t := range db.tables {
		return t.Vacuum()
	}
	return nil
}

func resolveTableName[T any](name ...string) string {
	if len(name) > 0 && name[0] != "" {
		return name[0]
	}

	var instance T
	t := reflect.TypeOf(instance)
	if t == nil {
		return ""
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Name() != "" {
		return t.Name()
	}
	return t.String()
}

func computeLayout[T any]() *embedcore.StructLayout {
	var instance T
	layout, _ := embedcore.ComputeStructLayout(instance)
	return layout
}

type TypedTable[T any] struct {
	db      *database
	name    string
	tableID uint8
	layout  *embedcore.StructLayout
	idxMgr  *typedIndexManager[T]
}

type typedIndexManager[T any] struct {
	db      *database
	layout  *embedcore.StructLayout
	indexes map[string]*Uint32MapIndex
}

func newTypedIndexManager[T any](db *database, layout *embedcore.StructLayout) *typedIndexManager[T] {
	return &typedIndexManager[T]{
		db:      db,
		layout:  layout,
		indexes: make(map[string]*Uint32MapIndex),
	}
}

func (im *typedIndexManager[T]) CreateIndex(fieldName string) error {
	for _, field := range im.layout.FieldOffsets {
		if field.Name == fieldName {
			im.indexes[fieldName] = NewUint32MapIndex()
			return nil
		}
	}
	return fmt.Errorf("field %s not found", fieldName)
}

func (im *typedIndexManager[T]) DropIndex(fieldName string) error {
	delete(im.indexes, fieldName)
	return nil
}

func (im *typedIndexManager[T]) HasIndex(fieldName string) bool {
	_, ok := im.indexes[fieldName]
	return ok
}

func (im *typedIndexManager[T]) GetIndexedFields() []string {
	fields := make([]string, 0, len(im.indexes))
	for f := range im.indexes {
		fields = append(fields, f)
	}
	return fields
}

func (im *typedIndexManager[T]) InsertIntoIndexes(record *T, recordID uint32) error {
	for fieldName, idx := range im.indexes {
		var field embedcore.FieldOffset
		var found bool
		for _, f := range im.layout.FieldOffsets {
			if f.Name == fieldName {
				field = f
				found = true
				break
			}
		}
		if !found {
			continue
		}
		val, err := embedcore.GetFieldValue(record, field)
		if err != nil {
			continue
		}
		key := fmt.Sprintf("%v", val)
		idx.Set(key, recordID)
	}
	return nil
}

func (im *typedIndexManager[T]) UpdateIndexes(record *T, recordID uint32) error {
	for fieldName, idx := range im.indexes {
		var field embedcore.FieldOffset
		var found bool
		for _, f := range im.layout.FieldOffsets {
			if f.Name == fieldName {
				field = f
				found = true
				break
			}
		}
		if !found {
			continue
		}
		val, err := embedcore.GetFieldValue(record, field)
		if err != nil {
			continue
		}
		key := fmt.Sprintf("%v", val)
		idx.Set(key, recordID)
	}
	return nil
}

func (im *typedIndexManager[T]) Query(fieldName string, value interface{}) ([]uint32, error) {
	idx, ok := im.indexes[fieldName]
	if !ok {
		return nil, fmt.Errorf("no index for field %s", fieldName)
	}
	key := fmt.Sprintf("%v", value)
	id, ok := idx.Get(key)
	if !ok {
		return []uint32{}, nil
	}
	return []uint32{id}, nil
}

func (im *typedIndexManager[T]) QueryRangeGreaterThan(fieldName string, value interface{}, inclusive bool) ([]uint32, error) {
	return nil, errors.New("QueryRangeGreaterThan not yet implemented")
}

func (im *typedIndexManager[T]) QueryRangeLessThan(fieldName string, value interface{}, inclusive bool) ([]uint32, error) {
	return nil, errors.New("QueryRangeLessThan not yet implemented")
}

func (im *typedIndexManager[T]) QueryRangeBetween(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]uint32, error) {
	return nil, errors.New("QueryRangeBetween not yet implemented")
}

func (t *TypedTable[T]) Insert(record *T) (uint32, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	entry := t.db.tableCat[t.name]
	if entry == nil {
		return 0, fmt.Errorf("table not found")
	}

	recordID := entry.NextRecordID
	entry.NextRecordID++

	pkVal, _ := t.getPKValue(record)
	if t.isZeroPK(pkVal) {
		t.setPKValue(record, recordID)
		pkVal = recordID
	}

	encoded, err := t.encodeRecord(record)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %v", err)
	}

	headerBytes := make([]byte, 12)
	headerBytes[0] = EcCode
	headerBytes[1] = RecordStartMark
	headerBytes[2] = t.tableID
	binary.BigEndian.PutUint32(headerBytes[3:7], recordID)
	binary.BigEndian.PutUint32(headerBytes[7:11], uint32(len(encoded)))
	headerBytes[11] = 1

	footerBytes := []byte{EcCode, RecordEndMark}

	recordBuf := make([]byte, 0, len(headerBytes)+len(encoded)+len(footerBytes))
	recordBuf = append(recordBuf, headerBytes...)
	recordBuf = append(recordBuf, encoded...)
	recordBuf = append(recordBuf, footerBytes...)

	offset, _ := t.db.alloc.Allocate(uint64(len(recordBuf)))
	t.db.file.WriteAt(recordBuf, int64(offset))

	indexKey := encodePKForIndex(t.tableID, pkVal)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, offset)
	t.db.pkIndex.Set(indexKey, val)

	if t.idxMgr != nil {
		t.idxMgr.InsertIntoIndexes(record, recordID)
	}

	return recordID, nil
}

func (t *TypedTable[T]) isZeroPK(val any) bool {
	if val == nil {
		return true
	}
	switch v := val.(type) {
	case int:
		return v == 0
	case int8:
		return v == 0
	case int16:
		return v == 0
	case int32:
		return v == 0
	case int64:
		return v == 0
	case uint:
		return v == 0
	case uint8:
		return v == 0
	case uint16:
		return v == 0
	case uint32:
		return v == 0
	case uint64:
		return v == 0
	case string:
		return v == ""
	}
	return false
}

func (t *TypedTable[T]) setPKValue(record *T, val any) {
	for _, field := range t.layout.FieldOffsets {
		if field.Primary {
			embedcore.SetFieldValue(record, field, val)
			return
		}
	}
}

func (t *TypedTable[T]) getPKValue(record *T) (any, error) {
	for _, field := range t.layout.FieldOffsets {
		if field.Primary {
			val, err := embedcore.GetFieldValue(record, field)
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	}
	return nil, fmt.Errorf("no primary key found")
}

func (t *TypedTable[T]) Get(id any) (*T, error) {
	t.db.mu.RLock()
	defer t.db.mu.RUnlock()

	indexKey := encodePKForIndex(t.tableID, id)

	val, ok := t.db.pkIndex.Get(indexKey)
	if !ok {
		return nil, fmt.Errorf("record not found")
	}

	offset := binary.BigEndian.Uint64(val)

	recordBuf := make([]byte, 4096)
	n, err := t.db.file.ReadAt(recordBuf, int64(offset))
	if err != nil && n < 12 {
		return nil, fmt.Errorf("read error at offset %d: %v", offset, err)
	}

	if recordBuf[0] != EcCode || recordBuf[1] != RecordStartMark {
		return nil, fmt.Errorf("invalid record header")
	}

	active := recordBuf[11]
	if active != 1 {
		return nil, fmt.Errorf("record is deleted")
	}

	var result T
	if err := t.decodeRecord(recordBuf[:n], &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (t *TypedTable[T]) queryByPK(pkValue string) (uint32, bool) {
	if t.idxMgr == nil {
		return 0, false
	}
	for fieldName, idx := range t.idxMgr.indexes {
		var isPrimary bool
		for _, f := range t.layout.FieldOffsets {
			if f.Name == fieldName {
				isPrimary = f.Primary
				break
			}
		}
		if !isPrimary {
			continue
		}
		id, ok := idx.Get(pkValue)
		return id, ok
	}
	return 0, false
}

func (t *TypedTable[T]) Update(id any, record *T) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	indexKey := encodePKForIndex(t.tableID, id)
	val, ok := t.db.pkIndex.Get(indexKey)
	if !ok {
		return fmt.Errorf("record not found")
	}

	oldOffset := binary.BigEndian.Uint64(val)

	recordBuf := make([]byte, 12)
	t.db.file.ReadAt(recordBuf, int64(oldOffset))
	if recordBuf[0] != EcCode || recordBuf[1] != RecordStartMark {
		return fmt.Errorf("invalid record at offset %d", oldOffset)
	}

	t.db.file.WriteAt([]byte{0}, int64(oldOffset+11))

	encoded, err := t.encodeRecord(record)
	if err != nil {
		return fmt.Errorf("failed to encode record: %v", err)
	}

	headerBytes := make([]byte, 12)
	headerBytes[0] = EcCode
	headerBytes[1] = RecordStartMark
	headerBytes[2] = t.tableID
	binary.BigEndian.PutUint32(headerBytes[3:7], binary.BigEndian.Uint32(recordBuf[3:7]))
	binary.BigEndian.PutUint32(headerBytes[7:11], uint32(len(encoded)))
	headerBytes[11] = 1

	footerBytes := []byte{EcCode, RecordEndMark}

	newRecordBuf := make([]byte, 0, len(headerBytes)+len(encoded)+len(footerBytes))
	newRecordBuf = append(newRecordBuf, headerBytes...)
	newRecordBuf = append(newRecordBuf, encoded...)
	newRecordBuf = append(newRecordBuf, footerBytes...)

	newOffset, _ := t.db.alloc.Allocate(uint64(len(newRecordBuf)))
	t.db.file.WriteAt(newRecordBuf, int64(newOffset))

	newVal := make([]byte, 8)
	binary.BigEndian.PutUint64(newVal, newOffset)
	t.db.pkIndex.Set(indexKey, newVal)

	return nil
}

func (t *TypedTable[T]) Delete(id any) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	indexKey := encodePKForIndex(t.tableID, id)
	val, ok := t.db.pkIndex.Get(indexKey)
	if !ok {
		return fmt.Errorf("record not found")
	}

	offset := binary.BigEndian.Uint64(val)

	t.db.file.WriteAt([]byte{0}, int64(offset+11))

	t.db.pkIndex.Delete(indexKey)

	return nil
}

func (t *TypedTable[T]) DeleteMany(ids []any) (int, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	deleted := 0
	for _, id := range ids {
		indexKey := encodePKForIndex(t.tableID, id)
		val, ok := t.db.pkIndex.Get(indexKey)
		if !ok {
			continue
		}

		offset := binary.BigEndian.Uint64(val)
		t.db.file.WriteAt([]byte{0}, int64(offset+11))
		t.db.pkIndex.Delete(indexKey)
		deleted++
	}
	return deleted, nil
}

func (t *TypedTable[T]) InsertMany(records []*T) ([]uint32, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	ids := make([]uint32, 0, len(records))
	for _, record := range records {
		id, err := t.insertLocked(record)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (t *TypedTable[T]) UpdateMany(ids []any, updater func(*T) error) (int, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	updated := 0
	for _, id := range ids {
		record, err := t.getLocked(id)
		if err != nil {
			continue
		}
		if err := updater(record); err != nil {
			continue
		}
		if err := t.updateLocked(id, record); err != nil {
			continue
		}
		updated++
	}
	return updated, nil
}

func (t *TypedTable[T]) insertLocked(record *T) (uint32, error) {
	entry := t.db.tableCat[t.name]
	if entry == nil {
		return 0, fmt.Errorf("table not found")
	}

	recordID := entry.NextRecordID
	entry.NextRecordID++

	pkVal, _ := t.getPKValue(record)
	if t.isZeroPK(pkVal) {
		t.setPKValue(record, recordID)
		pkVal = recordID
	}

	encoded, err := t.encodeRecord(record)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %v", err)
	}

	headerBytes := make([]byte, 12)
	headerBytes[0] = EcCode
	headerBytes[1] = RecordStartMark
	headerBytes[2] = t.tableID
	binary.BigEndian.PutUint32(headerBytes[3:7], recordID)
	binary.BigEndian.PutUint32(headerBytes[7:11], uint32(len(encoded)))
	headerBytes[11] = 1

	footerBytes := []byte{EcCode, RecordEndMark}

	recordBuf := make([]byte, 0, len(headerBytes)+len(encoded)+len(footerBytes))
	recordBuf = append(recordBuf, headerBytes...)
	recordBuf = append(recordBuf, encoded...)
	recordBuf = append(recordBuf, footerBytes...)

	offset, _ := t.db.alloc.Allocate(uint64(len(recordBuf)))
	t.db.file.WriteAt(recordBuf, int64(offset))

	indexKey := encodePKForIndex(t.tableID, pkVal)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, offset)
	t.db.pkIndex.Set(indexKey, val)

	if t.idxMgr != nil {
		t.idxMgr.InsertIntoIndexes(record, recordID)
	}

	return recordID, nil
}

func (t *TypedTable[T]) getLocked(id any) (*T, error) {
	indexKey := encodePKForIndex(t.tableID, id)
	val, ok := t.db.pkIndex.Get(indexKey)
	if !ok {
		return nil, fmt.Errorf("record not found")
	}

	offset := binary.BigEndian.Uint64(val)

	recordBuf := make([]byte, 4096)
	n, err := t.db.file.ReadAt(recordBuf, int64(offset))
	if err != nil && n < 12 {
		return nil, fmt.Errorf("read error at offset %d: %v", offset, err)
	}

	if recordBuf[0] != EcCode || recordBuf[1] != RecordStartMark {
		return nil, fmt.Errorf("invalid record header")
	}

	active := recordBuf[11]
	if active != 1 {
		return nil, fmt.Errorf("record is deleted")
	}

	var result T
	if err := t.decodeRecord(recordBuf[:n], &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (t *TypedTable[T]) updateLocked(id any, record *T) error {
	indexKey := encodePKForIndex(t.tableID, id)
	val, ok := t.db.pkIndex.Get(indexKey)
	if !ok {
		return fmt.Errorf("record not found")
	}

	offset := binary.BigEndian.Uint64(val)

	recID := binary.BigEndian.Uint32(val)

	encoded, err := t.encodeRecord(record)
	if err != nil {
		return fmt.Errorf("failed to encode record: %v", err)
	}

	headerBytes := make([]byte, 12)
	headerBytes[0] = EcCode
	headerBytes[1] = RecordStartMark
	headerBytes[2] = t.tableID
	binary.BigEndian.PutUint32(headerBytes[3:7], recID)
	binary.BigEndian.PutUint32(headerBytes[7:11], uint32(len(encoded)))
	headerBytes[11] = 1

	footerBytes := []byte{EcCode, RecordEndMark}

	recordBuf := make([]byte, 0, len(headerBytes)+len(encoded)+len(footerBytes))
	recordBuf = append(recordBuf, headerBytes...)
	recordBuf = append(recordBuf, encoded...)
	recordBuf = append(recordBuf, footerBytes...)

	t.db.file.WriteAt(recordBuf, int64(offset))

	if t.idxMgr != nil {
		t.idxMgr.UpdateIndexes(record, recID)
	}

	return nil
}

func (t *TypedTable[T]) Upsert(id any, record *T) (uint32, bool, error) {
	existing, err := t.Get(id)
	if err == nil && existing != nil {
		if updateErr := t.Update(id, record); updateErr != nil {
			return 0, false, updateErr
		}
		return 0, false, nil
	}

	insertID, err := t.Insert(record)
	if err != nil {
		return 0, true, err
	}
	return insertID, true, nil
}

func (t *TypedTable[T]) Query(fieldName string, value interface{}) ([]T, error) {
	if t.idxMgr != nil && t.idxMgr.HasIndex(fieldName) {
		recordIDs, err := t.idxMgr.Query(fieldName, value)
		if err != nil {
			return nil, err
		}

		slices.Sort(recordIDs)
		results := make([]T, 0, len(recordIDs))

		for _, id := range recordIDs {
			record, err := t.Get(id)
			if err == nil && record != nil {
				results = append(results, *record)
			}
		}

		return results, nil
	}

	if strings.Contains(fieldName, ".") {
		comparator := func(fieldValue interface{}) bool {
			return compareValues(fieldValue, value) == 0
		}
		return t.filterByField(fieldName, comparator)
	}

	return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
}

func (t *TypedTable[T]) QueryRangeGreaterThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) > 0 || (inclusive && compareValues(fieldValue, value) == 0)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *TypedTable[T]) QueryRangeLessThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) < 0 || (inclusive && compareValues(fieldValue, value) == 0)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *TypedTable[T]) QueryRangeBetween(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		cMin := compareValues(fieldValue, min)
		cMax := compareValues(fieldValue, max)
		aboveMin := cMin > 0 || (inclusiveMin && cMin == 0)
		belowMax := cMax < 0 || (inclusiveMax && cMax == 0)
		return aboveMin && belowMax
	}
	return t.filterByField(fieldName, comparator)
}

func (t *TypedTable[T]) QueryNotEqual(fieldName string, value interface{}) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) != 0
	}
	return t.filterByField(fieldName, comparator)
}

func (t *TypedTable[T]) QueryGreaterOrEqual(fieldName string, value interface{}) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) >= 0
	}
	return t.filterByField(fieldName, comparator)
}

func (t *TypedTable[T]) QueryLessOrEqual(fieldName string, value interface{}) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) <= 0
	}
	return t.filterByField(fieldName, comparator)
}

func (t *TypedTable[T]) QueryLike(fieldName string, pattern string) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		str, ok := fieldValue.(string)
		if !ok {
			return false
		}
		return matchLike(str, pattern)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *TypedTable[T]) QueryNotLike(fieldName string, pattern string) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		str, ok := fieldValue.(string)
		if !ok {
			return false
		}
		return !matchLike(str, pattern)
	}
	return t.filterByField(fieldName, comparator)
}

func matchLike(s, pattern string) bool {
	if pattern == "" {
		return s == ""
	}
	if pattern == "%" {
		return true
	}
	sLower := strings.ToLower(s)
	patternLower := strings.ToLower(pattern)
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		return strings.Contains(sLower, strings.Trim(patternLower, "%"))
	}
	if strings.HasPrefix(pattern, "%") {
		return strings.HasSuffix(sLower, strings.Trim(patternLower, "%"))
	}
	if strings.HasSuffix(pattern, "%") {
		return strings.HasPrefix(sLower, strings.Trim(patternLower, "%"))
	}
	return sLower == patternLower
}

func (t *TypedTable[T]) filterByField(fieldName string, fn func(interface{}) bool) ([]T, error) {
	field, err := t.findField(fieldName)
	if err != nil {
		return nil, err
	}

	scanner := t.ScanRecords()
	defer scanner.Close()

	results := make([]T, 0)
	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}
		fieldVal, err := embedcore.GetFieldValue(record, field)
		if err != nil {
			continue
		}
		if fn(fieldVal) {
			results = append(results, *record)
		}
	}
	return results, nil
}

func (t *TypedTable[T]) filterPagedByField(field embedcore.FieldOffset, fn func(interface{}) bool, offset, limit int) (*PagedResult[T], error) {
	scanner := t.ScanRecords()
	defer scanner.Close()

	var results []T
	totalCount := 0
	skipped := 0

	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}
		fieldVal, err := embedcore.GetFieldValue(record, field)
		if err != nil {
			continue
		}
		if fn(fieldVal) {
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

	hasMore := totalCount > offset+len(results)
	return &PagedResult[T]{
		Records:    results,
		TotalCount: totalCount,
		HasMore:    hasMore,
	}, nil
}

func (t *TypedTable[T]) findField(fieldName string) (embedcore.FieldOffset, error) {
	for _, f := range t.layout.FieldOffsets {
		if f.Name == fieldName {
			return f, nil
		}
	}
	return embedcore.FieldOffset{}, fmt.Errorf("field %s not found", fieldName)
}

func compareValues(a, b interface{}) int {
	aVal := reflect.ValueOf(a)

	switch aVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		aInt := aVal.Int()
		bInt := toInt64(b)
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		aUint := aVal.Uint()
		bUint := toUint64(b)
		if aUint < bUint {
			return -1
		} else if aUint > bUint {
			return 1
		}
		return 0
	case reflect.Float32, reflect.Float64:
		aFloat := aVal.Float()
		bFloat := toFloat64(b)
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	case reflect.String:
		aStr := aVal.String()
		bStr, _ := b.(string)
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}
	return 0
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	}
	return 0
}

func toUint64(v interface{}) uint64 {
	switch val := v.(type) {
	case int:
		return uint64(val)
	case int8:
		return uint64(val)
	case int16:
		return uint64(val)
	case int32:
		return uint64(val)
	case int64:
		return uint64(val)
	case uint:
		return uint64(val)
	case uint8:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint32:
		return uint64(val)
	case uint64:
		return val
	case float32:
		return uint64(val)
	case float64:
		return uint64(val)
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	}
	return 0
}

func (t *TypedTable[T]) All() ([]T, error) {
	scanner := t.ScanRecords()
	defer scanner.Close()

	results := make([]T, 0)

	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}
		results = append(results, *record)
	}

	return results, scanner.Err()
}

func (t *TypedTable[T]) Filter(fn func(T) bool) ([]T, error) {
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

func (t *TypedTable[T]) Scan(fn func(T) bool) error {
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

type TypedScanner[T any] struct {
	table     *TypedTable[T]
	indexSnap []uint32
	indexKeys [][]byte
	pos       int
	current   *T
	err       error
}

func (t *TypedTable[T]) ScanRecords() *TypedScanner[T] {
	t.db.mu.RLock()
	keys := make([][]byte, 0)
	t.db.pkIndex.Range(func(k, v []byte) bool {
		if len(k) >= 2 && k[0] == t.tableID {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keys = append(keys, keyCopy)
		}
		return true
	})
	t.db.mu.RUnlock()

	return &TypedScanner[T]{
		table:     t,
		indexSnap: nil,
		indexKeys: keys,
		pos:       0,
	}
}

func (s *TypedScanner[T]) Next() bool {
	if s.err != nil || s.pos >= len(s.indexKeys) {
		return false
	}

	encodedPK := s.indexKeys[s.pos][1:]
	var pkVal any
	switch s.table.layout.PKType {
	case reflect.String:
		pkVal, _, _ = embedcore.DecodeString(encodedPK)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val, _, _ := embedcore.DecodeVarint(encodedPK)
		pkVal = int64(val)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		val, _, _ := embedcore.DecodeUvarint(encodedPK)
		pkVal = uint64(val)
	default:
		val, _, _ := embedcore.DecodeUvarint(encodedPK)
		pkVal = uint64(val)
	}

	record, err := s.table.Get(pkVal)
	if err != nil {
		s.pos++
		return s.Next()
	}

	s.current = record
	s.pos++
	return true
}

func (s *TypedScanner[T]) Record() (*T, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.current, nil
}

func (s *TypedScanner[T]) Err() error {
	return s.err
}

func (s *TypedScanner[T]) Close() {
	s.indexKeys = nil
}

func (t *TypedTable[T]) Count() int {
	count := 0
	t.db.pkIndex.Range(func(k, v []byte) bool {
		if len(k) >= 2 && k[0] == t.tableID {
			count++
		}
		return true
	})
	return count
}

func (t *TypedTable[T]) CreateIndex(fieldName string) error {
	if t.idxMgr == nil {
		t.idxMgr = newTypedIndexManager[T](t.db, t.layout)
	}
	return t.idxMgr.CreateIndex(fieldName)
}

func (t *TypedTable[T]) DropIndex(fieldName string) error {
	if t.idxMgr == nil {
		return nil
	}
	return t.idxMgr.DropIndex(fieldName)
}

func (t *TypedTable[T]) Drop() error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	entry, ok := t.db.tableCat[t.name]
	if !ok {
		return fmt.Errorf("table not found")
	}

	entry.Dropped = true

	var keys [][]byte
	t.db.pkIndex.Range(func(k, v []byte) bool {
		if len(k) >= 2 && k[0] == t.tableID {
			keys = append(keys, k)
		}
		return true
	})
	for _, k := range keys {
		t.db.pkIndex.Delete(k)
	}

	return nil
}

func (t *TypedTable[T]) GetIndexedFields() []string {
	if t.idxMgr == nil {
		return nil
	}
	return t.idxMgr.GetIndexedFields()
}

func (t *TypedTable[T]) Name() string {
	return t.name
}

func (t *TypedTable[T]) QueryPaged(fieldName string, value interface{}, offset, limit int) (*PagedResult[T], error) {
	all, err := t.Query(fieldName, value)
	if err != nil {
		field, fieldErr := t.findField(fieldName)
		if fieldErr != nil {
			return nil, err
		}
		comparator := func(fieldVal interface{}) bool {
			return compareValues(fieldVal, value) == 0
		}
		return t.filterPagedByField(field, comparator, offset, limit)
	}
	return paginateResults(all, offset, limit), nil
}

func (t *TypedTable[T]) QueryRangeGreaterThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	return nil, errors.New("QueryRangeGreaterThanPaged not yet implemented")
}

func (t *TypedTable[T]) QueryRangeLessThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	return nil, errors.New("QueryRangeLessThanPaged not yet implemented")
}

func (t *TypedTable[T]) QueryRangeBetweenPaged(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool, offset, limit int) (*PagedResult[T], error) {
	return nil, errors.New("QueryRangeBetweenPaged not yet implemented")
}

func (t *TypedTable[T]) FilterPaged(fn func(T) bool, offset, limit int) (*PagedResult[T], error) {
	scanner := t.ScanRecords()
	defer scanner.Close()

	var results []T
	totalCount := 0
	skipped := 0

	for scanner.Next() {
		record, err := scanner.Record()
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

func (t *TypedTable[T]) AllPaged(offset, limit int) (*PagedResult[T], error) {
	all, err := t.All()
	if err != nil {
		return nil, err
	}
	return paginateResults(all, offset, limit), nil
}

type PagedResult[T any] struct {
	Records    []T
	TotalCount int
	HasMore    bool
	Offset     int
	Limit      int
}

func paginateResults[T any](all []T, offset, limit int) *PagedResult[T] {
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

func (t *TypedTable[T]) decodeRecord(data []byte, result *T) error {
	data = data[12:]
	data = data[:len(data)-2]

	for len(data) > 0 {
		if len(data) < 2 {
			break
		}

		fieldKey := data[0]
		if data[1] != valueStartMarker {
			break
		}

		data = data[2:]

		fieldOffset, exists := t.layout.FieldOffsets[fieldKey]
		if !exists {
			endIdx := bytes.IndexByte(data, valueEndMarker)
			if endIdx == -1 {
				break
			}
			data = data[endIdx+1:]
			continue
		}

		var val interface{}
		var err error

		switch fieldOffset.Type {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			val, data, err = embedcore.DecodeVarint(data)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val, data, err = embedcore.DecodeUvarint(data)
		case reflect.String:
			val, data, err = embedcore.DecodeString(data)
		case reflect.Bool:
			val, data, err = embedcore.DecodeBool(data)
		case reflect.Float64:
			val, data, err = embedcore.DecodeFloat64(data)
		case reflect.Float32:
			var v float64
			v, data, err = embedcore.DecodeFloat64(data)
			if err == nil {
				val = float32(v)
			}
		default:
			endIdx := bytes.IndexByte(data, valueEndMarker)
			if endIdx == -1 {
				break
			}
			data = data[endIdx+1:]
			continue
		}

		if err != nil {
			endIdx := bytes.IndexByte(data, valueEndMarker)
			if endIdx == -1 {
				break
			}
			data = data[endIdx+1:]
			continue
		}

		endIdx := bytes.IndexByte(data, valueEndMarker)
		if endIdx != -1 {
			data = data[endIdx+1:]
		}

		embedcore.SetFieldValue(result, fieldOffset, val)
	}
	return nil
}

func (t *TypedTable[T]) encodeRecord(record *T) ([]byte, error) {
	var buf []byte
	for key, field := range t.layout.FieldOffsets {
		if field.IsStruct && !field.IsTime {
			continue
		}

		buf = append(buf, key, valueStartMarker)

		val, _ := embedcore.GetFieldValue(record, field)
		switch v := val.(type) {
		case int:
			buf = embedcore.EncodeVarint(buf, int64(v))
		case int8:
			buf = embedcore.EncodeVarint(buf, int64(v))
		case int16:
			buf = embedcore.EncodeVarint(buf, int64(v))
		case int32:
			buf = embedcore.EncodeVarint(buf, int64(v))
		case int64:
			buf = embedcore.EncodeVarint(buf, v)
		case uint:
			buf = embedcore.EncodeUvarint(buf, uint64(v))
		case uint8:
			buf = embedcore.EncodeUvarint(buf, uint64(v))
		case uint16:
			buf = embedcore.EncodeUvarint(buf, uint64(v))
		case uint32:
			buf = embedcore.EncodeUvarint(buf, uint64(v))
		case uint64:
			buf = embedcore.EncodeUvarint(buf, v)
		case string:
			buf = embedcore.EncodeString(buf, v)
		case bool:
			buf = embedcore.EncodeBool(buf, v)
		case float64:
			buf = embedcore.EncodeFloat64(buf, v)
		case float32:
			buf = embedcore.EncodeFloat64(buf, float64(v))
		case time.Time:
			buf = embedcore.EncodeVarint(buf, v.Unix())
		default:
			buf = buf[:len(buf)-2]
			continue
		}

		buf = append(buf, valueEndMarker)
	}
	return buf, nil
}
