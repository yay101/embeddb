package embeddb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sort"
	"strconv"
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
	Migrate       bool
	AutoIndex     bool
	SyncThreshold uint64
	IdleThreshold time.Duration
}

type UseOptions struct {
	MaxVersions uint8
}

type VersionMetadata struct {
	Version   uint32
	CreatedAt time.Time
}

type DB struct {
	filename      string
	migrate       bool
	autoIndex     bool
	syncThreshold uint64
	idleThreshold time.Duration
	writeCount    uint64
	lastSync      time.Time
	idleTimer     *time.Timer
	lock          sync.Mutex
	closed        bool
	database      *database
	tables        map[string]*database
}

func Open(filename string, opts ...OpenOptions) (*DB, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename is required")
	}

	migrate := true
	autoIndex := true
	syncThreshold := uint64(1000)
	idleThreshold := 10 * time.Second
	if len(opts) > 0 {
		migrate = opts[0].Migrate
		autoIndex = opts[0].AutoIndex
		if opts[0].SyncThreshold > 0 {
			syncThreshold = opts[0].SyncThreshold
		}
		if opts[0].IdleThreshold > 0 {
			idleThreshold = opts[0].IdleThreshold
		}
	}

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("failed to close file handle: %w", err)
	}

	return &DB{
		filename:      filename,
		migrate:       migrate,
		autoIndex:     autoIndex,
		syncThreshold: syncThreshold,
		idleThreshold: idleThreshold,
		writeCount:    0,
		lastSync:      time.Now(),
		tables:        make(map[string]*database),
	}, nil
}

func Use[T any](db *DB, args ...any) (*Table[T], error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}

	var tableName string
	var useOpts UseOptions

	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			tableName = v
		case UseOptions:
			useOpts = v
		default:
			return nil, fmt.Errorf("invalid argument type: %T", arg)
		}
	}

	if tableName == "" {
		tableName = resolveTableName[T]()
	}

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
		return &Table[T]{
			db:          existing,
			name:        tableName,
			tableID:     existingTable.ID,
			layout:      layout,
			maxVersions: existingTable.MaxVersions,
		}, nil
	}

	var typedDB *database
	if db.database != nil {
		typedDB = db.database
	} else {
		var err error
		typedDB, err = openDatabase(db.filename, false, db)
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
			RecordCount:  0,
			MaxVersions:  useOpts.MaxVersions,
		}
		typedDB.tableCat[tableName] = table
	} else if table.LayoutHash != layout.Hash {
		if db.migrate {
			if err := migrateTable(typedDB, table, layout); err != nil {
				typedDB.mu.Unlock()
				return nil, fmt.Errorf("migration failed: %w", err)
			}
		}
		table.LayoutHash = layout.Hash
	}
	typedDB.mu.Unlock()

	typedTable := &Table[T]{
		db:          typedDB,
		name:        tableName,
		tableID:     table.ID,
		layout:      layout,
		maxVersions: table.MaxVersions,
	}

	if db.autoIndex {
		typedTable.idxMgr = newIndexManager[T](typedDB, layout)
		for _, field := range layout.FieldOffsets {
			if field.Name != "" && !field.Primary && field.Offset > 0 {
				_ = typedTable.idxMgr.CreateIndex(field.Name)
			}
		}
		typedTable.rebuildSecondaryIndexes()
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
		return t.flush()
	}
	return nil
}

func (db *DB) FastSync() error {
	if db == nil {
		return nil
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	if db.closed {
		return fmt.Errorf("database is closed")
	}

	for _, t := range db.tables {
		if err := t.file.Sync(); err != nil {
			return err
		}
	}

	db.writeCount = 0
	db.lastSync = time.Now()

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

type Table[T any] struct {
	db          *database
	name        string
	tableID     uint8
	layout      *embedcore.StructLayout
	idxMgr      *indexManager[T]
	maxVersions uint8
}

type indexManager[T any] struct {
	db         *database
	layout     *embedcore.StructLayout
	indexes    map[string]uint32IndexInterface
	fieldCache map[string]embedcore.FieldOffset
}

func newIndexManager[T any](db *database, layout *embedcore.StructLayout) *indexManager[T] {
	fieldCache := make(map[string]embedcore.FieldOffset)
	for _, f := range layout.FieldOffsets {
		fieldCache[f.Name] = f
	}
	return &indexManager[T]{
		db:         db,
		layout:     layout,
		indexes:    make(map[string]uint32IndexInterface),
		fieldCache: fieldCache,
	}
}

func (im *indexManager[T]) CreateIndex(fieldName string) error {
	for _, field := range im.layout.FieldOffsets {
		if field.Name == fieldName {
			im.indexes[fieldName] = newUint32MapIndex()
			return nil
		}
	}
	return fmt.Errorf("field %s not found", fieldName)
}

func (im *indexManager[T]) DropIndex(fieldName string) error {
	delete(im.indexes, fieldName)
	return nil
}

func (im *indexManager[T]) HasIndex(fieldName string) bool {
	_, ok := im.indexes[fieldName]
	return ok
}

func (im *indexManager[T]) GetIndexedFields() []string {
	fields := make([]string, 0, len(im.indexes))
	for f := range im.indexes {
		fields = append(fields, f)
	}
	return fields
}

func (im *indexManager[T]) InsertIntoIndexes(record *T, recordID uint32) error {
	for fieldName, idx := range im.indexes {
		field, found := im.fieldCache[fieldName]
		if !found {
			continue
		}
		if field.IsSlice {
			im.indexSliceElement(record, field, idx, recordID)
			continue
		}
		key := embedcore.GetFieldAsString(record, field)
		if key == "" {
			continue
		}
		idx.Set(key, recordID)
	}
	return nil
}

func (im *indexManager[T]) indexSliceElement(record *T, field embedcore.FieldOffset, idx uint32IndexInterface, recordID uint32) {
	v := reflect.ValueOf(record).Elem()
	for _, parentName := range field.Parent {
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).Name == parentName {
				v = v.Field(i)
				break
			}
		}
	}
	v = v.FieldByName(field.Name)
	if !v.IsValid() || v.Kind() != reflect.Slice {
		return
	}
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		key := ""
		switch elem.Kind() {
		case reflect.String:
			key = elem.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			key = strconv.FormatInt(elem.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			key = strconv.FormatUint(elem.Uint(), 10)
		case reflect.Float32:
			key = strconv.FormatFloat(elem.Float(), 'f', -1, 32)
		case reflect.Float64:
			key = strconv.FormatFloat(elem.Float(), 'f', -1, 64)
		}
		if key != "" {
			idx.Set(key, recordID)
		}
	}
}

func (im *indexManager[T]) UpdateIndexes(record *T, recordID uint32) error {
	for fieldName, idx := range im.indexes {
		field, found := im.fieldCache[fieldName]
		if !found {
			continue
		}
		if field.IsSlice {
			im.indexSliceElement(record, field, idx, recordID)
			continue
		}
		key := embedcore.GetFieldAsString(record, field)
		if key == "" {
			continue
		}
		idx.Set(key, recordID)
	}
	return nil
}

func (im *indexManager[T]) Query(fieldName string, value interface{}) ([]uint32, error) {
	idx, ok := im.indexes[fieldName]
	if !ok {
		return nil, fmt.Errorf("no index for field %s", fieldName)
	}
	key := fmt.Sprintf("%v", value)
	ids, ok := idx.GetAll(key)
	if !ok {
		return []uint32{}, nil
	}
	return ids, nil
}

func parseIndexKey(key string, fieldKind reflect.Kind) interface{} {
	switch fieldKind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v, err := strconv.ParseInt(key, 10, 64); err == nil {
			return v
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v, err := strconv.ParseUint(key, 10, 64); err == nil {
			return v
		}
	case reflect.Float32, reflect.Float64:
		if v, err := strconv.ParseFloat(key, 64); err == nil {
			return v
		}
	case reflect.String:
		return key
	case reflect.Bool:
		if v, err := strconv.ParseBool(key); err == nil {
			return v
		}
	}
	return key
}

func valueToIndexKey(val any, fieldKind reflect.Kind) string {
	switch v := val.(type) {
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	}
	return fmt.Sprintf("%v", val)
}

func (im *indexManager[T]) getFieldType(fieldName string) reflect.Kind {
	if f, ok := im.fieldCache[fieldName]; ok {
		return f.Type
	}
	return reflect.Invalid
}

func (im *indexManager[T]) QueryRangeGreaterThan(fieldName string, value interface{}, inclusive bool) ([]uint32, error) {
	idx, ok := im.indexes[fieldName]
	if !ok {
		return nil, fmt.Errorf("no index for field %s", fieldName)
	}

	fieldType := im.getFieldType(fieldName)
	keys := idx.SortedKeys()

	var results []uint32
	for _, k := range keys {
		parsedVal := parseIndexKey(k, fieldType)
		cmp := compareValues(parsedVal, value)
		if cmp > 0 || (inclusive && cmp == 0) {
			if ids, ok := idx.GetAll(k); ok {
				results = append(results, ids...)
			}
		}
	}
	if results == nil {
		return []uint32{}, nil
	}
	return results, nil
}

func (im *indexManager[T]) QueryRangeLessThan(fieldName string, value interface{}, inclusive bool) ([]uint32, error) {
	idx, ok := im.indexes[fieldName]
	if !ok {
		return nil, fmt.Errorf("no index for field %s", fieldName)
	}

	fieldType := im.getFieldType(fieldName)
	keys := idx.SortedKeys()

	var results []uint32
	for _, k := range keys {
		parsedVal := parseIndexKey(k, fieldType)
		cmp := compareValues(parsedVal, value)
		if cmp < 0 || (inclusive && cmp == 0) {
			if ids, ok := idx.GetAll(k); ok {
				results = append(results, ids...)
			}
		}
	}
	if results == nil {
		return []uint32{}, nil
	}
	return results, nil
}

func (im *indexManager[T]) QueryRangeBetween(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]uint32, error) {
	idx, ok := im.indexes[fieldName]
	if !ok {
		return nil, fmt.Errorf("no index for field %s", fieldName)
	}

	fieldType := im.getFieldType(fieldName)
	keys := idx.SortedKeys()

	var results []uint32
	for _, k := range keys {
		parsedVal := parseIndexKey(k, fieldType)
		cMin := compareValues(parsedVal, min)
		cMax := compareValues(parsedVal, max)
		aboveMin := cMin > 0 || (inclusiveMin && cMin == 0)
		belowMax := cMax < 0 || (inclusiveMax && cMax == 0)
		if aboveMin && belowMax {
			if ids, ok := idx.GetAll(k); ok {
				results = append(results, ids...)
			}
		}
	}
	if results == nil {
		return []uint32{}, nil
	}
	return results, nil
}

func (t *Table[T]) Insert(record *T) (uint32, error) {
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
	} else {
		indexKey := encodePKForIndex(t.tableID, pkVal)
		if _, exists := t.db.pkIndex.Get(indexKey); exists {
			return 0, fmt.Errorf("primary key already exists: %v", pkVal)
		}
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

	if t.maxVersions > 0 {
		t.db.versionIndex.Add(indexKey, 1, offset, time.Now().UnixNano())
	}

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]++
	} else {
		entry.RecordCount++
	}

	if t.idxMgr != nil {
		t.idxMgr.InsertIntoIndexes(record, recordID)
	}

	t.db.autoSync()

	return recordID, nil
}

func (t *Table[T]) isZeroPK(val any) bool {
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

func (t *Table[T]) setPKValue(record *T, val any) {
	for _, field := range t.layout.FieldOffsets {
		if field.Primary {
			embedcore.SetFieldValue(record, field, val)
			return
		}
	}
}

func (t *Table[T]) getPKValue(record *T) (any, error) {
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

func (t *Table[T]) Get(id any) (*T, error) {
	t.db.mu.RLock()
	defer t.db.mu.RUnlock()

	indexKey := encodePKForIndex(t.tableID, id)

	val, ok := t.db.pkIndex.Get(indexKey)
	if !ok {
		return nil, fmt.Errorf("record not found")
	}

	offset := binary.BigEndian.Uint64(val)

	headerBuf := make([]byte, 12)
	_, err := t.db.file.ReadAt(headerBuf, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("read error at offset %d: %v", offset, err)
	}

	if headerBuf[0] != EcCode || headerBuf[1] != RecordStartMark {
		return nil, fmt.Errorf("invalid record header")
	}

	active := headerBuf[11]
	if active != 1 {
		return nil, fmt.Errorf("record is deleted")
	}

	recLen := binary.BigEndian.Uint32(headerBuf[7:11])
	totalLen := 12 + int(recLen) + 2

	recordBuf := make([]byte, totalLen)
	copy(recordBuf, headerBuf)
	n, err := t.db.file.ReadAt(recordBuf[12:], int64(offset)+12)
	if err != nil || n < int(totalLen-12) {
		return nil, fmt.Errorf("read error at offset %d: %v", offset, err)
	}

	var result T
	if err := t.decodeRecord(recordBuf, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (t *Table[T]) queryByPK(pkValue string) (uint32, bool) {
	if t.idxMgr == nil {
		return 0, false
	}
	for fieldName, idx := range t.idxMgr.indexes {
		if f, ok := t.idxMgr.fieldCache[fieldName]; !ok || !f.Primary {
			continue
		}
		id, ok := idx.Get(pkValue)
		return id, ok
	}
	return 0, false
}

func (t *Table[T]) GetVersion(id any, version uint32) (*T, error) {
	if t.maxVersions == 0 {
		return nil, fmt.Errorf("versioning not enabled for this table")
	}

	t.db.mu.RLock()
	defer t.db.mu.RUnlock()

	indexKey := encodePKForIndex(t.tableID, id)
	versionInfo, ok := t.db.versionIndex.GetVersion(indexKey, version)
	if !ok {
		return nil, fmt.Errorf("version %d not found for record", version)
	}

	offset := versionInfo.Offset

	headerBuf := make([]byte, 12)
	_, err := t.db.file.ReadAt(headerBuf, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("read error at offset %d: %v", offset, err)
	}

	if headerBuf[0] != EcCode || headerBuf[1] != RecordStartMark {
		return nil, fmt.Errorf("invalid record header")
	}

	active := headerBuf[11]
	if active != 1 {
		return nil, fmt.Errorf("record version is deleted")
	}

	recLen := binary.BigEndian.Uint32(headerBuf[7:11])
	totalLen := 12 + int(recLen) + 2

	recordBuf := make([]byte, totalLen)
	copy(recordBuf, headerBuf)
	n, err := t.db.file.ReadAt(recordBuf[12:], int64(offset)+12)
	if err != nil || n < int(totalLen-12) {
		return nil, fmt.Errorf("read error at offset %d: %v", offset, err)
	}

	var result T
	if err := t.decodeRecord(recordBuf, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (t *Table[T]) ListVersions(id any) ([]VersionMetadata, error) {
	if t.maxVersions == 0 {
		return nil, fmt.Errorf("versioning not enabled for this table")
	}

	t.db.mu.RLock()
	defer t.db.mu.RUnlock()

	indexKey := encodePKForIndex(t.tableID, id)
	versions := t.db.versionIndex.GetVersions(indexKey)

	result := make([]VersionMetadata, 0, len(versions))
	for _, v := range versions {
		result = append(result, VersionMetadata{
			Version:   v.Version,
			CreatedAt: time.Unix(0, v.CreatedAt),
		})
	}

	return result, nil
}

func (t *Table[T]) Update(id any, record *T) error {
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

	recordID := binary.BigEndian.Uint32(recordBuf[3:7])

	encoded, err := t.encodeRecord(record)
	if err != nil {
		return fmt.Errorf("failed to encode record: %v", err)
	}

	headerBytes := make([]byte, 12)
	headerBytes[0] = EcCode
	headerBytes[1] = RecordStartMark
	headerBytes[2] = t.tableID
	binary.BigEndian.PutUint32(headerBytes[3:7], recordID)
	binary.BigEndian.PutUint32(headerBytes[7:11], uint32(len(encoded)))
	headerBytes[11] = 1

	footerBytes := []byte{EcCode, RecordEndMark}

	newRecordBuf := make([]byte, 0, len(headerBytes)+len(encoded)+len(footerBytes))
	newRecordBuf = append(newRecordBuf, headerBytes...)
	newRecordBuf = append(newRecordBuf, encoded...)
	newRecordBuf = append(newRecordBuf, footerBytes...)

	newOffset, _ := t.db.alloc.Allocate(uint64(len(newRecordBuf)))
	t.db.file.WriteAt(newRecordBuf, int64(newOffset))

	if t.maxVersions > 0 {
		versions := t.db.versionIndex.GetVersions(indexKey)
		if len(versions) == 0 {
			t.db.versionIndex.Add(indexKey, 1, oldOffset, time.Now().UnixNano())
			versions = []VersionInfo{{Version: 1, Offset: oldOffset, CreatedAt: time.Now().UnixNano()}}
		}
		newVersion := uint32(1)
		for _, v := range versions {
			if v.Version >= newVersion {
				newVersion = v.Version + 1
			}
		}
		t.db.versionIndex.Add(indexKey, newVersion, newOffset, time.Now().UnixNano())
		versions = append(versions, VersionInfo{Version: newVersion, Offset: newOffset, CreatedAt: time.Now().UnixNano()})
		maxTotal := int(t.maxVersions) + 1
		for len(versions) > maxTotal {
			oldestIdx := 0
			oldestVersion := versions[0].Version
			for i, v := range versions {
				if v.Version < versions[oldestIdx].Version {
					oldestIdx = i
					oldestVersion = v.Version
				}
			}
			t.db.file.WriteAt([]byte{0}, int64(versions[oldestIdx].Offset+11))
			t.db.versionIndex.RemoveVersion(indexKey, oldestVersion)
			versions = append(versions[:oldestIdx], versions[oldestIdx+1:]...)
		}
	} else {
		t.db.file.WriteAt([]byte{0}, int64(oldOffset+11))
	}

	newVal := make([]byte, 8)
	binary.BigEndian.PutUint64(newVal, newOffset)
	t.db.pkIndex.Set(indexKey, newVal)

	t.db.autoSync()

	return nil
}

func (t *Table[T]) Delete(id any) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	entry := t.db.tableCat[t.name]

	indexKey := encodePKForIndex(t.tableID, id)
	val, ok := t.db.pkIndex.Get(indexKey)
	if !ok {
		return fmt.Errorf("record not found")
	}

	offset := binary.BigEndian.Uint64(val)

	t.db.file.WriteAt([]byte{0}, int64(offset+11))

	t.db.pkIndex.Delete(indexKey)

	if t.maxVersions > 0 {
		t.db.versionIndex.RemoveKey(indexKey)
	}

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]--
	} else {
		entry.RecordCount--
	}

	return nil
}

func (t *Table[T]) DeleteMany(ids []any) (int, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	entry := t.db.tableCat[t.name]

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
		entry.RecordCount--
	}
	return deleted, nil
}

func (t *Table[T]) InsertMany(records []*T) ([]uint32, error) {
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

	t.db.autoSync()

	return ids, nil
}

func (t *Table[T]) UpdateMany(ids []any, updater func(*T) error) (int, error) {
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

func (t *Table[T]) insertLocked(record *T) (uint32, error) {
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
	} else {
		indexKey := encodePKForIndex(t.tableID, pkVal)
		if _, exists := t.db.pkIndex.Get(indexKey); exists {
			return 0, fmt.Errorf("primary key already exists: %v", pkVal)
		}
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

	if t.maxVersions > 0 {
		t.db.versionIndex.Add(indexKey, 1, offset, time.Now().UnixNano())
	}

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]++
	} else {
		entry.RecordCount++
	}

	if t.idxMgr != nil {
		t.idxMgr.InsertIntoIndexes(record, recordID)
	}

	return recordID, nil
}

func (t *Table[T]) getLocked(id any) (*T, error) {
	indexKey := encodePKForIndex(t.tableID, id)
	val, ok := t.db.pkIndex.Get(indexKey)
	if !ok {
		return nil, fmt.Errorf("record not found")
	}

	offset := binary.BigEndian.Uint64(val)

	headerBuf := make([]byte, 12)
	_, err := t.db.file.ReadAt(headerBuf, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("read error at offset %d: %v", offset, err)
	}

	if headerBuf[0] != EcCode || headerBuf[1] != RecordStartMark {
		return nil, fmt.Errorf("invalid record header")
	}

	active := headerBuf[11]
	if active != 1 {
		return nil, fmt.Errorf("record is deleted")
	}

	recLen := binary.BigEndian.Uint32(headerBuf[7:11])
	totalLen := 12 + int(recLen) + 2

	recordBuf := make([]byte, totalLen)
	copy(recordBuf, headerBuf)
	n, err := t.db.file.ReadAt(recordBuf[12:], int64(offset)+12)
	if err != nil || n < int(totalLen-12) {
		return nil, fmt.Errorf("read error at offset %d: %v", offset, err)
	}

	var result T
	if err := t.decodeRecord(recordBuf, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (t *Table[T]) updateLocked(id any, record *T) error {
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

func (t *Table[T]) Upsert(id any, record *T) (uint32, bool, error) {
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

func (t *Table[T]) Query(fieldName string, value interface{}) ([]T, error) {
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

func (t *Table[T]) QueryRangeGreaterThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	if t.idxMgr != nil && t.idxMgr.HasIndex(fieldName) {
		recordIDs, err := t.idxMgr.QueryRangeGreaterThan(fieldName, value, inclusive)
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

	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) > 0 || (inclusive && compareValues(fieldValue, value) == 0)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryRangeLessThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	if t.idxMgr != nil && t.idxMgr.HasIndex(fieldName) {
		recordIDs, err := t.idxMgr.QueryRangeLessThan(fieldName, value, inclusive)
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

	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) < 0 || (inclusive && compareValues(fieldValue, value) == 0)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryRangeBetween(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]T, error) {
	if t.idxMgr != nil && t.idxMgr.HasIndex(fieldName) {
		recordIDs, err := t.idxMgr.QueryRangeBetween(fieldName, min, max, inclusiveMin, inclusiveMax)
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

	comparator := func(fieldValue interface{}) bool {
		cMin := compareValues(fieldValue, min)
		cMax := compareValues(fieldValue, max)
		aboveMin := cMin > 0 || (inclusiveMin && cMin == 0)
		belowMax := cMax < 0 || (inclusiveMax && cMax == 0)
		return aboveMin && belowMax
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryNotEqual(fieldName string, value interface{}) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) != 0
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryGreaterOrEqual(fieldName string, value interface{}) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) >= 0
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryLessOrEqual(fieldName string, value interface{}) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) <= 0
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryLike(fieldName string, pattern string) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		str, ok := fieldValue.(string)
		if !ok {
			return false
		}
		return matchLike(str, pattern)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryNotLike(fieldName string, pattern string) ([]T, error) {
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

func (t *Table[T]) filterByField(fieldName string, fn func(interface{}) bool) ([]T, error) {
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

func (t *Table[T]) filterPagedByField(field embedcore.FieldOffset, fn func(interface{}) bool, offset, limit int) (*PagedResult[T], error) {
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

func (t *Table[T]) findField(fieldName string) (embedcore.FieldOffset, error) {
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

func (t *Table[T]) All() ([]T, error) {
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

type Scanner[T any] struct {
	table     *Table[T]
	indexSnap []uint32
	indexKeys [][]byte
	pos       int
	current   *T
	err       error
}

func (t *Table[T]) ScanRecords() *Scanner[T] {
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

	return &Scanner[T]{
		table:     t,
		indexSnap: nil,
		indexKeys: keys,
		pos:       0,
	}
}

func (s *Scanner[T]) Next() bool {
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

func (s *Scanner[T]) Record() (*T, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.current, nil
}

func (s *Scanner[T]) Err() error {
	return s.err
}

func (s *Scanner[T]) Close() {
	s.indexKeys = nil
}

func (t *Table[T]) Count() int {
	entry := t.db.tableCat[t.name]
	if entry != nil && entry.RecordCount > 0 {
		return int(entry.RecordCount)
	}
	count := 0
	t.db.pkIndex.Range(func(k, v []byte) bool {
		if len(k) >= 2 && k[0] == t.tableID {
			count++
		}
		return true
	})
	return count
}

func (t *Table[T]) CreateIndex(fieldName string) error {
	if t.idxMgr == nil {
		t.idxMgr = newIndexManager[T](t.db, t.layout)
	}
	return t.idxMgr.CreateIndex(fieldName)
}

func (t *Table[T]) rebuildSecondaryIndexes() {
	if t.idxMgr == nil {
		return
	}

	t.db.pkIndex.Range(func(k []byte, v []byte) bool {
		if len(k) < 2 || k[0] != t.tableID {
			return true
		}

		var recordID uint32
		switch t.layout.PKType {
		case reflect.String:
			_, idStr, _ := embedcore.DecodeString(k[1:])
			if len(idStr) >= 4 {
				recordID = binary.BigEndian.Uint32(idStr[:4])
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			val, _, _ := embedcore.DecodeVarint(k[1:])
			recordID = uint32(int64(val))
		default:
			val, _, _ := embedcore.DecodeUvarint(k[1:])
			recordID = uint32(val)
		}

		if recordID == 0 {
			return true
		}

		record, err := t.Get(recordID)
		if err != nil {
			return true
		}

		t.idxMgr.InsertIntoIndexes(record, recordID)
		return true
	})
}

func (t *Table[T]) DropIndex(fieldName string) error {
	if t.idxMgr == nil {
		return nil
	}
	return t.idxMgr.DropIndex(fieldName)
}

func (t *Table[T]) Drop() error {
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

	entry.RecordCount = 0

	return nil
}

func (t *Table[T]) GetIndexedFields() []string {
	if t.idxMgr == nil {
		return nil
	}
	return t.idxMgr.GetIndexedFields()
}

func (t *Table[T]) Name() string {
	return t.name
}

func (t *Table[T]) QueryPaged(fieldName string, value interface{}, offset, limit int) (*PagedResult[T], error) {
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

func (t *Table[T]) QueryRangeGreaterThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	all, err := t.QueryRangeGreaterThan(fieldName, value, inclusive)
	if err != nil {
		return nil, err
	}
	return paginateResults(all, offset, limit), nil
}

func (t *Table[T]) QueryRangeLessThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	all, err := t.QueryRangeLessThan(fieldName, value, inclusive)
	if err != nil {
		return nil, err
	}
	return paginateResults(all, offset, limit), nil
}

func (t *Table[T]) QueryRangeBetweenPaged(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool, offset, limit int) (*PagedResult[T], error) {
	all, err := t.QueryRangeBetween(fieldName, min, max, inclusiveMin, inclusiveMax)
	if err != nil {
		return nil, err
	}
	return paginateResults(all, offset, limit), nil
}

func (t *Table[T]) FilterPaged(fn func(T) bool, offset, limit int) (*PagedResult[T], error) {
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

func (t *Table[T]) AllPaged(offset, limit int) (*PagedResult[T], error) {
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

func (t *Table[T]) decodeRecord(data []byte, result *T) error {
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
		case reflect.Struct:
			if fieldOffset.IsTime {
				var unixVal int64
				unixVal, data, err = embedcore.DecodeVarint(data)
				if err == nil {
					val = time.Unix(unixVal, 0).UTC()
				}
			} else {
				endIdx := bytes.IndexByte(data, valueEndMarker)
				if endIdx == -1 {
					break
				}
				data = data[endIdx+1:]
				continue
			}
		case reflect.Slice:
			if fieldOffset.IsBytes {
				val, data, err = embedcore.DecodeBytes(data)
			} else if fieldOffset.IsSlice && fieldOffset.SliceElem.Kind() == reflect.String {
				val, data, err = embedcore.DecodeSlice(data)
			} else if fieldOffset.IsSlice && fieldOffset.SliceElem.Kind() == reflect.Int {
				val, data, err = embedcore.DecodeIntSlice(data)
			} else if fieldOffset.IsSlice && fieldOffset.SliceElem.Kind() == reflect.Struct {
				val, data, err = t.decodeSliceOfStructs(data, fieldOffset)
			} else {
				endIdx := bytes.IndexByte(data, valueEndMarker)
				if endIdx == -1 {
					break
				}
				data = data[endIdx+1:]
				continue
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

func (t *Table[T]) encodeRecord(record *T) ([]byte, error) {
	buf := make([]byte, 0, 64)

	keys := make([]byte, 0, len(t.layout.FieldOffsets))
	for k := range t.layout.FieldOffsets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, key := range keys {
		field := t.layout.FieldOffsets[key]
		if field.IsStruct && !field.IsTime && !field.IsSlice {
			continue
		}

		buf = append(buf, key, valueStartMarker)

		switch field.Type {
		case reflect.Int:
			buf = embedcore.EncodeVarint(buf, int64(embedcore.GetIntField(record, field)))
		case reflect.Int8:
			buf = embedcore.EncodeVarint(buf, int64(embedcore.GetInt8Field(record, field)))
		case reflect.Int16:
			buf = embedcore.EncodeVarint(buf, int64(embedcore.GetInt16Field(record, field)))
		case reflect.Int32:
			buf = embedcore.EncodeVarint(buf, int64(embedcore.GetInt32Field(record, field)))
		case reflect.Int64:
			buf = embedcore.EncodeVarint(buf, embedcore.GetInt64Field(record, field))
		case reflect.Uint:
			buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUintField(record, field)))
		case reflect.Uint8:
			buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUint8Field(record, field)))
		case reflect.Uint16:
			buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUint16Field(record, field)))
		case reflect.Uint32:
			buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUint32Field(record, field)))
		case reflect.Uint64:
			buf = embedcore.EncodeUvarint(buf, embedcore.GetUint64Field(record, field))
		case reflect.String:
			buf = embedcore.EncodeString(buf, embedcore.GetStringField(record, field))
		case reflect.Bool:
			buf = embedcore.EncodeBool(buf, embedcore.GetBoolField(record, field))
		case reflect.Float64:
			buf = embedcore.EncodeFloat64(buf, embedcore.GetFloat64Field(record, field))
		case reflect.Float32:
			buf = embedcore.EncodeFloat64(buf, float64(embedcore.GetFloat32Field(record, field)))
		case reflect.Struct:
			if field.IsTime {
				buf = embedcore.EncodeVarint(buf, embedcore.GetTimeField(record, field).Unix())
			}
		case reflect.Slice:
			if field.IsBytes {
				bytesVal, _ := embedcore.GetBytesField(record, field)
				buf = embedcore.EncodeBytes(buf, bytesVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.String {
				sliceVal := embedcore.GetStringSlice(record, field)
				buf = embedcore.EncodeSlice(buf, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int {
				sliceVal := embedcore.GetIntSlice(record, field)
				buf = embedcore.EncodeIntSlice(buf, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Struct {
				buf = t.encodeSliceOfStructs(record, field, buf)
			} else {
				buf = buf[:len(buf)-2]
				continue
			}
		default:
			buf = buf[:len(buf)-2]
			continue
		}

		buf = append(buf, valueEndMarker)
	}

	return t.encodeNestedStructs(record, buf)
}

func (t *Table[T]) encodeNestedStructs(record *T, buf []byte) ([]byte, error) {
	for _, field := range t.layout.FieldOffsets {
		// Skip non-struct fields or time fields
		if !field.IsStruct || field.IsTime {
			continue
		}
		// Also skip slices - they're handled by encodeSliceOfStructs
		if field.IsSlice {
			continue
		}

		parentPath := field.Parent
		if len(parentPath) == 0 {
			continue
		}

		for nestedKey, nestedField := range t.layout.FieldOffsets {
			if len(nestedField.Parent) != len(parentPath)+1 {
				continue
			}

			isChild := true
			for i, p := range parentPath {
				if nestedField.Parent[i] != p {
					isChild = false
					break
				}
			}
			if !isChild {
				continue
			}

			if nestedField.Type == reflect.Struct {
				continue
			}

			switch nestedField.Type {
			case reflect.Int:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeVarint(buf, int64(embedcore.GetIntField(record, nestedField)))
			case reflect.Int8:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeVarint(buf, int64(embedcore.GetInt8Field(record, nestedField)))
			case reflect.Int16:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeVarint(buf, int64(embedcore.GetInt16Field(record, nestedField)))
			case reflect.Int32:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeVarint(buf, int64(embedcore.GetInt32Field(record, nestedField)))
			case reflect.Int64:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeVarint(buf, embedcore.GetInt64Field(record, nestedField))
			case reflect.Uint:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUintField(record, nestedField)))
			case reflect.Uint8:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUint8Field(record, nestedField)))
			case reflect.Uint16:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUint16Field(record, nestedField)))
			case reflect.Uint32:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUint32Field(record, nestedField)))
			case reflect.Uint64:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeUvarint(buf, embedcore.GetUint64Field(record, nestedField))
			case reflect.String:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeString(buf, embedcore.GetStringField(record, nestedField))
			case reflect.Bool:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeBool(buf, embedcore.GetBoolField(record, nestedField))
			case reflect.Float64:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeFloat64(buf, embedcore.GetFloat64Field(record, nestedField))
			case reflect.Float32:
				buf = append(buf, nestedKey, valueStartMarker)
				buf = embedcore.EncodeFloat64(buf, float64(embedcore.GetFloat32Field(record, nestedField)))
			case reflect.Slice:
				if nestedField.IsSlice && nestedField.SliceElem.Kind() == reflect.String {
					buf = append(buf, nestedKey, valueStartMarker)
					sliceVal := embedcore.GetStringSlice(record, nestedField)
					buf = embedcore.EncodeSlice(buf, sliceVal)
				} else if nestedField.IsSlice && nestedField.SliceElem.Kind() == reflect.Int {
					buf = append(buf, nestedKey, valueStartMarker)
					sliceVal := embedcore.GetIntSlice(record, nestedField)
					buf = embedcore.EncodeIntSlice(buf, sliceVal)
				} else if nestedField.IsSlice && nestedField.SliceElem.Kind() == reflect.Struct {
					buf = append(buf, nestedKey, valueStartMarker)
					buf = t.encodeSliceOfStructs(record, nestedField, buf)
				} else {
					continue
				}
			default:
				continue
			}

			buf = append(buf, valueEndMarker)
		}
	}

	return buf, nil
}

func (t *Table[T]) encodeSliceOfStructs(record *T, field embedcore.FieldOffset, buf []byte) []byte {
	// Navigate to the slice value using reflection
	rootVal := reflect.ValueOf(record).Elem()

	// Navigate through parent path to get the nested struct if applicable
	var val reflect.Value
	if len(field.Parent) > 0 {
		val = rootVal
		for _, part := range field.Parent {
			val = val.FieldByName(part)
			if !val.IsValid() {
				return buf
			}
		}
		// Get the last part of the field name (the actual slice field)
		fieldParts := strings.Split(field.Name, ".")
		lastPart := fieldParts[len(fieldParts)-1]
		val = val.FieldByName(lastPart)
	} else {
		val = rootVal.FieldByName(field.Name)
	}

	if !val.IsValid() || val.IsNil() {
		return buf
	}

	numElems := val.Len()
	buf = embedcore.EncodeUvarint(buf, uint64(numElems))

	if numElems == 0 {
		return buf
	}

	elementType := field.SliceElem
	elemLayout, err := embedcore.ComputeStructLayout(reflect.New(elementType).Interface())
	if err != nil {
		return buf
	}

	for i := 0; i < numElems; i++ {
		elem := val.Index(i)
		elemPtr := elem.Addr().Interface()

		for key, f := range elemLayout.FieldOffsets {
			if f.Type == reflect.Struct {
				continue
			}

			elemKey := key + 128 // Offset element keys to avoid conflicts with parent
			buf = append(buf, elemKey, valueStartMarker)

			switch f.Type {
			case reflect.Int:
				buf = embedcore.EncodeVarint(buf, int64(embedcore.GetIntField(elemPtr, f)))
			case reflect.Int8:
				buf = embedcore.EncodeVarint(buf, int64(embedcore.GetInt8Field(elemPtr, f)))
			case reflect.Int16:
				buf = embedcore.EncodeVarint(buf, int64(embedcore.GetInt16Field(elemPtr, f)))
			case reflect.Int32:
				buf = embedcore.EncodeVarint(buf, int64(embedcore.GetInt32Field(elemPtr, f)))
			case reflect.Int64:
				buf = embedcore.EncodeVarint(buf, embedcore.GetInt64Field(elemPtr, f))
			case reflect.Uint:
				buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUintField(elemPtr, f)))
			case reflect.Uint8:
				buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUint8Field(elemPtr, f)))
			case reflect.Uint16:
				buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUint16Field(elemPtr, f)))
			case reflect.Uint32:
				buf = embedcore.EncodeUvarint(buf, uint64(embedcore.GetUint32Field(elemPtr, f)))
			case reflect.Uint64:
				buf = embedcore.EncodeUvarint(buf, embedcore.GetUint64Field(elemPtr, f))
			case reflect.String:
				buf = embedcore.EncodeString(buf, embedcore.GetStringField(elemPtr, f))
			case reflect.Bool:
				buf = embedcore.EncodeBool(buf, embedcore.GetBoolField(elemPtr, f))
			case reflect.Float64:
				buf = embedcore.EncodeFloat64(buf, embedcore.GetFloat64Field(elemPtr, f))
			case reflect.Float32:
				buf = embedcore.EncodeFloat64(buf, float64(embedcore.GetFloat32Field(elemPtr, f)))
			default:
				buf = buf[:len(buf)-2]
				continue
			}

			buf = append(buf, valueEndMarker)
		}

		buf = append(buf, embedcore.SliceElementMarker)
	}

	return buf
}

func (t *Table[T]) decodeSliceOfStructs(data []byte, fieldOffset embedcore.FieldOffset) (interface{}, []byte, error) {
	length, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, data, errors.New("invalid slice length")
	}
	data = data[n:]

	elementType := fieldOffset.SliceElem
	elemLayout, err := embedcore.ComputeStructLayout(reflect.New(elementType).Interface())
	if err != nil {
		return nil, data, err
	}

	result := reflect.MakeSlice(reflect.SliceOf(elementType), 0, int(length))

	for i := 0; i < int(length); i++ {
		elem := reflect.New(elementType)

		for len(data) > 0 {
			if data[0] == valueEndMarker {
				data = data[1:]
				break
			}
			if data[0] == embedcore.SliceElementMarker {
				data = data[1:]
				break
			}
			if len(data) < 2 {
				break
			}

			key := data[0]
			if data[1] != valueStartMarker {
				data = data[1:]
				continue
			}
			data = data[2:]

			adjustedKey := key - 128
			f, exists := elemLayout.FieldOffsets[adjustedKey]
			if !exists {
				endIdx := bytes.IndexByte(data, valueEndMarker)
				if endIdx == -1 {
					endIdx = bytes.IndexByte(data, embedcore.SliceElementMarker)
				}
				if endIdx == -1 {
					break
				}
				data = data[endIdx+1:]
				continue
			}

			var val interface{}
			var decodeErr error

			switch f.Type {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				val, data, decodeErr = embedcore.DecodeVarint(data)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				val, data, decodeErr = embedcore.DecodeUvarint(data)
			case reflect.String:
				val, data, decodeErr = embedcore.DecodeString(data)
			case reflect.Bool:
				val, data, decodeErr = embedcore.DecodeBool(data)
			case reflect.Float64:
				val, data, decodeErr = embedcore.DecodeFloat64(data)
			case reflect.Float32:
				var v float64
				v, data, decodeErr = embedcore.DecodeFloat64(data)
				if decodeErr == nil {
					val = float32(v)
				}
			default:
				endIdx := bytes.IndexByte(data, valueEndMarker)
				if endIdx == -1 {
					endIdx = bytes.IndexByte(data, embedcore.SliceElementMarker)
				}
				if endIdx == -1 {
					break
				}
				data = data[endIdx+1:]
				continue
			}

			if decodeErr != nil {
				endIdx := bytes.IndexByte(data, valueEndMarker)
				if endIdx == -1 {
					endIdx = bytes.IndexByte(data, embedcore.SliceElementMarker)
				}
				if endIdx == -1 {
					break
				}
				data = data[endIdx+1:]
				continue
			}

			endIdx := bytes.IndexByte(data, valueEndMarker)
			if endIdx == -1 {
				endIdx = bytes.IndexByte(data, embedcore.SliceElementMarker)
			}
			if endIdx != -1 {
				data = data[endIdx+1:]
			}

			embedcore.SetFieldValue(elem.Interface(), f, val)
		}

		result = reflect.Append(result, elem.Elem())
	}

	return result.Interface(), data, nil
}
