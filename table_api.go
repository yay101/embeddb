package embeddb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	embedcore "github.com/yay101/embeddbcore"
	"github.com/yay101/embeddbmmap"
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
		orphanedEntry := (*tableCatalogEntry)(nil)
		for _, entry := range typedDB.tableCat {
			if entry.Dropped {
				continue
			}
			if strings.HasPrefix(entry.Name, "table_") && entry.Name != tableName {
				orphanedEntry = entry
				break
			}
		}
		if orphanedEntry != nil {
			delete(typedDB.tableCat, orphanedEntry.Name)
			orphanedEntry.Name = tableName
			orphanedEntry.SchemaVersion = layout.SchemaVersion
			orphanedEntry.MaxVersions = useOpts.MaxVersions
			typedDB.tableCat[tableName] = orphanedEntry
			table = orphanedEntry
		} else {
			var maxID uint8
			for _, entry := range typedDB.tableCat {
				if entry.ID > maxID && !entry.Dropped {
					maxID = entry.ID
				}
			}
			maxID++
			table = &tableCatalogEntry{
				ID:            maxID,
				Name:          tableName,
				SchemaVersion: layout.SchemaVersion,
				NextRecordID:  1,
				RecordCount:   0,
				MaxVersions:   useOpts.MaxVersions,
			}
			typedDB.tableCat[tableName] = table
		}
	} else if table.SchemaVersion != layout.SchemaVersion {
		if db.migrate {
			if err := migrateTable(typedDB, table, layout); err != nil {
				typedDB.mu.Unlock()
				return nil, fmt.Errorf("migration failed: %w", err)
			}
			table.SchemaVersion = layout.SchemaVersion
		} else {
			typedDB.mu.Unlock()
			return nil, fmt.Errorf("schema mismatch for table %q: migration is disabled but struct layout has changed", tableName)
		}
	}
	typedDB.mu.Unlock()

	typedTable := &Table[T]{
		db:          typedDB,
		name:        tableName,
		tableID:     table.ID,
		layout:      layout,
		maxVersions: table.MaxVersions,
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

	var firstErr error
	for _, t := range db.tables {
		if err := t.flush(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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
		if r := t.region.Load(); r != nil {
			if err := r.Sync(embeddbmmap.SyncSync); err != nil {
				return err
			}
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

	var firstErr error
	for _, t := range db.tables {
		if err := t.Vacuum(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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
	maxVersions uint8
}

func (t *Table[T]) insertSecondaryKeys(record *T, offset uint64) {
	if t.db.parent == nil {
		return
	}
	if !t.db.parent.autoIndex {
		if t.db.explicitIndexes == nil {
			return
		}
		explicitFields := t.db.explicitIndexes[t.name]
		if len(explicitFields) == 0 {
			return
		}
		explicitSet := make(map[string]bool, len(explicitFields))
		for _, f := range explicitFields {
			explicitSet[f] = true
		}
		for _, field := range t.layout.Fields {
			if field.Name != "" && !field.Primary && field.Offset > 0 && !field.IsSlice && explicitSet[field.Name] {
				if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][field.Name] {
					continue
				}
				key := embedcore.GetFieldAsString(record, field)
				if key != "" {
					secKey := encodeSecondaryKey(t.tableID, field.Name, key, offset)
					t.db.index.Insert(secKey, offset)
				}
			}
		}
		return
	}
	for _, field := range t.layout.Fields {
		if field.Name != "" && !field.Primary && field.Offset > 0 && !field.IsSlice {
			if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][field.Name] {
				continue
			}
			key := embedcore.GetFieldAsString(record, field)
			if key != "" {
				secKey := encodeSecondaryKey(t.tableID, field.Name, key, offset)
				t.db.index.Insert(secKey, offset)
			}
		}
	}
}

func (t *Table[T]) deleteSecondaryKeys(record *T, offset uint64) {
	if t.db.parent == nil || !t.db.parent.autoIndex {
		return
	}
	for _, field := range t.layout.Fields {
		if field.Name != "" && !field.Primary && field.Offset > 0 && !field.IsSlice {
			key := embedcore.GetFieldAsString(record, field)
			if key != "" {
				secKey := encodeSecondaryKey(t.tableID, field.Name, key, offset)
				t.db.index.Delete(secKey)
			}
		}
	}
}

func (t *Table[T]) deactivateRecord(offset uint64) {
	deactivateBuf := make([]byte, embedcore.RecordHeaderSize)
	t.db.readAt(deactivateBuf, int64(offset))
	deactivateBuf[1] &^= embedcore.FlagsActive
	t.db.writeAt(deactivateBuf, int64(offset))
}

func (t *Table[T]) normalizePK(id any) any {
	v := reflect.ValueOf(id)
	pkType := reflect.TypeOf(id)
	switch t.layout.PKType {
	case reflect.Uint:
		pkType = reflect.TypeOf(uint(0))
	case reflect.Uint8:
		pkType = reflect.TypeOf(uint8(0))
	case reflect.Uint16:
		pkType = reflect.TypeOf(uint16(0))
	case reflect.Uint32:
		pkType = reflect.TypeOf(uint32(0))
	case reflect.Uint64:
		pkType = reflect.TypeOf(uint64(0))
	case reflect.Int:
		pkType = reflect.TypeOf(int(0))
	case reflect.Int8:
		pkType = reflect.TypeOf(int8(0))
	case reflect.Int16:
		pkType = reflect.TypeOf(int16(0))
	case reflect.Int32:
		pkType = reflect.TypeOf(int32(0))
	case reflect.Int64:
		pkType = reflect.TypeOf(int64(0))
	case reflect.String:
		pkType = reflect.TypeOf("")
	default:
		return id
	}
	if v.CanConvert(pkType) {
		return v.Convert(pkType).Interface()
	}
	return id
}

func (t *Table[T]) readRecordAt(offset uint64) (*T, error) {
	hdrBuf := make([]byte, embedcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		return nil, fmt.Errorf("failed to read record header at offset %d: %w", offset, err)
	}

	hdr, err := decodeRecordHeader(hdrBuf)
	if err != nil {
		return nil, fmt.Errorf("invalid record header at offset %d: %w", offset, err)
	}

	if hdr.Version != V2RecordVersion {
		return nil, fmt.Errorf("invalid record version at offset %d", offset)
	}

	if !hdr.IsActive() {
		return nil, fmt.Errorf("record is deleted")
	}

	if hdr.PayloadLen > 128*1024*1024 {
		return nil, fmt.Errorf("record payload too large at offset %d: %d bytes", offset, hdr.PayloadLen)
	}

	totalLen := recordTotalSize(hdr)
	recordBuf := make([]byte, totalLen)
	copy(recordBuf, hdrBuf)
	if totalLen > embedcore.RecordHeaderSize {
		if err := t.db.readAt(recordBuf[embedcore.RecordHeaderSize:], int64(offset)+int64(embedcore.RecordHeaderSize)); err != nil {
			return nil, fmt.Errorf("failed to read record data at offset %d: %w", offset, err)
		}
	}

	var result T
	if err := t.decodeRecord(recordBuf, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (t *Table[T]) encodeRecord(record *T, recordID uint32, flags byte, prevVersionOff uint64) ([]byte, error) {
	payload, err := encodeFieldPayload(record, t.layout)
	if err != nil {
		return nil, fmt.Errorf("failed to encode field payload: %w", err)
	}
	return buildV2Record(t.tableID, recordID, t.layout.SchemaVersion, flags, prevVersionOff, payload), nil
}

func (t *Table[T]) decodeRecord(data []byte, result *T) error {
	_, payload, err := parseV2Record(data)
	if err != nil {
		return fmt.Errorf("failed to parse v2 record: %w", err)
	}
	return decodeFieldPayload(payload, result, t.layout)
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
		if _, err := t.db.index.Get(encodePrimaryKey(t.tableID, pkVal)); err == nil {
			return 0, fmt.Errorf("primary key already exists: %v", pkVal)
		}
	}

	recordBuf, err := t.encodeRecord(record, recordID, embedcore.FlagsActive, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}

	offset, _, err := t.db.alloc.Allocate(uint64(len(recordBuf)))
	if err != nil {
		return 0, fmt.Errorf("failed to allocate space: %w", err)
	}
	if err := t.db.writeAt(recordBuf, int64(offset)); err != nil {
		return 0, fmt.Errorf("failed to write record: %w", err)
	}

	if err := t.db.index.Insert(encodePrimaryKey(t.tableID, pkVal), offset); err != nil {
		return 0, fmt.Errorf("failed to insert primary key: %w", err)
	}

	if t.maxVersions > 0 {
		t.db.index.Insert(encodeVersionKey(t.tableID, recordID, 1), offset)
	}

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]++
	} else {
		entry.RecordCount++
	}

	t.insertSecondaryKeys(record, offset)

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
	for _, field := range t.layout.Fields {
		if field.Primary {
			embedcore.SetFieldValue(record, field, val)
			return
		}
	}
}

func (t *Table[T]) getPKValue(record *T) (any, error) {
	for _, field := range t.layout.Fields {
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

	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return nil, fmt.Errorf("record not found")
	}

	return t.readRecordAt(offset)
}

func (t *Table[T]) GetVersion(id any, version uint32) (*T, error) {
	if t.maxVersions == 0 {
		return nil, fmt.Errorf("versioning not enabled for this table")
	}

	t.db.mu.RLock()
	defer t.db.mu.RUnlock()

	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return nil, fmt.Errorf("record not found")
	}

	hdrBuf := make([]byte, embedcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		return nil, fmt.Errorf("failed to read record header: %w", err)
	}
	hdr, err := decodeRecordHeader(hdrBuf)
	if err != nil {
		return nil, fmt.Errorf("invalid record header: %w", err)
	}

	versionOffset, err := t.db.index.Get(encodeVersionKey(t.tableID, hdr.RecordID, version))
	if err != nil {
		return nil, fmt.Errorf("version %d not found for record", version)
	}

	return t.readRecordAt(versionOffset)
}

func (t *Table[T]) ListVersions(id any) ([]VersionMetadata, error) {
	if t.maxVersions == 0 {
		return nil, fmt.Errorf("versioning not enabled for this table")
	}

	t.db.mu.RLock()
	defer t.db.mu.RUnlock()

	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return nil, fmt.Errorf("record not found")
	}

	hdrBuf := make([]byte, embedcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		return nil, fmt.Errorf("failed to read record header: %w", err)
	}
	hdr, err := decodeRecordHeader(hdrBuf)
	if err != nil {
		return nil, fmt.Errorf("invalid record header: %w", err)
	}

	prefix := encodeVersionKeyPrefix(t.tableID, hdr.RecordID)
	var results []VersionMetadata
	t.db.index.Scan(func(key []byte, value uint64) bool {
		if bytes.HasPrefix(key, prefix) {
			if _, _, ver, ok := parseVersionKey(key); ok {
				results = append(results, VersionMetadata{Version: ver})
			}
		}
		return true
	})

	return results, nil
}

func (t *Table[T]) Update(id any, record *T) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return fmt.Errorf("record not found")
	}

	hdrBuf := make([]byte, embedcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		return fmt.Errorf("failed to read record for update: %w", err)
	}

	hdr, err := decodeRecordHeader(hdrBuf)
	if err != nil {
		return fmt.Errorf("invalid record at offset %d: %w", offset, err)
	}

	if !hdr.IsActive() {
		return fmt.Errorf("record at offset %d is not active", offset)
	}

	recordID := hdr.RecordID

	totalLen := recordTotalSize(hdr)
	oldRecordBuf := make([]byte, totalLen)
	copy(oldRecordBuf, hdrBuf)
	if err := t.db.readAt(oldRecordBuf[embedcore.RecordHeaderSize:], int64(offset)+int64(embedcore.RecordHeaderSize)); err != nil {
		return fmt.Errorf("failed to read old record data: %w", err)
	}

	var oldRecord T
	t.decodeRecord(oldRecordBuf, &oldRecord)

	t.deleteSecondaryKeys(&oldRecord, offset)

	var flags byte = embedcore.FlagsActive
	var prevVersionOff uint64
	if t.maxVersions > 0 {
		flags |= embedcore.FlagsHasPrevVersion
		prevVersionOff = offset
	}

	newRecordBuf, err := t.encodeRecord(record, recordID, flags, prevVersionOff)
	if err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}

	newOffset, _, err := t.db.alloc.Allocate(uint64(len(newRecordBuf)))
	if err != nil {
		return fmt.Errorf("failed to allocate space: %w", err)
	}
	if err := t.db.writeAt(newRecordBuf, int64(newOffset)); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	if t.maxVersions > 0 {
		prefix := encodeVersionKeyPrefix(t.tableID, recordID)
		type verInfo struct {
			version uint32
			offset  uint64
		}
		var versions []verInfo
		t.db.index.Scan(func(key []byte, value uint64) bool {
			if bytes.HasPrefix(key, prefix) {
				if _, _, ver, ok := parseVersionKey(key); ok {
					versions = append(versions, verInfo{version: ver, offset: value})
				}
			}
			return true
		})

		if len(versions) == 0 {
			t.db.index.Insert(encodeVersionKey(t.tableID, recordID, 1), offset)
			versions = append(versions, verInfo{version: 1, offset: offset})
		}
		newVersion := uint32(1)
		for _, v := range versions {
			if v.version >= newVersion {
				newVersion = v.version + 1
			}
		}
		t.db.index.Insert(encodeVersionKey(t.tableID, recordID, newVersion), newOffset)
		versions = append(versions, verInfo{version: newVersion, offset: newOffset})

		maxTotal := int(t.maxVersions) + 1
		for len(versions) > maxTotal {
			oldestIdx := 0
			for i, v := range versions {
				if v.version < versions[oldestIdx].version {
					oldestIdx = i
				}
			}
			t.deactivateRecord(versions[oldestIdx].offset)
			t.db.index.Delete(encodeVersionKey(t.tableID, recordID, versions[oldestIdx].version))
			versions = append(versions[:oldestIdx], versions[oldestIdx+1:]...)
		}
	} else {
		t.deactivateRecord(offset)
	}

	t.db.index.Insert(encodePrimaryKey(t.tableID, t.normalizePK(id)), newOffset)

	t.insertSecondaryKeys(record, newOffset)

	t.db.autoSync()

	return nil
}

func (t *Table[T]) Delete(id any) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	entry := t.db.tableCat[t.name]

	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return fmt.Errorf("record not found")
	}

	var recordID uint32
	hdrBuf := make([]byte, embedcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
	}
	if hdrBuf[0] == V2RecordVersion {
		hdr, _ := decodeRecordHeader(hdrBuf)
		if hdr.IsActive() {
			recordID = hdr.RecordID
			totalLen := recordTotalSize(hdr)
			oldRecordBuf := make([]byte, totalLen)
			copy(oldRecordBuf, hdrBuf)
			if err := t.db.readAt(oldRecordBuf[embedcore.RecordHeaderSize:], int64(offset)+int64(embedcore.RecordHeaderSize)); err != nil {
			}

			var oldRecord T
			t.decodeRecord(oldRecordBuf, &oldRecord)

			t.deleteSecondaryKeys(&oldRecord, offset)
		}
	}

	t.deactivateRecord(offset)

	t.db.index.Delete(encodePrimaryKey(t.tableID, t.normalizePK(id)))

	if t.maxVersions > 0 && recordID > 0 {
		prefix := encodeVersionKeyPrefix(t.tableID, recordID)
		var versionKeys [][]byte
		t.db.index.Scan(func(key []byte, value uint64) bool {
			if bytes.HasPrefix(key, prefix) {
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				versionKeys = append(versionKeys, keyCopy)
			}
			return true
		})
		for _, k := range versionKeys {
			t.db.index.Delete(k)
		}
	}

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]--
	} else if entry.RecordCount > 0 {
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
		offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
		if err != nil {
			continue
		}

		hdrBuf := make([]byte, embedcore.RecordHeaderSize)
		if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		}
		if hdrBuf[0] == V2RecordVersion {
			hdr, _ := decodeRecordHeader(hdrBuf)
			if hdr.IsActive() {
				totalLen := recordTotalSize(hdr)
				oldRecordBuf := make([]byte, totalLen)
				copy(oldRecordBuf, hdrBuf)
				if err := t.db.readAt(oldRecordBuf[embedcore.RecordHeaderSize:], int64(offset)+int64(embedcore.RecordHeaderSize)); err != nil {
					continue
				}

				var oldRecord T
				t.decodeRecord(oldRecordBuf, &oldRecord)

				t.deleteSecondaryKeys(&oldRecord, offset)
			}
		}

		t.deactivateRecord(offset)
		t.db.index.Delete(encodePrimaryKey(t.tableID, t.normalizePK(id)))
		deleted++
		if t.db.tx != nil {
			t.db.tx.recordCounts[t.name]--
		} else if entry.RecordCount > 0 {
			entry.RecordCount--
		}
	}
	return deleted, nil
}

func (t *Table[T]) InsertMany(records []*T) ([]uint32, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	var firstErr error
	ids := make([]uint32, 0, len(records))
	for _, record := range records {
		id, err := t.insertLocked(record)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		ids = append(ids, id)
	}

	t.db.autoSync()

	return ids, firstErr
}

func (t *Table[T]) UpdateMany(ids []any, updater func(*T) error) (int, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	var firstErr error
	updated := 0
	for _, id := range ids {
		record, err := t.getLocked(id)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := updater(record); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := t.updateLocked(id, record); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		updated++
	}
	return updated, firstErr
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
		if _, err := t.db.index.Get(encodePrimaryKey(t.tableID, pkVal)); err == nil {
			return 0, fmt.Errorf("primary key already exists: %v", pkVal)
		}
	}

	recordBuf, err := t.encodeRecord(record, recordID, embedcore.FlagsActive, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}

	offset, _, err := t.db.alloc.Allocate(uint64(len(recordBuf)))
	if err != nil {
		return 0, fmt.Errorf("failed to allocate space: %w", err)
	}
	if err := t.db.writeAt(recordBuf, int64(offset)); err != nil {
		return 0, fmt.Errorf("failed to write record: %w", err)
	}

	t.db.index.Insert(encodePrimaryKey(t.tableID, pkVal), offset)

	if t.maxVersions > 0 {
		t.db.index.Insert(encodeVersionKey(t.tableID, recordID, 1), offset)
	}

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]++
	} else {
		entry.RecordCount++
	}

	t.insertSecondaryKeys(record, offset)

	return recordID, nil
}

func (t *Table[T]) getLocked(id any) (*T, error) {
	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return nil, fmt.Errorf("record not found")
	}

	return t.readRecordAt(offset)
}

func (t *Table[T]) updateLocked(id any, record *T) error {
	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return fmt.Errorf("record not found")
	}

	oldOffset := offset

	hdrBuf := make([]byte, embedcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(oldOffset)); err != nil {
		return fmt.Errorf("failed to read record header at offset %d: %w", oldOffset, err)
	}

	hdr, err := decodeRecordHeader(hdrBuf)
	if err != nil {
		return fmt.Errorf("invalid record at offset %d: %w", oldOffset, err)
	}

	recordID := hdr.RecordID

	totalLen := recordTotalSize(hdr)
	oldRecordBuf := make([]byte, totalLen)
	copy(oldRecordBuf, hdrBuf)
	if err := t.db.readAt(oldRecordBuf[embedcore.RecordHeaderSize:], int64(oldOffset)+int64(embedcore.RecordHeaderSize)); err != nil {
		return fmt.Errorf("failed to read old record data: %w", err)
	}

	var oldRecord T
	t.decodeRecord(oldRecordBuf, &oldRecord)

	t.deleteSecondaryKeys(&oldRecord, oldOffset)

	var flags byte = embedcore.FlagsActive
	var prevVersionOff uint64
	if t.maxVersions > 0 {
		flags |= embedcore.FlagsHasPrevVersion
		prevVersionOff = oldOffset
	}

	newRecordBuf, err := t.encodeRecord(record, recordID, flags, prevVersionOff)
	if err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}

	newOffset, _, err := t.db.alloc.Allocate(uint64(len(newRecordBuf)))
	if err != nil {
		return fmt.Errorf("failed to allocate space: %w", err)
	}
	if err := t.db.writeAt(newRecordBuf, int64(newOffset)); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	if t.maxVersions > 0 {
		prefix := encodeVersionKeyPrefix(t.tableID, recordID)
		type verInfo struct {
			version uint32
			offset  uint64
		}
		var versions []verInfo
		t.db.index.Scan(func(key []byte, value uint64) bool {
			if bytes.HasPrefix(key, prefix) {
				if _, _, ver, ok := parseVersionKey(key); ok {
					versions = append(versions, verInfo{version: ver, offset: value})
				}
			}
			return true
		})

		if len(versions) == 0 {
			t.db.index.Insert(encodeVersionKey(t.tableID, recordID, 1), oldOffset)
		}
		newVersion := uint32(1)
		for _, v := range versions {
			if v.version >= newVersion {
				newVersion = v.version + 1
			}
		}
		t.db.index.Insert(encodeVersionKey(t.tableID, recordID, newVersion), newOffset)
	} else {
		t.deactivateRecord(oldOffset)
	}

	t.db.index.Insert(encodePrimaryKey(t.tableID, t.normalizePK(id)), newOffset)

	t.insertSecondaryKeys(record, newOffset)

	return nil
}

func (t *Table[T]) Upsert(id any, record *T) (uint32, bool, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	_, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))

	if err == nil {
		if err := t.updateLocked(t.normalizePK(id), record); err != nil {
			return 0, false, err
		}
		return 0, false, nil
	}

	entry := t.db.tableCat[t.name]
	if entry == nil {
		return 0, false, fmt.Errorf("table not found")
	}

	recordID := entry.NextRecordID
	entry.NextRecordID++

	t.setPKValue(record, t.normalizePK(id))

	recordBuf, err := t.encodeRecord(record, recordID, embedcore.FlagsActive, 0)
	if err != nil {
		return 0, true, fmt.Errorf("failed to encode record: %w", err)
	}

	offset, _, err := t.db.alloc.Allocate(uint64(len(recordBuf)))
	if err != nil {
		return 0, false, fmt.Errorf("failed to allocate space: %w", err)
	}
	if err := t.db.writeAt(recordBuf, int64(offset)); err != nil {
		return 0, false, fmt.Errorf("failed to write record: %w", err)
	}

	pkVal, _ := t.getPKValue(record)
	t.db.index.Insert(encodePrimaryKey(t.tableID, pkVal), offset)

	t.insertSecondaryKeys(record, offset)

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]++
	} else {
		entry.RecordCount++
	}

	t.db.autoSync()

	return recordID, true, nil
}

func (t *Table[T]) Query(fieldName string, value interface{}) ([]T, error) {
	if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][fieldName] {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	autoIndex := t.db.parent != nil && t.db.parent.autoIndex
	explicitAllowed := false
	if !autoIndex && t.db.explicitIndexes != nil {
		for _, f := range t.db.explicitIndexes[t.name] {
			if f == fieldName {
				explicitAllowed = true
				break
			}
		}
	}

	if autoIndex || explicitAllowed {
		field, err := t.findField(fieldName)
		if err == nil && !field.Primary && field.Offset > 0 {
			strValue := valueToIndexKey(value, field.Type)
			prefix := encodeSecondaryKeyPrefixWithValue(t.tableID, fieldName, strValue)

			var offsets []uint64
			_ = t.db.index.Scan(func(key []byte, val uint64) bool {
				if bytes.HasPrefix(key, prefix) {
					offsets = append(offsets, val)
				}
				return true
			})

			slices.Sort(offsets)
			results := make([]T, 0, len(offsets))

			t.db.mu.RLock()
			for _, off := range offsets {
				record, err := t.readRecordAt(off)
				if err == nil && record != nil {
					results = append(results, *record)
				}
			}
			t.db.mu.RUnlock()

			return results, nil
		}
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
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) > 0 || (inclusive && compareValues(fieldValue, value) == 0)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryRangeLessThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) < 0 || (inclusive && compareValues(fieldValue, value) == 0)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryRangeBetween(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]T, error) {
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
	for _, f := range t.layout.Fields {
		if f.Name == fieldName {
			return f, nil
		}
	}
	return embedcore.FieldOffset{}, fmt.Errorf("field %s not found", fieldName)
}

func compareValues(a, b interface{}) int {
	if aTime, ok := a.(time.Time); ok {
		bTime, ok := b.(time.Time)
		if !ok {
			return 0
		}
		if aTime.Before(bTime) {
			return -1
		} else if aTime.After(bTime) {
			return 1
		}
		return 0
	}

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
	case reflect.Struct:
		if v, err := strconv.ParseInt(key, 10, 64); err == nil {
			return time.Unix(0, v).UTC()
		}
	}
	return key
}

func valueToIndexKey(val any, fieldKind reflect.Kind) string {
	switch v := val.(type) {
	case time.Time:
		return strconv.FormatInt(v.UnixNano(), 10)
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

type scanEntry struct {
	pkValue any
}

type Scanner[T any] struct {
	table   *Table[T]
	entries []scanEntry
	pos     int
	current *T
	err     error
}

func (t *Table[T]) ScanRecords() *Scanner[T] {
	t.db.mu.RLock()
	var entries []scanEntry
	_ = t.db.index.Scan(func(key []byte, value uint64) bool {
		if len(key) >= 2 && key[0] == indexNSPrimary && key[1] == t.tableID {
			pkBytes := key[2:]
			var pkVal any
			switch t.layout.PKType {
			case reflect.String:
				s, _, _ := embedcore.DecodeString(pkBytes)
				pkVal = s
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v, _, _ := embedcore.DecodeVarint(pkBytes)
				pkVal = int(v)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v, _, _ := embedcore.DecodeUvarint(pkBytes)
				pkVal = uint64(v)
			default:
				v, _, _ := embedcore.DecodeUvarint(pkBytes)
				pkVal = uint64(v)
			}
			entries = append(entries, scanEntry{pkValue: pkVal})
		}
		return true
	})
	t.db.mu.RUnlock()

	return &Scanner[T]{
		table:   t,
		entries: entries,
		pos:     0,
	}
}

func (s *Scanner[T]) Next() bool {
	for {
		if s.err != nil || s.pos >= len(s.entries) {
			return false
		}

		record, err := s.table.Get(s.entries[s.pos].pkValue)
		s.pos++
		if err != nil {
			continue
		}

		s.current = record
		return true
	}
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
	s.entries = nil
}

func (t *Table[T]) Count() int {
	t.db.mu.RLock()
	defer t.db.mu.RUnlock()
	entry := t.db.tableCat[t.name]
	if entry != nil {
		return int(entry.RecordCount)
	}
	count := 0
	_ = t.db.index.Scan(func(key []byte, value uint64) bool {
		if len(key) >= 2 && key[0] == indexNSPrimary && key[1] == t.tableID {
			count++
		}
		return true
	})
	return count
}

func (t *Table[T]) CreateIndex(fieldName string) error {
	if t.db.parent != nil && !t.db.parent.autoIndex {
		t.db.mu.Lock()
		if t.db.explicitIndexes == nil {
			t.db.explicitIndexes = make(map[string][]string)
		}
		found := false
		for _, f := range t.db.explicitIndexes[t.name] {
			if f == fieldName {
				found = true
				break
			}
		}
		if !found {
			t.db.explicitIndexes[t.name] = append(t.db.explicitIndexes[t.name], fieldName)
		}
		t.db.mu.Unlock()
	}

	field, err := t.findField(fieldName)
	if err != nil {
		return err
	}

	prefix := encodeSecondaryKeyPrefix(t.tableID, fieldName)
	var existing int
	t.db.mu.RLock()
	t.db.index.Scan(func(key []byte, value uint64) bool {
		if bytes.HasPrefix(key, prefix) {
			existing++
		}
		return true
	})
	t.db.mu.RUnlock()
	if existing > 0 {
		return nil
	}

	scanner := t.ScanRecords()
	defer scanner.Close()
	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}
		key := embedcore.GetFieldAsString(record, field)
		if key != "" {
			pkVal, _ := t.getPKValue(record)
			offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(pkVal)))
			if err != nil {
				continue
			}
			secKey := encodeSecondaryKey(t.tableID, fieldName, key, offset)
			t.db.index.Insert(secKey, offset)
		}
	}

	if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil {
		delete(t.db.droppedIndexes[t.name], fieldName)
	}

	return nil
}

func (t *Table[T]) DropIndex(fieldName string) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	prefix := encodeSecondaryKeyPrefix(t.tableID, fieldName)
	var keys [][]byte
	t.db.index.Scan(func(key []byte, value uint64) bool {
		if bytes.HasPrefix(key, prefix) {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			keys = append(keys, keyCopy)
		}
		return true
	})
	for _, k := range keys {
		t.db.index.Delete(k)
	}

	if t.db.droppedIndexes == nil {
		t.db.droppedIndexes = make(map[string]map[string]bool)
	}
	if t.db.droppedIndexes[t.name] == nil {
		t.db.droppedIndexes[t.name] = make(map[string]bool)
	}
	t.db.droppedIndexes[t.name][fieldName] = true

	return nil
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
	t.db.index.Scan(func(key []byte, value uint64) bool {
		if len(key) >= 2 && key[1] == t.tableID {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			keys = append(keys, keyCopy)
		}
		return true
	})
	for _, k := range keys {
		t.db.index.Delete(k)
	}

	entry.RecordCount = 0

	return nil
}

func (t *Table[T]) GetIndexedFields() []string {
	var fields []string
	if t.db.parent != nil && t.db.parent.autoIndex {
		for _, field := range t.layout.Fields {
			if field.Name != "" && !field.Primary && field.Offset > 0 && !field.IsSlice {
				if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][field.Name] {
					continue
				}
				fields = append(fields, field.Name)
			}
		}
	} else {
		if t.db.explicitIndexes != nil {
			for _, f := range t.db.explicitIndexes[t.name] {
				if t.db.droppedIndexes == nil || t.db.droppedIndexes[t.name] == nil || !t.db.droppedIndexes[t.name][f] {
					fields = append(fields, f)
				}
			}
		}
	}
	return fields
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
