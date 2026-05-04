package embeddb

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/yay101/embeddbcore"
	"github.com/yay101/embeddbmmap"
)

const FlagsCompressed byte = 0x04

var hdrBufPool = sync.Pool{
	New: func() any {
		return make([]byte, embeddbcore.RecordHeaderSize)
	},
}

func encodePKForIndex(tableID uint8, pkValue any) []byte {
	var key []byte
	key = append(key, tableID)
	switch v := pkValue.(type) {
	case int:
		key = embeddbcore.EncodeUvarint(key, uint64(int64(v)))
	case int8:
		key = embeddbcore.EncodeUvarint(key, uint64(int64(v)))
	case int16:
		key = embeddbcore.EncodeUvarint(key, uint64(int64(v)))
	case int32:
		key = embeddbcore.EncodeUvarint(key, uint64(int64(v)))
	case int64:
		key = embeddbcore.EncodeUvarint(key, uint64(v))
	case uint:
		key = embeddbcore.EncodeUvarint(key, uint64(v))
	case uint8:
		key = embeddbcore.EncodeUvarint(key, uint64(v))
	case uint16:
		key = embeddbcore.EncodeUvarint(key, uint64(v))
	case uint32:
		key = embeddbcore.EncodeUvarint(key, uint64(v))
	case uint64:
		key = embeddbcore.EncodeUvarint(key, v)
	case string:
		key = embeddbcore.EncodeString(key, v)
	}
	return key
}

type StorageMode int

const (
	StorageMmap StorageMode = iota
	StorageFile
)

type OpenOptions struct {
	Migrate        bool
	AutoIndex      bool
	SyncThreshold  uint64
	IdleThreshold  time.Duration
	CachePages     int
	EncryptionKey  []byte
	StorageMode    StorageMode
	WAL            bool
	Compression    bool
	CompressMinLen int
}

type UseOptions struct {
	MaxVersions uint8
}

type VersionMetadata struct {
	Version   uint32
	CreatedAt time.Time
}

type DB struct {
	filename       string
	migrate        bool
	autoIndex      bool
	syncThreshold  uint64
	idleThreshold  time.Duration
	cachePages     int
	encryptionKey  []byte
	storageMode    StorageMode
	wal            bool
	compression    bool
	compressMinLen int
	writeCount     uint64
	lastSync       time.Time
	lock           sync.Mutex
	closed         bool
	database       *database
	tables         map[string]*database
}

func Open(filename string, opts ...OpenOptions) (*DB, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename is required")
	}

	migrate := true
	autoIndex := true
	syncThreshold := uint64(1000)
	idleThreshold := 10 * time.Second
	cachePages := 0
	storageMode := StorageMmap
	wal := false
	compression := false
	compressMinLen := 64
	var encryptionKey []byte
	if len(opts) > 0 {
		migrate = opts[0].Migrate
		autoIndex = opts[0].AutoIndex
		if opts[0].SyncThreshold > 0 {
			syncThreshold = opts[0].SyncThreshold
		}
		if opts[0].IdleThreshold > 0 {
			idleThreshold = opts[0].IdleThreshold
		}
		if opts[0].CachePages > 0 {
			cachePages = opts[0].CachePages
		}
		encryptionKey = opts[0].EncryptionKey
		storageMode = opts[0].StorageMode
		wal = opts[0].WAL
		compression = opts[0].Compression
		if opts[0].CompressMinLen > 0 {
			compressMinLen = opts[0].CompressMinLen
		}
	}

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("failed to close file handle: %w", err)
	}

	if len(encryptionKey) > 0 {
		if _, err := newFieldCipher(encryptionKey); err != nil {
			return nil, fmt.Errorf("invalid encryption key: %w", err)
		}
	}

	return &DB{
		filename:       filename,
		migrate:        migrate,
		autoIndex:      autoIndex,
		syncThreshold:  syncThreshold,
		idleThreshold:  idleThreshold,
		cachePages:     cachePages,
		encryptionKey:  encryptionKey,
		storageMode:    storageMode,
		wal:            wal,
		compression:    compression,
		compressMinLen: compressMinLen,
		writeCount:     0,
		lastSync:       time.Now(),
		tables:         make(map[string]*database),
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
		var cipher *fieldCipher
		if len(db.encryptionKey) > 0 {
			cipher, _ = newFieldCipher(db.encryptionKey)
		}
		return &Table[T]{
			db:          existing,
			name:        tableName,
			tableID:     existingTable.ID,
			layout:      layout,
			maxVersions: existingTable.MaxVersions,
			cipher:      cipher,
		}, nil
	}

	var typedDB *database
	if db.database != nil {
		typedDB = db.database
	} else {
		var err error
		typedDB, err = openDatabase(db.filename, false, db, db.storageMode, db.wal)
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

	var cipher *fieldCipher
	if len(db.encryptionKey) > 0 {
		cipher, _ = newFieldCipher(db.encryptionKey)
	}

	typedTable := &Table[T]{
		db:          typedDB,
		name:        tableName,
		tableID:     table.ID,
		layout:      layout,
		maxVersions: table.MaxVersions,
		cipher:      cipher,
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

type DBStats struct {
	CacheStats map[string]CacheStats
}

func (db *DB) Stats() DBStats {
	stats := DBStats{
		CacheStats: make(map[string]CacheStats),
	}
	if db.database != nil && db.database.index != nil {
		stats.CacheStats["primary"] = db.database.index.GetCacheStats()
	}
	return stats
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

func computeLayout[T any]() *embeddbcore.StructLayout {
	var instance T
	layout, _ := embeddbcore.ComputeStructLayout(instance)
	return layout
}

type Table[T any] struct {
	db          *database
	name        string
	tableID     uint8
	layout      *embeddbcore.StructLayout
	maxVersions uint8
	cipher      *fieldCipher
}

func (t *Table[T]) deactivateRecord(offset uint64) {
	deactivateBuf := hdrBufPool.Get().([]byte)
	defer hdrBufPool.Put(deactivateBuf)
	t.db.readAt(deactivateBuf, int64(offset))
	deactivateBuf[1] &^= embeddbcore.FlagsActive
	t.db.writeAt(deactivateBuf, int64(offset))
}

func (t *Table[T]) normalizePK(id any) any {
	switch t.layout.PKType {
	case reflect.Uint:
		switch v := id.(type) {
		case int:
			return uint(v)
		case int8:
			return uint(v)
		case int16:
			return uint(v)
		case int32:
			return uint(v)
		case int64:
			return uint(v)
		case uint:
			return v
		case uint8:
			return uint(v)
		case uint16:
			return uint(v)
		case uint32:
			return uint(v)
		case uint64:
			return uint(v)
		}
	case reflect.Uint8:
		switch v := id.(type) {
		case int:
			return uint8(v)
		case uint:
			return uint8(v)
		case uint32:
			return uint8(v)
		}
	case reflect.Uint16:
		switch v := id.(type) {
		case int:
			return uint16(v)
		case uint:
			return uint16(v)
		case uint32:
			return uint16(v)
		}
	case reflect.Uint32:
		switch v := id.(type) {
		case int:
			return uint32(v)
		case uint:
			return uint32(v)
		case uint64:
			return uint32(v)
		}
	case reflect.Uint64:
		switch v := id.(type) {
		case int:
			return uint64(v)
		case uint:
			return v
		case uint32:
			return uint64(v)
		}
	case reflect.Int:
		switch v := id.(type) {
		case int:
			return v
		case uint:
			return int(v)
		case uint32:
			return int(v)
		case uint64:
			return int(v)
		}
	case reflect.Int8:
		switch v := id.(type) {
		case int:
			return int8(v)
		case uint:
			return int8(v)
		}
	case reflect.Int16:
		switch v := id.(type) {
		case int:
			return int16(v)
		case uint:
			return int16(v)
		}
	case reflect.Int32:
		switch v := id.(type) {
		case int:
			return int32(v)
		case uint:
			return int32(v)
		}
	case reflect.Int64:
		switch v := id.(type) {
		case int:
			return int64(v)
		case uint:
			return int64(v)
		}
	case reflect.String:
		if s, ok := id.(string); ok {
			return s
		}
	default:
		return id
	}
	return id
}

func (t *Table[T]) readRecordAt(offset uint64) (*T, error) {
	hdrBuf := hdrBufPool.Get().([]byte)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		hdrBufPool.Put(hdrBuf)
		return nil, fmt.Errorf("failed to read record header at offset %d: %w", offset, err)
	}

	hdr, err := decodeRecordHeader(hdrBuf)
	if err != nil {
		hdrBufPool.Put(hdrBuf)
		return nil, fmt.Errorf("invalid record header at offset %d: %w", offset, err)
	}

	if hdr.Version != V2RecordVersion {
		hdrBufPool.Put(hdrBuf)
		return nil, fmt.Errorf("invalid record version at offset %d", offset)
	}

	if !hdr.IsActive() {
		hdrBufPool.Put(hdrBuf)
		return nil, fmt.Errorf("record is deleted")
	}

	if hdr.PayloadLen > 128*1024*1024 {
		hdrBufPool.Put(hdrBuf)
		return nil, fmt.Errorf("record payload too large at offset %d: %d bytes", offset, hdr.PayloadLen)
	}

	totalLen := recordTotalSize(hdr)
	recordBuf := make([]byte, totalLen)
	copy(recordBuf[:embeddbcore.RecordHeaderSize], hdrBuf[:embeddbcore.RecordHeaderSize])
	hdrBufPool.Put(hdrBuf)

	if totalLen > embeddbcore.RecordHeaderSize {
		if err := t.db.readAt(recordBuf[embeddbcore.RecordHeaderSize:], int64(offset)+int64(embeddbcore.RecordHeaderSize)); err != nil {
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
	payload, err := encodeFieldPayload(record, t.layout, t.cipher)
	if err != nil {
		return nil, fmt.Errorf("failed to encode field payload: %w", err)
	}

	if t.db.parent != nil && t.db.parent.compression && len(payload) >= t.db.parent.compressMinLen {
		compressed := snappy.Encode(nil, payload)
		if len(compressed) < len(payload) {
			flags |= FlagsCompressed
			payload = compressed
		}
	}

	return buildV2Record(t.tableID, recordID, t.layout.SchemaVersion, flags, prevVersionOff, payload), nil
}

func (t *Table[T]) decodeRecord(data []byte, result *T) error {
	hdr, payload, err := parseV2Record(data)
	if err != nil {
		return fmt.Errorf("failed to parse v2 record: %w", err)
	}

	if hdr.Flags&FlagsCompressed != 0 {
		payload, err = snappy.Decode(nil, payload)
		if err != nil {
			return fmt.Errorf("failed to decompress record: %w", err)
		}
	}

	return decodeFieldPayload(payload, result, t.layout, t.cipher)
}

func (t *Table[T]) Name() string {
	return t.name
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

func (t *Table[T]) getRecordIDAt(offset uint64) (uint32, error) {
	hdrBuf := hdrBufPool.Get().([]byte)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		hdrBufPool.Put(hdrBuf)
		return 0, fmt.Errorf("failed to read record header: %w", err)
	}
	hdr, err := decodeRecordHeader(hdrBuf)
	hdrBufPool.Put(hdrBuf)
	if err != nil {
		return 0, fmt.Errorf("invalid record header: %w", err)
	}
	return hdr.RecordID, nil
}
