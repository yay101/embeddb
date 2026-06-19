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

// StorageMode determines how the database file is accessed.
// StorageMmap uses memory-mapped I/O (default), StorageFile uses direct file I/O,
// and StorageMemory keeps the entire database in anonymous memory (no persistence).
type StorageMode int

const (
	StorageMmap StorageMode = iota
	StorageFile
	StorageMemory
)

// OpenOptions configures database behavior when opening a file.
//
// Migrate enables automatic schema migration when struct layouts change.
// AutoIndex automatically creates secondary indexes for fields tagged with `db:"index"`.
// SyncThreshold triggers a sync after this many writes (0 disables count-based sync).
// IdleThreshold triggers a sync after this duration of inactivity (0 disables time-based sync).
// CachePages sets a fixed B-tree page cache size (0 uses adaptive sizing).
// EncryptionKey enables AES-256-GCM field-level encryption (cannot be used with Compression).
// StorageMode selects the storage backend (mmap, file, or memory).
// WAL enables write-ahead logging for crash recovery.
// Compression enables snappy compression for record payloads.
// CompressMinLen is the minimum payload size before compression is attempted (default 64).
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

// UseOptions configures table-specific behavior.
// MaxVersions sets the number of previous record versions to retain (0 disables versioning).
type UseOptions struct {
	MaxVersions uint8
}

// VersionMetadata contains metadata about a specific record version.
type VersionMetadata struct {
	Version   uint32
	CreatedAt time.Time
}

// DB represents an embedded database instance backed by a single file.
// It manages multiple typed tables, handles persistence, caching, and synchronization.
// A DB must be closed with Close() when no longer needed to ensure data is flushed.
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

// Open creates or opens an embedded database at the given filename.
// If no options are provided, defaults are used: migration enabled, auto-indexing enabled,
// sync threshold of 1000 writes, idle threshold of 10 seconds, adaptive cache sizing,
// mmap storage mode, no WAL, no compression.
//
// Returns an error if the filename is empty (for non-memory modes), the file cannot be
// opened, the encryption key is invalid, or compression and encryption are both enabled.
func Open(filename string, opts ...OpenOptions) (*DB, error) {
	optsCopy := OpenOptions{}
	if len(opts) > 0 {
		optsCopy = opts[0]
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
		migrate = optsCopy.Migrate
		autoIndex = optsCopy.AutoIndex
		if optsCopy.SyncThreshold > 0 {
			syncThreshold = optsCopy.SyncThreshold
		}
		if optsCopy.IdleThreshold > 0 {
			idleThreshold = optsCopy.IdleThreshold
		}
		if optsCopy.CachePages > 0 {
			cachePages = optsCopy.CachePages
		}
		encryptionKey = optsCopy.EncryptionKey
		storageMode = optsCopy.StorageMode
		wal = optsCopy.WAL
		compression = optsCopy.Compression
		if optsCopy.CompressMinLen > 0 {
			compressMinLen = optsCopy.CompressMinLen
		}
	}

	if wal && storageMode == StorageMemory {
		return nil, fmt.Errorf("WAL is not supported with StorageMemory mode")
	}

	if compression && len(encryptionKey) > 0 {
		return nil, fmt.Errorf("compression and encryption cannot be used together: encrypted data is incompressible")
	}

	if storageMode != StorageMemory {
		if filename == "" {
			return nil, fmt.Errorf("filename is required")
		}
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("failed to close file handle: %w", err)
		}
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

func validateLayout[T any]() error {
	var instance T
	t := reflect.TypeOf(instance)
	if t == nil {
		return nil
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	return validateFields(t)
}

func validateFields(t reflect.Type) error {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		if !f.IsExported() {
			continue
		}

		dbTag := f.Tag.Get("db")
		if dbTag == "-" {
			continue
		}

		if err := validateFieldType(f.Type, f.Name); err != nil {
			return err
		}

		parts := strings.Split(dbTag, ",")
		hasIndex := false
		hasEncrypt := false
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "index" {
				hasIndex = true
			}
			if part == "encrypt" {
				hasEncrypt = true
			}
		}

		if hasIndex && hasEncrypt {
			return fmt.Errorf("field %q cannot have both index and encrypt tags: encrypted fields cannot be indexed", f.Name)
		}
	}
	return nil
}

func validateFieldType(t reflect.Type, name string) error {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.Bool, reflect.String:
		return nil
	case reflect.Slice:
		return validateFieldType(t.Elem(), name)
	case reflect.Struct:
		if t.PkgPath() == "time" && t.Name() == "Time" {
			return nil
		}
		return validateFields(t)
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return fmt.Errorf("unsupported map key type %v for field %q: only string keys are supported", t.Key().Kind(), name)
		}
		if err := validateFieldType(t.Elem(), name); err != nil {
			return err
		}
		return nil
	case reflect.Array:
		return fmt.Errorf("unsupported field type [N]T for field %q: fixed-size arrays are not supported", name)
	case reflect.Chan:
		return fmt.Errorf("unsupported field type chan for field %q", name)
	case reflect.Func:
		return fmt.Errorf("unsupported field type func for field %q", name)
	case reflect.Interface:
		return nil
	case reflect.Ptr:
		return fmt.Errorf("unsupported field type pointer for field %q", name)
	case reflect.Complex64, reflect.Complex128:
		return fmt.Errorf("unsupported field type complex for field %q", name)
	case reflect.Uintptr:
		return fmt.Errorf("unsupported field type uintptr for field %q", name)
	case reflect.UnsafePointer:
		return fmt.Errorf("unsupported field type unsafe.Pointer for field %q", name)
	}
	return nil
}

// Use returns a type-safe Table[T] for the given struct type T.
// The table name is inferred from the type name unless explicitly provided as a string argument.
// UseOptions can be passed to configure versioning (e.g., UseOptions{MaxVersions: 5}).
//
// If the table doesn't exist, it is created. If the struct layout has changed since the table
// was created, automatic migration is performed (if Migrate is enabled in OpenOptions).
//
// Example:
//
//	type User struct { ID uint; Name string }
//	db, _ := embeddb.Open("my.db")
//	users, _ := embeddb.Use[User](db)
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

	if err := validateLayout[T](); err != nil {
		return nil, err
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

// Close flushes all pending writes, syncs the B-tree index, and releases the file lock.
// After Close, the DB cannot be used for further operations.
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

// Begin starts a transaction. All subsequent writes are tracked and can be rolled back.
// Only one transaction can be active at a time.
func (db *DB) Begin() *Transaction {
	if db == nil || db.database == nil {
		return nil
	}
	if tx := db.database.begin(); tx != nil {
		return &Transaction{tx: tx}
	}
	return nil
}

// Transaction holds the state of an active transaction. Call Commit to persist
// all changes or Rollback to discard them.
type Transaction struct {
	tx *transaction
}

// Commit persists all writes made during this transaction. Returns an error
// if the transaction was already committed or rolled back.
func (tx *Transaction) Commit() error {
	if tx == nil || tx.tx == nil {
		return fmt.Errorf("no active transaction")
	}
	return tx.tx.commit()
}

// Rollback discards all writes made during this transaction and restores the
// database to its state before Begin was called. Returns an error if the
// transaction was already committed.
func (tx *Transaction) Rollback() error {
	if tx == nil || tx.tx == nil {
		return fmt.Errorf("no active transaction")
	}
	return tx.tx.rollback()
}

// Sync flushes all pending writes to disk for all open tables. This is a full sync that
// compacts the B-tree index and ensures durability. It is called automatically based on
// SyncThreshold and IdleThreshold options.
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

// TableStats contains runtime statistics for a single table.
type TableStats struct {
	Name        string // table name
	RecordCount int    // number of active records
	TableID     uint8  // internal table identifier
}

// AllocatorStats contains memory allocation statistics.
type AllocatorStats struct {
	NextOffset uint64 // next available offset in the file
	ActualSize uint64 // total size allocated (including free space)
	FreeBytes  uint64 // total bytes in the free list
	FreeBlocks int    // number of free blocks
}

// DBStats contains runtime statistics for the database, including per-table metrics,
// file size, allocator state, and B-tree cache performance.
type DBStats struct {
	Tables     map[string]TableStats   // per-table statistics
	FileSize   int64                   // current database file size
	WALSize    int64                   // WAL file size (0 if WAL not enabled)
	Allocator  AllocatorStats          // allocator state
	CacheStats map[string]CacheStats   // per-table B-tree cache stats
	IndexKeys  int                     // total keys in the primary B-tree index
	BTreeDepth int                     // height of the B+ tree
}

// Stats returns runtime statistics for the database, including per-table record counts,
// file size, WAL size, allocator state, cache hit rates, and total index keys.
func (db *DB) Stats() DBStats {
	stats := DBStats{
		Tables:     make(map[string]TableStats),
		CacheStats: make(map[string]CacheStats),
	}

	if db.database != nil {
		db.database.mu.RLock()

		// Per-table stats
		for name, entry := range db.database.tableCat {
			if !entry.Dropped {
				stats.Tables[name] = TableStats{
					Name:        name,
					RecordCount: int(entry.RecordCount),
					TableID:     entry.ID,
				}
			}
		}

		// Allocator stats
		db.database.alloc.mu.Lock()
		var freeBytes uint64
		for _, fb := range db.database.alloc.freeList {
			freeBytes += fb.length
		}
		stats.Allocator = AllocatorStats{
			NextOffset: db.database.alloc.nextOffset,
			ActualSize: db.database.alloc.actualSize,
			FreeBytes:  freeBytes,
			FreeBlocks: len(db.database.alloc.freeList),
		}
		db.database.alloc.mu.Unlock()

		// Index key count
		stats.IndexKeys = db.database.index.count
		stats.BTreeDepth = db.database.index.Depth()

		db.database.mu.RUnlock()

		// Cache stats
		stats.CacheStats["primary"] = db.database.index.GetCacheStats()

		// File size
		if db.database.file != nil {
			if fi, err := db.database.file.Stat(); err == nil {
				stats.FileSize = fi.Size()
			}
		} else {
			r := db.database.region.Load()
			if r != nil {
				r.RLock()
				stats.FileSize = r.Size()
				r.RUnlock()
			}
		}

		// WAL size
		if db.database.wal != nil && db.database.wal.file != nil {
			if fi, err := db.database.wal.file.Stat(); err == nil {
				stats.WALSize = fi.Size()
			}
		}
	}

	return stats
}

// EnableAllocatorDebug enables debug tracking in the allocator. All subsequent
// allocations are tracked and the allocator will panic if it returns the same
// offset twice or if allocations overlap. Use this to detect double-allocation bugs.
// Intended for testing/debugging only. Call before any table operations.
func (db *DB) EnableAllocatorDebug() {
	if db.database != nil {
		db.database.alloc.mu.Lock()
		db.database.alloc.debugMode = true
		db.database.alloc.allocSet = make(map[[2]uint64]struct{})
		db.database.alloc.mu.Unlock()
	}
}

// FastSync performs an asynchronous sync of the memory-mapped region without
// rebuilding the B-tree index. This is faster than Sync() but provides weaker
// durability guarantees.
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

// Vacuum compacts the database file by removing dead records and rebuilding all indexes.
// This reduces file size after many updates/deletes but is an expensive operation.
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

// Table represents a type-safe collection of records of type T.
// It provides CRUD operations, querying, indexing, and scanning capabilities.
// Tables are obtained via Use[T](db).
type Table[T any] struct {
	db          *database
	name        string
	tableID     uint8
	layout      *embeddbcore.StructLayout
	maxVersions uint8
	cipher      *fieldCipher
}

func (t *Table[T]) deactivateRecord(offset uint64) error {
	deactivateBuf := hdrBufPool.Get().([]byte)
	defer hdrBufPool.Put(deactivateBuf)
	if err := t.db.readAt(deactivateBuf, int64(offset)); err != nil {
		return fmt.Errorf("deactivateRecord read at %d: %w", offset, err)
	}
	deactivateBuf[1] &^= embeddbcore.FlagsActive
	if err := t.db.writeAt(deactivateBuf, int64(offset)); err != nil {
		return fmt.Errorf("deactivateRecord write at %d: %w", offset, err)
	}
	return nil
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

// Name returns the name of the table.
func (t *Table[T]) Name() string {
	return t.name
}

// TableID returns the internal table identifier used for index key construction.
// Exported for netembeddb to construct secondary index keys via EncodeSecondaryKey.
func (t *Table[T]) TableID() uint8 {
	return t.tableID
}

// Drop marks the table as dropped and removes all its index entries from the B-tree.
// The records remain in the file until a Vacuum is performed.
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
