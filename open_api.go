package embeddb

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
)

// OpenOptions configures how typed table handles are opened from DB.
type OpenOptions struct {
	Migrate   bool
	AutoIndex bool
}

type tableBinding interface {
	close() error
	sync() error
	vacuum() error
}

type typedBinding[T any] struct {
	db    *Database[T]
	table *Table[T]
}

func (b *typedBinding[T]) close() error {
	return b.db.Close()
}

func (b *typedBinding[T]) sync() error {
	return b.db.Sync()
}

func (b *typedBinding[T]) vacuum() error {
	return b.db.Vacuum()
}

// DB is a non-generic database handle used to open typed tables on demand.
//
// Use Use[T](db, "optional_table_name") to obtain a typed table handle.
// Each table/type pair gets its own typed Database[T] handle internally.
type DB struct {
	filename  string
	migrate   bool
	autoIndex bool

	lock   sync.Mutex
	closed bool
	tables map[string]tableBinding
}

// Open opens (or creates) a database file and returns a non-generic DB handle.
//
// By default, Open uses migrate=false and autoIndex=false for typed handles.
// You can override that behavior with OpenOptions:
//
//	db, _ := embeddb.Open("app.db", embeddb.OpenOptions{Migrate: true, AutoIndex: true})
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
		tables:    make(map[string]tableBinding),
	}, nil
}

// Use returns a typed table handle from a non-generic DB.
//
// Example:
//
//	db, _ := embeddb.Open("app.db")
//	users, _ := embeddb.Use[User](db, "users")
func Use[T any](db *DB, name ...string) (*Table[T], error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}

	tableName := resolveTableName[T](name...)
	key := bindingKey[T](tableName)

	db.lock.Lock()
	if db.closed {
		db.lock.Unlock()
		return nil, fmt.Errorf("database is closed")
	}
	if existing, ok := db.tables[key]; ok {
		binding, ok := existing.(*typedBinding[T])
		db.lock.Unlock()
		if !ok {
			return nil, fmt.Errorf("table binding type mismatch for %q", tableName)
		}
		return binding.table, nil
	}
	db.lock.Unlock()

	typedDB, err := New[T](db.filename, db.migrate, db.autoIndex)
	if err != nil {
		return nil, err
	}

	table, err := typedDB.Table(tableName)
	if err != nil {
		_ = typedDB.Close()
		return nil, err
	}

	binding := &typedBinding[T]{
		db:    typedDB,
		table: table,
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	if db.closed {
		_ = binding.close()
		return nil, fmt.Errorf("database is closed")
	}

	if existing, ok := db.tables[key]; ok {
		existingBinding, ok := existing.(*typedBinding[T])
		if ok {
			_ = binding.close()
			return existingBinding.table, nil
		}
		_ = binding.close()
		return nil, fmt.Errorf("table binding type mismatch for %q", tableName)
	}

	db.tables[key] = binding
	return table, nil
}

// Close closes all typed handles opened through Use[T].
func (db *DB) Close() error {
	if db == nil {
		return nil
	}

	db.lock.Lock()
	if db.closed {
		db.lock.Unlock()
		return nil
	}

	bindings := make([]tableBinding, 0, len(db.tables))
	for _, b := range db.tables {
		bindings = append(bindings, b)
	}
	db.tables = make(map[string]tableBinding)
	db.closed = true
	db.lock.Unlock()

	var closeErr error
	for _, b := range bindings {
		if err := b.close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	return closeErr
}

// Sync flushes database metadata and index state to disk.
// If no typed tables were opened through Use[T], Sync is a no-op.
func (db *DB) Sync() error {
	if db == nil {
		return nil
	}

	db.lock.Lock()
	if db.closed {
		db.lock.Unlock()
		return fmt.Errorf("database is closed")
	}
	var binding tableBinding
	for _, b := range db.tables {
		binding = b
		break
	}
	db.lock.Unlock()

	if binding == nil {
		return nil
	}

	return binding.sync()
}

// Vacuum compacts the underlying database file.
// If no typed tables were opened through Use[T], Vacuum is a no-op.
func (db *DB) Vacuum() error {
	if db == nil {
		return nil
	}

	db.lock.Lock()
	if db.closed {
		db.lock.Unlock()
		return fmt.Errorf("database is closed")
	}
	var binding tableBinding
	for _, b := range db.tables {
		binding = b
		break
	}
	db.lock.Unlock()

	if binding == nil {
		return nil
	}

	return binding.vacuum()
}

// SecondaryIndexStoreStats reports embedded secondary-index blob usage.
// It reads directly from the underlying DB file path.
func (db *DB) SecondaryIndexStoreStats() (*SecondaryIndexStoreStats, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	return GetSecondaryIndexStoreStats(db.filename)
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

func bindingKey[T any](tableName string) string {
	var instance T
	t := reflect.TypeOf(instance)
	if t == nil {
		return "<nil>:" + tableName
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.PkgPath() + ":" + t.String() + ":" + tableName
}
