package embeddb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

var (
	ErrTransactionInProgress = errors.New("transaction already in progress")
	ErrNoTransaction         = errors.New("no transaction in progress")
)

// TransactionState represents the current state of a transaction
type TransactionState int

const (
	// TransactionNone indicates no transaction is in progress
	TransactionNone TransactionState = iota
	// TransactionActive indicates a transaction is currently active
	TransactionActive
	// TransactionCommitted indicates a transaction has been committed
	TransactionCommitted
	// TransactionRolledBack indicates a transaction has been rolled back
	TransactionRolledBack
)

// Transaction represents a database transaction
type Transaction struct {
	// The state of the transaction
	state TransactionState
	// The temporary file for the transaction
	tempFile *os.File
	// The temporary index for the transaction (per-table)
	tempIndex map[uint8]map[uint32]uint32
	// The original header state values (not the entire header with mutex)
	originalIndexStart    uint32
	originalIndexEnd      uint32
	originalNextRecordID  uint32
	originalNextOffset    uint32
	originalVersion       string
	originalTocStart      uint32
	originalEntryStart    uint32
	originalLgIndexStart  uint32
	originalIndexCapacity uint32
	// Lock for the transaction
	lock sync.RWMutex
	// Reference to the database
	db any
}

// beginTransaction starts a new transaction
// This acquires db.lock and releases it before returning
func (db *Database[T]) beginTransaction() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.beginTransactionLocked()
}

// beginTransactionLocked starts a new transaction
// IMPORTANT: Caller must hold db.lock before calling this method
func (db *Database[T]) beginTransactionLocked() error {
	// If a transaction is already in progress, clean it up first
	// This can happen when using writeLock to serialize operations
	if db.transaction != nil && db.transaction.state == TransactionActive {
		// Clean up old transaction
		if db.transaction.tempFile != nil {
			db.transaction.tempFile.Close()
			os.Remove(db.transaction.tempFile.Name())
		}
		db.transaction = nil
	}

	// Create a temporary file for the transaction
	tempFile, err := os.CreateTemp("", "embeddb-txn-*")
	if err != nil {
		return fmt.Errorf("failed to create transaction temporary file: %w", err)
	}

	// Save the original header state values (not copying the mutex)
	var originalIndexStart uint32 = db.header.indexStart
	var originalIndexEnd uint32 = db.header.indexEnd
	var originalNextRecordID uint32 = db.header.nextRecordID
	var originalNextOffset uint32 = db.header.nextOffset
	var originalVersion string = db.header.Version
	var originalTocStart uint32 = db.header.tocStart
	var originalEntryStart uint32 = db.header.entryStart
	var originalLgIndexStart uint32 = db.header.lgIndexStart
	var originalIndexCapacity uint32 = db.header.indexCapacity

	// Create a copy of the current index (all tables, for rollback)
	tempIndex := make(map[uint8]map[uint32]uint32)
	for tableID, idx := range db.indexes {
		tempIndex[tableID] = make(map[uint32]uint32)
		for k, v := range idx {
			tempIndex[tableID][k] = v
		}
	}

	// Create a new transaction
	db.transaction = &Transaction{
		state:                 TransactionActive,
		tempFile:              tempFile,
		tempIndex:             tempIndex,
		originalIndexStart:    originalIndexStart,
		originalIndexEnd:      originalIndexEnd,
		originalNextRecordID:  originalNextRecordID,
		originalNextOffset:    originalNextOffset,
		originalVersion:       originalVersion,
		originalTocStart:      originalTocStart,
		originalEntryStart:    originalEntryStart,
		originalLgIndexStart:  originalLgIndexStart,
		originalIndexCapacity: originalIndexCapacity,
		db:                    db,
	}

	return nil
}

// commitTransaction commits the current transaction
// This acquires db.lock and releases it before returning
func (db *Database[T]) commitTransaction() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.commitTransactionLocked()
}

// commitTransactionLocked commits the current transaction
// IMPORTANT: Caller must hold db.lock before calling this method
func (db *Database[T]) commitTransactionLocked() error {
	// Check if a transaction is in progress
	if db.transaction == nil || db.transaction.state != TransactionActive {
		return ErrNoTransaction
	}

	// Flush any pending writes
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("failed to flush database file: %w", err)
	}

	// Clean up the temporary file
	if err := db.transaction.tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close transaction temporary file: %w", err)
	}
	if err := os.Remove(db.transaction.tempFile.Name()); err != nil {
		return fmt.Errorf("failed to remove transaction temporary file: %w", err)
	}

	// Mark the transaction as committed
	db.transaction.state = TransactionCommitted
	db.transaction = nil

	return nil
}

// rollbackTransaction rolls back the current transaction
// This acquires db.lock and releases it before returning
func (db *Database[T]) rollbackTransaction() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.rollbackTransactionLocked()
}

// rollbackTransactionLocked rolls back the current transaction
// IMPORTANT: Caller must hold db.lock before calling this method
func (db *Database[T]) rollbackTransactionLocked() error {
	// Check if a transaction is in progress
	if db.transaction == nil || db.transaction.state != TransactionActive {
		return ErrNoTransaction
	}

	// Restore the original header state values
	atomic.StoreUint32(&db.header.indexStart, db.transaction.originalIndexStart)
	atomic.StoreUint32(&db.header.indexEnd, db.transaction.originalIndexEnd)
	atomic.StoreUint32(&db.header.nextRecordID, db.transaction.originalNextRecordID)
	atomic.StoreUint32(&db.header.nextOffset, db.transaction.originalNextOffset)
	db.header.Version = db.transaction.originalVersion
	atomic.StoreUint32(&db.header.tocStart, db.transaction.originalTocStart)
	atomic.StoreUint32(&db.header.entryStart, db.transaction.originalEntryStart)
	atomic.StoreUint32(&db.header.lgIndexStart, db.transaction.originalLgIndexStart)
	atomic.StoreUint32(&db.header.indexCapacity, db.transaction.originalIndexCapacity)

	// Restore the original index (all tables)
	db.indexes = make(map[uint8]map[uint32]uint32)
	for tableID, idx := range db.transaction.tempIndex {
		db.indexes[tableID] = make(map[uint32]uint32)
		for k, v := range idx {
			db.indexes[tableID][k] = v
		}
	}

	// Clean up the temporary file
	if err := db.transaction.tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close transaction temporary file: %w", err)
	}
	if err := os.Remove(db.transaction.tempFile.Name()); err != nil {
		return fmt.Errorf("failed to remove transaction temporary file: %w", err)
	}

	// Mark the transaction as rolled back
	db.transaction.state = TransactionRolledBack
	db.transaction = nil

	return nil
}

// rollbackTransactionAndReloadMMap rolls back the transaction and reloads mmap
// This is a convenience method that handles both operations safely
// IMPORTANT: Caller must hold db.lock before calling this method
func (db *Database[T]) rollbackTransactionAndReloadMMap() error {
	if err := db.rollbackTransactionLocked(); err != nil {
		return err
	}
	// ReloadMMap uses mlock, not db.lock, so this is safe
	return db.ReloadMMap()
}

// isInTransaction checks if a transaction is currently active
func (db *Database[T]) isInTransaction() bool {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.transaction != nil && db.transaction.state == TransactionActive
}

// isInTransactionLocked checks if a transaction is currently active
// IMPORTANT: Caller must hold db.lock (read or write) before calling this method
func (db *Database[T]) isInTransactionLocked() bool {
	return db.transaction != nil && db.transaction.state == TransactionActive
}
