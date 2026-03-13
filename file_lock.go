package embeddb

import (
	"path/filepath"
	"sync"
	"sync/atomic"
)

type sharedFileState struct {
	writeLock sync.Mutex
	refCount  int32
}

var sharedFileStates sync.Map // map[string]*sharedFileState

func normalizeFileKey(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(abs)
}

func getSharedFileState(path string) *sharedFileState {
	if state, ok := sharedFileStates.Load(path); ok {
		return state.(*sharedFileState)
	}

	newState := &sharedFileState{}
	actual, _ := sharedFileStates.LoadOrStore(path, newState)
	return actual.(*sharedFileState)
}

func (db *Database[T]) registerHandle(path string) {
	key := normalizeFileKey(path)
	db.fileKey = key
	db.sharedState = getSharedFileState(key)
	atomic.AddInt32(&db.sharedState.refCount, 1)
	db.writeLock = &db.sharedState.writeLock
}

func (db *Database[T]) unregisterHandle() {
	if db.sharedState == nil {
		return
	}
	atomic.AddInt32(&db.sharedState.refCount, -1)
}

func (db *Database[T]) needsCrossHandleSync() bool {
	if db.sharedState == nil {
		return false
	}
	return atomic.LoadInt32(&db.sharedState.refCount) > 1
}

func (db *Database[T]) syncStateFromDiskForWrite() error {
	if err := db.decodeHeader(); err != nil {
		return err
	}

	if err := db.ReloadMMap(); err != nil {
		return err
	}

	db.lock.Lock()
	db.indexes = make(map[uint8]map[uint32]uint32)
	db.lock.Unlock()

	if err := db.ReadIndex(); err != nil {
		return err
	}

	if err := db.readTableCatalog(); err != nil {
		return err
	}

	return nil
}
