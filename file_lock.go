package embeddb

import "sync"

var sharedWriteLocks sync.Map // map[string]*sync.Mutex

func getSharedWriteLock(path string) *sync.Mutex {
	if lock, ok := sharedWriteLocks.Load(path); ok {
		return lock.(*sync.Mutex)
	}

	newLock := &sync.Mutex{}
	actual, _ := sharedWriteLocks.LoadOrStore(path, newLock)
	return actual.(*sync.Mutex)
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
