package embeddb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"sync"
	"syscall"
)

const (
	EcCode           = byte(0x1B)
	RecordStartMark  = byte(0x02)
	RecordEndMark    = byte(0x03)
	valueStartMarker = byte(0x1E)
	valueEndMarker   = byte(0x1F)
)

type mapIndex struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newMapIndex() *mapIndex {
	return &mapIndex{
		data: make(map[string][]byte),
	}
}

func (mi *mapIndex) Set(key []byte, value []byte) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	keyCopy := make([]byte, len(key))
	valCopy := make([]byte, len(value))
	copy(keyCopy, key)
	copy(valCopy, value)
	mi.data[string(keyCopy)] = valCopy
}

func (mi *mapIndex) SetUint32(key []byte, value uint32) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	val := make([]byte, 4)
	binary.BigEndian.PutUint32(val, value)
	mi.data[string(keyCopy)] = val
}

func (mi *mapIndex) Get(key []byte) ([]byte, bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	val, ok := mi.data[string(key)]
	if !ok {
		return nil, false
	}
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy, true
}

func (mi *mapIndex) GetUint32(key []byte) (uint32, bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	val, ok := mi.data[string(key)]
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint32(val), true
}

func (mi *mapIndex) Delete(key []byte) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	delete(mi.data, string(key))
}

func (mi *mapIndex) Range(fn func(k []byte, v []byte) bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	for k, v := range mi.data {
		keyCopy := make([]byte, len(k))
		valCopy := make([]byte, len(v))
		copy(keyCopy, k)
		copy(valCopy, v)
		if !fn(keyCopy, valCopy) {
			return
		}
	}
}

func (mi *mapIndex) Size() int {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return len(mi.data)
}

type uint32MapIndex struct {
	mu   sync.RWMutex
	data map[string]uint32
}

func newUint32MapIndex() *uint32MapIndex {
	return &uint32MapIndex{
		data: make(map[string]uint32),
	}
}

func (mi *uint32MapIndex) Set(key string, value uint32) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.data[key] = value
}

func (mi *uint32MapIndex) Get(key string) (uint32, bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	val, ok := mi.data[key]
	return val, ok
}

func (mi *uint32MapIndex) Delete(key string) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	delete(mi.data, key)
}

func (mi *uint32MapIndex) Range(fn func(k string, v uint32) bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	for k, v := range mi.data {
		if !fn(k, v) {
			return
		}
	}
}

func (mi *uint32MapIndex) Size() int {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return len(mi.data)
}

func lockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("database is locked by another process")
	}
	return nil
}

func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

type tableCatalogEntry struct {
	ID           uint8
	Name         string
	LayoutHash   string
	NextRecordID uint32
	Dropped      bool
}

type tableCatalog map[string]*tableCatalogEntry

func (tc tableCatalog) GetTableID(name string) (uint8, bool) {
	entry := tc[name]
	if entry == nil || entry.Dropped {
		return 0, false
	}
	return entry.ID, true
}

func (tc tableCatalog) AddTable(name string, layoutHash string) uint8 {
	var maxID uint8
	for _, entry := range tc {
		if entry.ID > maxID && !entry.Dropped {
			maxID = entry.ID
		}
	}
	maxID++
	tc[name] = &tableCatalogEntry{
		ID:           maxID,
		Name:         name,
		LayoutHash:   layoutHash,
		NextRecordID: 1,
	}
	return maxID
}

type database struct {
	mu       sync.RWMutex
	file     *os.File
	filename string
	pkIndex  *mapIndex
	alloc    *allocator
	tableCat tableCatalog
	tx       *Transaction
}

func openDatabase(filename string, migrate bool) (*database, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	if err := lockFile(file); err != nil {
		file.Close()
		return nil, err
	}

	db := &database{
		filename: filename,
		file:     file,
		pkIndex:  newMapIndex(),
		alloc:    &allocator{},
		tableCat: make(tableCatalog),
	}

	stat, _ := file.Stat()
	if stat.Size() == 0 {
		db.alloc.Reset(4096, nil)
	}

	if err := db.load(); err != nil {
		unlockFile(file)
		file.Close()
		return nil, err
	}

	return db, nil
}

func (db *database) load() error {
	stat, err := db.file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		return nil
	}

	var header [64]byte
	if _, err := db.file.ReadAt(header[:], 0); err != nil {
		return err
	}

	if header[0] != EcCode || header[1] != RecordStartMark {
		return fmt.Errorf("invalid database header")
	}

	tableCount := binary.LittleEndian.Uint32(header[32:36])
	tocOffset := binary.LittleEndian.Uint32(header[36:40])
	nextOffset := binary.LittleEndian.Uint64(header[40:48])

	db.alloc.Reset(nextOffset, nil)

	if tableCount > 0 && tocOffset > 0 {
		var tocData []byte
		if int(tocOffset) < int(stat.Size()) {
			tocLen := int(stat.Size()) - int(tocOffset)
			if tocLen > 0 && tocLen < 1024*1024 {
				tocData = make([]byte, tocLen)
				_, err = db.file.ReadAt(tocData, int64(tocOffset))
				if err != nil {
					return err
				}
			}
		}

		if tocData != nil {
			db.tableCat = decodeTableCatalog(tocData)
		}
	}

	db.pkIndex.Range(func(k []byte, v []byte) bool {
		return true
	})

	recordStart := int64(64)
	if stat.Size() > recordStart && int64(tocOffset) > recordStart {
		for offset := recordStart; offset < int64(tocOffset); offset++ {
			hdrBuf := make([]byte, 12)
			_, err := db.file.ReadAt(hdrBuf, offset)
			if err != nil || hdrBuf[0] != EcCode || hdrBuf[1] != RecordStartMark {
				continue
			}

			recLen := binary.BigEndian.Uint32(hdrBuf[7:11])
			totalLen := 12 + int(recLen) + 2

			if int64(offset)+int64(totalLen) > int64(tocOffset) {
				break
			}

			recordBuf := make([]byte, totalLen)
			copy(recordBuf, hdrBuf)
			_, err = db.file.ReadAt(recordBuf[12:], offset+12)
			if err != nil {
				break
			}

			tableID := recordBuf[2]
			recordID := binary.BigEndian.Uint32(recordBuf[3:7])
			active := recordBuf[11]

			if active == 1 {
				key := encodePKForIndex(tableID, recordID)
				val := make([]byte, 8)
				binary.BigEndian.PutUint64(val, uint64(offset))
				db.pkIndex.Set(key, val)
			}

			offset += int64(totalLen) - 1
		}
	}

	return nil
}

func decodeTableCatalog(data []byte) tableCatalog {
	tc := make(tableCatalog)
	if len(data) < 4 {
		return tc
	}

	offset := 0
	count := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	for i := uint32(0); i < count && offset < len(data); i++ {
		if offset+5 > len(data) {
			break
		}

		id := data[offset]
		offset++

		nameLen := int(data[offset])
		offset++
		if offset+nameLen > len(data) {
			break
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen

		if offset+8 > len(data) {
			break
		}
		hashLen := int(data[offset])
		offset++
		if offset+hashLen > len(data) {
			break
		}
		layoutHash := string(data[offset : offset+hashLen])
		offset += hashLen

		nextRecID := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		tc[name] = &tableCatalogEntry{
			ID:           id,
			Name:         name,
			LayoutHash:   layoutHash,
			NextRecordID: nextRecID,
		}
	}

	return tc
}

func (db *database) flush() error {
	header := make([]byte, 64)
	header[0] = EcCode
	header[1] = RecordStartMark
	copy(header[2:34], []byte("embeddb v1.0"))
	binary.LittleEndian.PutUint32(header[32:36], uint32(len(db.tableCat)))
	binary.LittleEndian.PutUint64(header[40:48], db.alloc.nextOffset)

	encodedCat := db.encodeTableCatalog()
	recordStart := int64(db.alloc.nextOffset)
	if recordStart < 64 {
		recordStart = 64
	}

	type recordInfo struct {
		offset uint64
		key    []byte
		length int
	}

	var records []recordInfo
	db.pkIndex.Range(func(k []byte, v []byte) bool {
		off, _ := binary.ReadUvarint(bytes.NewReader(v))
		records = append(records, recordInfo{
			offset: off,
			key:    k,
		})
		return true
	})

	slices.SortFunc(records, func(a, b recordInfo) int {
		return slices.Compare(a.key, b.key)
	})

	recordDataStart := recordStart
	updatedIndex := make(map[string][]byte)

	for _, rec := range records {
		hdrBuf := make([]byte, 12)
		_, err := db.file.ReadAt(hdrBuf, int64(rec.offset))
		if err != nil {
			continue
		}
		if hdrBuf[0] != EcCode || hdrBuf[1] != RecordStartMark {
			continue
		}
		recLen := binary.BigEndian.Uint32(hdrBuf[7:11])
		totalLen := 12 + int(recLen) + 2

		recData := make([]byte, totalLen)
		copy(recData, hdrBuf)

		if totalLen > 12 {
			n, err := db.file.ReadAt(recData[12:], int64(rec.offset)+12)
			if err != nil || n < totalLen-12 {
				continue
			}
		}

		db.file.WriteAt(recData, recordDataStart)

		newVal := make([]byte, 8)
		binary.BigEndian.PutUint64(newVal, uint64(recordDataStart))
		updatedIndex[string(rec.key)] = newVal

		recordDataStart += int64(totalLen)
	}

	for k, v := range updatedIndex {
		db.pkIndex.Set([]byte(k), v)
	}

	catOffset := recordDataStart

	binary.LittleEndian.PutUint32(header[36:40], uint32(catOffset))

	if _, err := db.file.WriteAt(header, 0); err != nil {
		return err
	}

	if _, err := db.file.WriteAt(encodedCat, catOffset); err != nil {
		return err
	}

	newSize := catOffset + int64(len(encodedCat))
	if err := db.file.Truncate(newSize); err != nil {
		return err
	}

	if _, err := db.file.WriteAt(header, 0); err != nil {
		return err
	}

	return db.file.Sync()
}

func (db *database) encodeTableCatalog() []byte {
	count := uint32(0)
	for _, entry := range db.tableCat {
		if !entry.Dropped {
			count++
		}
	}

	var buf []byte
	buf = binary.LittleEndian.AppendUint32(buf, count)

	for _, entry := range db.tableCat {
		if entry.Dropped {
			continue
		}
		buf = append(buf, entry.ID)
		buf = append(buf, byte(len(entry.Name)))
		buf = append(buf, []byte(entry.Name)...)
		buf = append(buf, byte(len(entry.LayoutHash)))
		buf = append(buf, []byte(entry.LayoutHash)...)
		buf = binary.LittleEndian.AppendUint32(buf, entry.NextRecordID)
	}

	return buf
}

func (db *database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.flush(); err != nil {
		return err
	}

	unlockFile(db.file)
	return db.file.Close()
}

func (db *database) Use(tableName string) (*tableCatalogEntry, error) {
	entry, exists := db.tableCat[tableName]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}
	if entry.Dropped {
		return nil, fmt.Errorf("table was dropped: %s", tableName)
	}
	return entry, nil
}

func (db *database) Vacuum() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	newFile, err := os.OpenFile(db.filename+".vacuum", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer os.Remove(db.filename + ".vacuum")

	stat, _ := db.file.Stat()
	if stat == nil {
		newFile.Close()
		return fmt.Errorf("could not stat database file")
	}

	header := make([]byte, 64)
	header[0] = EcCode
	header[1] = RecordStartMark
	copy(header[2:34], []byte("embeddb v1.0"))
	binary.LittleEndian.PutUint32(header[32:36], uint32(len(db.tableCat)))
	binary.LittleEndian.PutUint64(header[40:48], db.alloc.nextOffset)

	if _, err := newFile.Write(header); err != nil {
		newFile.Close()
		return err
	}

	newOffset := int64(64)
	newPkIndex := newMapIndex()

	entries := make([]*tableCatalogEntry, 0)
	for _, entry := range db.tableCat {
		if !entry.Dropped {
			entries = append(entries, entry)
		}
	}

	for _, entry := range entries {
		ids := make([]uint32, 0)
		db.pkIndex.Range(func(k []byte, v []byte) bool {
			if len(k) >= 2 && k[0] == entry.ID {
				ids = append(ids, binary.BigEndian.Uint32(k[1:5]))
			}
			return true
		})

		for _, recID := range ids {
			key := encodePKForIndex(entry.ID, recID)
			val, ok := db.pkIndex.Get(key)
			if !ok {
				continue
			}

			offset := binary.BigEndian.Uint64(val)
			recordBuf := make([]byte, 4096)
			n, err := db.file.ReadAt(recordBuf, int64(offset))
			if err != nil || n < 14 {
				continue
			}

			if recordBuf[0] != EcCode || recordBuf[1] != RecordStartMark {
				continue
			}

			active := recordBuf[11]
			if active != 1 {
				continue
			}

			recLen := binary.BigEndian.Uint32(recordBuf[7:11])
			totalLen := 12 + int(recLen) + 2

			if totalLen <= n {
				record := recordBuf[:totalLen]
				if _, err := newFile.Write(record); err != nil {
					newFile.Close()
					return err
				}

				newVal := make([]byte, 8)
				binary.BigEndian.PutUint64(newVal, uint64(newOffset))
				newPkIndex.Set(key, newVal)

				newOffset += int64(totalLen)
			}
		}
	}

	tocData := db.encodeTableCatalog()
	if _, err := newFile.Write(tocData); err != nil {
		newFile.Close()
		return err
	}

	newPkIndex.Range(func(k []byte, v []byte) bool {
		db.pkIndex.Set(k, v)
		return true
	})

	newFile.Close()
	db.file.Close()

	if err := os.Rename(db.filename+".vacuum", db.filename); err != nil {
		return err
	}

	db.file, err = os.OpenFile(db.filename, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	return lockFile(db.file)
}

type Transaction struct {
	db        *database
	snapshot  *mapIndex
	committed bool
}

func (db *database) Begin() *Transaction {
	db.mu.Lock()
	defer db.mu.Unlock()

	snapshot := newMapIndex()
	db.pkIndex.Range(func(k []byte, v []byte) bool {
		keyCopy := make([]byte, len(k))
		valCopy := make([]byte, len(v))
		copy(keyCopy, k)
		copy(valCopy, v)
		snapshot.Set(keyCopy, valCopy)
		return true
	})

	tx := &Transaction{
		db:       db,
		snapshot: snapshot,
	}
	db.tx = tx
	return tx
}

func (tx *Transaction) Commit() error {
	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	tx.committed = true
	tx.db.tx = nil
	return nil
}

func (tx *Transaction) Rollback() error {
	if tx.committed {
		return fmt.Errorf("cannot rollback committed transaction")
	}

	var keys [][]byte
	tx.db.pkIndex.Range(func(k []byte, v []byte) bool {
		keys = append(keys, k)
		return true
	})
	for _, k := range keys {
		tx.db.pkIndex.Delete(k)
	}

	tx.snapshot.Range(func(k []byte, v []byte) bool {
		tx.db.pkIndex.Set(k, v)
		return true
	})
	tx.db.tx = nil
	return nil
}
