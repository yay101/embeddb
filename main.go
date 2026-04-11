package embeddb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"sync"
	"syscall"
	"time"

	embedcore "github.com/yay101/embeddbcore"
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

func (mi *mapIndex) RootOffset() uint64 {
	return 0
}

func (mi *mapIndex) SetRootOffset(off uint64) {
}

type pkIndexInterface interface {
	Set(key []byte, value []byte)
	Get(key []byte) ([]byte, bool)
	Delete(key []byte)
	Range(fn func(k []byte, v []byte) bool)
	Size() int
	RootOffset() uint64
	SetRootOffset(off uint64)
}

type uint32MapIndex struct {
	mu   sync.RWMutex
	data map[string][]uint32
}

func newUint32MapIndex() *uint32MapIndex {
	return &uint32MapIndex{
		data: make(map[string][]uint32),
	}
}

func (mi *uint32MapIndex) Set(key string, value uint32) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.data[key] = append(mi.data[key], value)
}

func (mi *uint32MapIndex) Get(key string) (uint32, bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	vals, ok := mi.data[key]
	if !ok || len(vals) == 0 {
		return 0, false
	}
	return vals[0], true
}

func (mi *uint32MapIndex) GetAll(key string) ([]uint32, bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	vals, ok := mi.data[key]
	if !ok || len(vals) == 0 {
		return nil, false
	}
	return vals, true
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
		if len(v) > 0 {
			if !fn(k, v[0]) {
				return
			}
		}
	}
}

func (mi *uint32MapIndex) Size() int {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return len(mi.data)
}

func (mi *uint32MapIndex) SortedKeys() []string {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	keys := make([]string, 0, len(mi.data))
	for k := range mi.data {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

type uint32IndexInterface interface {
	Set(key string, value uint32)
	Get(key string) (uint32, bool)
	GetAll(key string) ([]uint32, bool)
	Delete(key string)
	Range(fn func(k string, v uint32) bool)
	SortedKeys() []string
	Size() int
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
	RecordCount  uint32
	MaxVersions  uint8
	BTreeRoot    uint64
}

type tableCatalog map[string]*tableCatalogEntry

type VersionInfo struct {
	Version   uint32
	Offset    uint64
	CreatedAt int64
}

type versionIndex struct {
	mu       sync.RWMutex
	data     map[string][]VersionInfo
	snapshot map[string][]VersionInfo
}

func newVersionIndex() *versionIndex {
	return &versionIndex{
		data: make(map[string][]VersionInfo),
	}
}

func (vi *versionIndex) Add(key []byte, version uint32, offset uint64, createdAt int64) {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	keyStr := string(key)
	versions := vi.data[keyStr]
	versions = append(versions, VersionInfo{
		Version:   version,
		Offset:    offset,
		CreatedAt: createdAt,
	})
	vi.data[keyStr] = versions
}

func (vi *versionIndex) GetVersions(key []byte) []VersionInfo {
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	keyStr := string(key)
	versions := vi.data[keyStr]
	result := make([]VersionInfo, len(versions))
	copy(result, versions)
	return result
}

func (vi *versionIndex) GetVersion(key []byte, version uint32) (VersionInfo, bool) {
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	keyStr := string(key)
	versions := vi.data[keyStr]
	for _, v := range versions {
		if v.Version == version {
			return v, true
		}
	}
	return VersionInfo{}, false
}

func (vi *versionIndex) RemoveVersion(key []byte, version uint32) {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	keyStr := string(key)
	versions := vi.data[keyStr]
	for i, v := range versions {
		if v.Version == version {
			versions = append(versions[:i], versions[i+1:]...)
			break
		}
	}
	vi.data[keyStr] = versions
}

func (vi *versionIndex) RemoveKey(key []byte) {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	delete(vi.data, string(key))
}

func (vi *versionIndex) Range(fn func(key []byte, versions []VersionInfo) bool) {
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	for k, versions := range vi.data {
		keyCopy := []byte(k)
		versionsCopy := make([]VersionInfo, len(versions))
		copy(versionsCopy, versions)
		if !fn(keyCopy, versionsCopy) {
			return
		}
	}
}

func (vi *versionIndex) Clear() {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.data = make(map[string][]VersionInfo)
}

func (vi *versionIndex) RootOffset() uint64 {
	return 0
}

func (vi *versionIndex) SetRootOffset(off uint64) {
}

func (vi *versionIndex) Snapshot() {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.snapshot = make(map[string][]VersionInfo)
	for k, versions := range vi.data {
		versionsCopy := make([]VersionInfo, len(versions))
		copy(versionsCopy, versions)
		vi.snapshot[k] = versionsCopy
	}
}

func (vi *versionIndex) RestoreSnapshot() {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	if vi.snapshot != nil {
		vi.data = vi.snapshot
		vi.snapshot = nil
	}
}

func (vi *versionIndex) ClearSnapshot() {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.snapshot = nil
}

func (db *database) encodeVersionCatalog() []byte {
	var buf []byte

	count := uint32(0)
	vi := db.versionIndex
	vi.mu.RLock()
	for _, versions := range vi.data {
		count += uint32(len(versions))
	}
	vi.mu.RUnlock()

	buf = binary.LittleEndian.AppendUint32(buf, count)

	vi.Range(func(key []byte, versions []VersionInfo) bool {
		for _, v := range versions {
			buf = embedcore.EncodeUvarint(buf, uint64(len(key)))
			buf = append(buf, key...)
			buf = binary.LittleEndian.AppendUint32(buf, v.Version)
			buf = binary.LittleEndian.AppendUint64(buf, v.Offset)
			buf = binary.LittleEndian.AppendUint64(buf, uint64(v.CreatedAt))
		}
		return true
	})

	return buf
}

func decodeVersionCatalog(data []byte, vi *versionIndex) {
	if len(data) < 4 {
		return
	}

	offset := 0
	count := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	for i := uint32(0); i < count && offset < len(data); i++ {
		if offset >= len(data) {
			break
		}
		keyLen, remaining, err := embedcore.DecodeUvarint(data[offset:])
		if err != nil {
			break
		}
		consumed := len(data[offset:]) - len(remaining)
		offset += consumed
		if offset+int(keyLen) > len(data) {
			break
		}
		key := make([]byte, keyLen)
		copy(key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)

		if offset+20 > len(data) {
			break
		}
		version := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		recordOffset := binary.LittleEndian.Uint64(data[offset:])
		offset += 8
		createdAt := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		vi.Add(key, version, recordOffset, createdAt)
	}
}

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
	mu            sync.RWMutex
	file          *os.File
	filename      string
	pkIndex       pkIndexInterface
	alloc         *allocator
	tableCat      tableCatalog
	tx            *Transaction
	parent        *DB
	versionIndex  *versionIndex
	pkIndexBTRoot uint64
}

func openDatabase(filename string, migrate bool, parent *DB) (*database, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	if err := lockFile(file); err != nil {
		file.Close()
		return nil, err
	}

	alloc := newAllocator(file)

	stat, _ := file.Stat()
	var pkIndexBTRoot uint64
	if stat.Size() > 0 {
		headerBuf := make([]byte, 64)
		if _, err := file.ReadAt(headerBuf, 0); err == nil {
			if headerBuf[0] == EcCode && headerBuf[1] == RecordStartMark {
				pkIndexBTRoot = binary.LittleEndian.Uint64(headerBuf[56:64])
			}
		}
	}

	db := &database{
		filename:      filename,
		file:          file,
		alloc:         alloc,
		tableCat:      make(tableCatalog),
		tx:            nil,
		parent:        parent,
		versionIndex:  newVersionIndex(),
		pkIndexBTRoot: pkIndexBTRoot,
	}

	if stat.Size() == 0 {
		db.alloc.Reset(4096, nil)
	}

	var pkIdx pkIndexInterface
	btIdx, err := newBtreeMapIndex(db, pkIndexBTRoot)
	if err != nil {
		unlockFile(file)
		file.Close()
		return nil, err
	}
	pkIdx = btIdx
	db.pkIndex = pkIdx

	if err := db.load(); err != nil {
		if rebuildErr := db.rebuildIndexFromScan(); rebuildErr != nil {
			unlockFile(file)
			file.Close()
			return nil, fmt.Errorf("failed to load index: %w, failed to rebuild: %v", err, rebuildErr)
		}
	}

	if bti, ok := db.pkIndex.(*btreeMapIndex); ok {
		db.pkIndexBTRoot = bti.RootOffset()
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
	var versionCatalogOffset uint32
	if len(header) >= 56 {
		versionCatalogOffset = binary.LittleEndian.Uint32(header[48:56])
	}
	var pkIndexBTRoot uint64
	if len(header) >= 64 {
		pkIndexBTRoot = binary.LittleEndian.Uint64(header[56:64])
	}
	db.pkIndexBTRoot = pkIndexBTRoot

	db.alloc.Reset(nextOffset, nil)

	if tableCount > 0 && tocOffset > 0 && int64(tocOffset) < stat.Size() {
		var tocData []byte
		var versionCatalogData []byte

		if versionCatalogOffset > 0 && int64(versionCatalogOffset) > int64(tocOffset) && int64(versionCatalogOffset) < stat.Size() {
			tocLen := int(versionCatalogOffset) - int(tocOffset)
			if tocLen > 0 && tocLen < 1024*1024 {
				tocData = make([]byte, tocLen)
				_, err = db.file.ReadAt(tocData, int64(tocOffset))
				if err != nil {
					return err
				}
			}
			versionLen := int(stat.Size()) - int(versionCatalogOffset)
			if versionLen > 0 && versionLen < 10*1024*1024 {
				versionCatalogData = make([]byte, versionLen)
				_, err = db.file.ReadAt(versionCatalogData, int64(versionCatalogOffset))
				if err != nil {
					return err
				}
			}
		} else if int(tocOffset) < int(stat.Size()) {
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

		if versionCatalogData != nil {
			decodeVersionCatalog(versionCatalogData, db.versionIndex)
		}
	}

	db.pkIndex.Range(func(k []byte, v []byte) bool {
		return true
	})

	recordStart := int64(64)
	endOffset := int64(tocOffset)
	if versionCatalogOffset > 0 && int64(versionCatalogOffset) < stat.Size() {
		endOffset = int64(versionCatalogOffset)
	}
	if endOffset == 0 || endOffset > stat.Size() {
		endOffset = int64(tocOffset)
	}
	if endOffset > stat.Size() {
		endOffset = stat.Size()
	}

	if stat.Size() > recordStart && endOffset > recordStart {
		for offset := recordStart; offset < endOffset; offset++ {
			hdrBuf := make([]byte, 12)
			_, err := db.file.ReadAt(hdrBuf, offset)
			if err != nil || hdrBuf[0] != EcCode || hdrBuf[1] != RecordStartMark {
				continue
			}

			recLen := binary.BigEndian.Uint32(hdrBuf[7:11])
			totalLen := 12 + int(recLen) + 2

			if int64(offset)+int64(totalLen) > endOffset {
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

func (db *database) rebuildIndexFromScan() error {
	stat, err := db.file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		db.alloc.Reset(4096, nil)
		return nil
	}

	fileSize := stat.Size()
	var maxOffset uint64 = 64

	offset := int64(64)
	for offset < fileSize-12 {
		hdrBuf := make([]byte, 12)
		n, err := db.file.ReadAt(hdrBuf, offset)
		if err != nil || n < 12 {
			offset++
			continue
		}

		if hdrBuf[0] != EcCode || hdrBuf[1] != RecordStartMark {
			offset++
			continue
		}

		recLen := binary.BigEndian.Uint32(hdrBuf[7:11])
		totalLen := 12 + int(recLen) + 2

		if totalLen < 14 || offset+int64(totalLen) > fileSize {
			offset++
			continue
		}

		recordBuf := make([]byte, totalLen)
		copy(recordBuf, hdrBuf)
		_, err = db.file.ReadAt(recordBuf[12:], offset+12)
		if err != nil {
			offset++
			continue
		}

		if recordBuf[totalLen-2] == EcCode && recordBuf[totalLen-1] == RecordEndMark {
			tableID := recordBuf[2]
			recordID := binary.BigEndian.Uint32(recordBuf[3:7])
			active := recordBuf[11]

			if active == 1 {
				key := encodePKForIndex(tableID, recordID)
				val := make([]byte, 8)
				binary.BigEndian.PutUint64(val, uint64(offset))
				db.pkIndex.Set(key, val)

				endOffset := uint64(offset) + uint64(totalLen)
				if endOffset > maxOffset {
					maxOffset = endOffset
				}
				offset += int64(totalLen)
				continue
			}
		}
		offset++
	}

	db.alloc.Reset(maxOffset, nil)

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

		recordCount := uint32(0)
		if offset+4 <= len(data) {
			recordCount = binary.LittleEndian.Uint32(data[offset:])
			offset += 4
		}

		maxVersions := uint8(0)
		if offset < len(data) {
			maxVersions = data[offset]
			offset++
		}

		tc[name] = &tableCatalogEntry{
			ID:           id,
			Name:         name,
			LayoutHash:   layoutHash,
			NextRecordID: nextRecID,
			RecordCount:  recordCount,
			MaxVersions:  maxVersions,
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
	encodedVersionCat := db.encodeVersionCatalog()

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
		off := binary.BigEndian.Uint64(v)
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
	updatedVersionOffsets := make(map[string]map[uint32]uint64)

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

		versionKey := string(rec.key)
		versions := db.versionIndex.GetVersions(rec.key)
		for _, v := range versions {
			if v.Offset == rec.offset {
				if updatedVersionOffsets[versionKey] == nil {
					updatedVersionOffsets[versionKey] = make(map[uint32]uint64)
				}
				updatedVersionOffsets[versionKey][v.Version] = uint64(recordDataStart)
				break
			}
		}

		recordDataStart += int64(totalLen)
	}

	for key, versionOffsets := range updatedVersionOffsets {
		versions := db.versionIndex.GetVersions([]byte(key))
		for _, v := range versions {
			if newOffset, ok := versionOffsets[v.Version]; ok {
				db.versionIndex.RemoveVersion([]byte(key), v.Version)
				db.versionIndex.Add([]byte(key), v.Version, newOffset, v.CreatedAt)
			}
		}
	}

	// Rebuild the B-tree from scratch so its nodes are allocated contiguously
	// after the compacted records.  The old B-tree nodes are scattered
	// throughout the file and will be discarded by the truncation below.
	if bti, ok := db.pkIndex.(*btreeMapIndex); ok {
		bti.bt.Close()
	}

	// Reset allocator so new B-tree nodes are allocated right after the records.
	// Ensure a minimum offset so we don't overwrite the file header.
	btreeStart := uint64(recordDataStart)
	if btreeStart < 4096 {
		btreeStart = 4096
	}
	db.alloc.Reset(btreeStart, nil)
	// Force actualSize to match so Allocate extends from the right place.
	db.file.Truncate(int64(btreeStart))
	db.alloc.SetFile(db.file)

	newBtIdx, err := newBtreeMapIndex(db, 0)
	if err != nil {
		return err
	}
	for k, v := range updatedIndex {
		newBtIdx.Set([]byte(k), v)
	}
	newBtIdx.bt.Sync()
	db.pkIndex = newBtIdx
	db.pkIndexBTRoot = newBtIdx.RootOffset()

	// The allocator's nextOffset now sits past the last B-tree node.
	catOffset := int64(db.alloc.nextOffset)

	binary.LittleEndian.PutUint32(header[36:40], uint32(catOffset))

	if _, err := db.file.WriteAt(header, 0); err != nil {
		return err
	}

	if _, err := db.file.WriteAt(encodedCat, catOffset); err != nil {
		return err
	}

	versionCatOffset := catOffset + int64(len(encodedCat))
	binary.LittleEndian.PutUint32(header[48:56], uint32(versionCatOffset))
	binary.LittleEndian.PutUint64(header[56:64], db.pkIndexBTRoot)
	binary.LittleEndian.PutUint64(header[40:48], db.alloc.nextOffset)

	if _, err := db.file.WriteAt(encodedVersionCat, versionCatOffset); err != nil {
		return err
	}

	newSize := versionCatOffset + int64(len(encodedVersionCat))
	if err := db.file.Truncate(newSize); err != nil {
		return err
	}

	if _, err := db.file.WriteAt(header, 0); err != nil {
		return err
	}

	// Re-open the B-tree so it re-mmaps the final file.
	if bti, ok := db.pkIndex.(*btreeMapIndex); ok {
		bti.bt.Close()
	}
	reopenedBtIdx, err := newBtreeMapIndex(db, db.pkIndexBTRoot)
	if err != nil {
		return err
	}
	db.pkIndex = reopenedBtIdx

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
		buf = binary.LittleEndian.AppendUint32(buf, entry.RecordCount)
		buf = append(buf, entry.MaxVersions)
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

func (db *database) autoSync() {
	if db.parent == nil {
		return
	}

	db.parent.lock.Lock()
	defer db.parent.lock.Unlock()

	if db.parent.closed {
		return
	}

	db.parent.writeCount++

	syncNeeded := false
	if db.parent.syncThreshold > 0 && db.parent.writeCount >= db.parent.syncThreshold {
		syncNeeded = true
	} else if db.parent.idleThreshold > 0 && time.Since(db.parent.lastSync) >= db.parent.idleThreshold {
		syncNeeded = true
	}

	if syncNeeded {
		for _, t := range db.parent.tables {
			t.file.Sync()
		}
		db.parent.writeCount = 0
		db.parent.lastSync = time.Now()
	}
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
	newVersionIndex := newVersionIndex()

	entries := make([]*tableCatalogEntry, 0)
	for _, entry := range db.tableCat {
		if !entry.Dropped {
			entries = append(entries, entry)
		}
	}

	offsetMap := make(map[uint64]uint64)

	for _, entry := range entries {
		var keys [][]byte
		db.pkIndex.Range(func(k []byte, v []byte) bool {
			if len(k) >= 2 && k[0] == entry.ID {
				keyCopy := make([]byte, len(k))
				copy(keyCopy, k)
				keys = append(keys, keyCopy)
			}
			return true
		})

		for _, key := range keys {
			val, ok := db.pkIndex.Get(key)
			if !ok {
				continue
			}

			offset := binary.BigEndian.Uint64(val)
			recordBuf := make([]byte, 4096)
			n, _ := db.file.ReadAt(recordBuf, int64(offset))
			if n < 14 {
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

			if totalLen <= n && totalLen >= 14 {
				record := recordBuf[:totalLen]
				if _, err := newFile.Write(record); err != nil {
					newFile.Close()
					return err
				}

				newVal := make([]byte, 8)
				binary.BigEndian.PutUint64(newVal, uint64(newOffset))
				newPkIndex.Set(key, newVal)

				versions := db.versionIndex.GetVersions(key)
				for _, v := range versions {
					if v.Offset == offset {
						newVersionIndex.Add(key, v.Version, uint64(newOffset), v.CreatedAt)
						break
					}
				}

				offsetMap[offset] = uint64(newOffset)
				newOffset += int64(totalLen)
			}
		}
	}

	db.versionIndex.Range(func(key []byte, versions []VersionInfo) bool {
		for _, v := range versions {
			if newOffset, ok := offsetMap[v.Offset]; ok {
				existing := newVersionIndex.GetVersions(key)
				found := false
				for _, ev := range existing {
					if ev.Version == v.Version {
						found = true
						break
					}
				}
				if !found {
					newVersionIndex.Add(key, v.Version, newOffset, v.CreatedAt)
				}
			}
		}
		return true
	})

	tocData := db.encodeTableCatalog()
	tocOffset := newOffset

	versionCatData := db.encodeVersionCatalog()
	versionCatOffset := tocOffset + int64(len(tocData))

	binary.LittleEndian.PutUint32(header[36:40], uint32(tocOffset))
	binary.LittleEndian.PutUint32(header[48:56], uint32(versionCatOffset))
	binary.LittleEndian.PutUint64(header[40:48], uint64(versionCatOffset+int64(len(versionCatData))))

	if _, err := newFile.WriteAt(header, 0); err != nil {
		newFile.Close()
		return err
	}

	if _, err := newFile.Write(tocData); err != nil {
		newFile.Close()
		return err
	}

	if _, err := newFile.Write(versionCatData); err != nil {
		newFile.Close()
		return err
	}

	db.versionIndex = newVersionIndex
	db.alloc.Reset(uint64(versionCatOffset+int64(len(versionCatData))), nil)

	newFile.Close()
	db.file.Close()

	if err := os.Rename(db.filename+".vacuum", db.filename); err != nil {
		return err
	}

	db.file, err = os.OpenFile(db.filename, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	if err := lockFile(db.file); err != nil {
		return err
	}

	db.alloc.SetFile(db.file)

	db.pkIndex = newMapIndex()
	db.pkIndexBTRoot = 0

	newPkIndex.Range(func(k []byte, v []byte) bool {
		db.pkIndex.Set(k, v)
		return true
	})

	return nil
}

type Transaction struct {
	db                   *database
	snapshot             *mapIndex
	pkRootSnapshot       uint64
	versionIndexSnapshot map[string][]VersionInfo
	committed            bool
	recordCounts         map[string]uint32
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

	pkRootSnapshot := db.pkIndexBTRoot

	db.versionIndex.Snapshot()
	versionSnap := make(map[string][]VersionInfo)
	db.versionIndex.Range(func(k []byte, versions []VersionInfo) bool {
		versionsCopy := make([]VersionInfo, len(versions))
		copy(versionsCopy, versions)
		versionSnap[string(k)] = versionsCopy
		return true
	})

	tx := &Transaction{
		db:                   db,
		snapshot:             snapshot,
		pkRootSnapshot:       pkRootSnapshot,
		versionIndexSnapshot: versionSnap,
		recordCounts:         make(map[string]uint32),
	}
	db.tx = tx
	return tx
}

func (tx *Transaction) Commit() error {
	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	tx.committed = true
	tx.db.versionIndex.ClearSnapshot()
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

	tx.db.pkIndexBTRoot = tx.pkRootSnapshot
	if bti, ok := tx.db.pkIndex.(*btreeMapIndex); ok {
		bti.SetRootOffset(tx.pkRootSnapshot)
	}

	tx.db.versionIndex.Clear()
	for k, versions := range tx.versionIndexSnapshot {
		versionsCopy := make([]VersionInfo, len(versions))
		copy(versionsCopy, versions)
		tx.db.versionIndex.data[k] = versionsCopy
	}

	for tableName, delta := range tx.recordCounts {
		if entry := tx.db.tableCat[tableName]; entry != nil {
			if entry.RecordCount >= delta {
				entry.RecordCount -= delta
			}
		}
	}

	tx.db.tx = nil
	return nil
}

func migrateTable(db *database, table *tableCatalogEntry, newLayout *embedcore.StructLayout) error {
	type migrateRecord struct {
		key    []byte
		offset uint64
	}

	var records []migrateRecord

	db.pkIndex.Range(func(k []byte, v []byte) bool {
		if len(k) >= 2 && k[0] == table.ID {
			offset := binary.BigEndian.Uint64(v)
			records = append(records, migrateRecord{
				key:    k,
				offset: offset,
			})
		}
		return true
	})

	if len(records) == 0 {
		return nil
	}

	var maxOffset uint64 = 64

	for _, rec := range records {
		hdrBuf := make([]byte, 12)
		_, err := db.file.ReadAt(hdrBuf, int64(rec.offset))
		if err != nil || hdrBuf[0] != EcCode || hdrBuf[1] != RecordStartMark {
			continue
		}

		recLen := binary.BigEndian.Uint32(hdrBuf[7:11])
		totalLen := 12 + int(recLen) + 2

		recordBuf := make([]byte, totalLen)
		copy(recordBuf, hdrBuf)
		_, err = db.file.ReadAt(recordBuf[12:], int64(rec.offset)+12)
		if err != nil {
			continue
		}

		recordID := binary.BigEndian.Uint32(recordBuf[3:7])
		active := recordBuf[11]

		if active != 1 {
			continue
		}

		encoded := recordBuf[12 : len(recordBuf)-2]

		var newEncoded []byte
		for _, field := range newLayout.FieldOffsets {
			if field.IsStruct && !field.IsTime {
				continue
			}

			key := field.Key
			startIdx := bytes.Index(encoded, []byte{key, valueStartMarker})
			if startIdx == -1 {
				continue
			}

			startIdx += 2
			endIdx := bytes.Index(encoded[startIdx:], []byte{valueEndMarker})
			if endIdx == -1 {
				continue
			}

			fieldData := encoded[startIdx : startIdx+endIdx]

			newEncoded = append(newEncoded, key)
			newEncoded = append(newEncoded, valueStartMarker)
			newEncoded = append(newEncoded, fieldData...)
			newEncoded = append(newEncoded, valueEndMarker)
		}

		if len(newEncoded) == 0 {
			continue
		}

		newHeader := make([]byte, 12)
		newHeader[0] = EcCode
		newHeader[1] = RecordStartMark
		newHeader[2] = table.ID
		binary.BigEndian.PutUint32(newHeader[3:7], recordID)
		binary.BigEndian.PutUint32(newHeader[7:11], uint32(len(newEncoded)))
		newHeader[11] = 1

		newFooter := []byte{EcCode, RecordEndMark}

		newRecord := make([]byte, 0, len(newHeader)+len(newEncoded)+len(newFooter))
		newRecord = append(newRecord, newHeader...)
		newRecord = append(newRecord, newEncoded...)
		newRecord = append(newRecord, newFooter...)

		newOffset, _ := db.alloc.Allocate(uint64(len(newRecord)))
		db.file.WriteAt(newRecord, int64(newOffset))

		newVal := make([]byte, 8)
		binary.BigEndian.PutUint64(newVal, newOffset)
		db.pkIndex.Set(rec.key, newVal)

		endOffset := newOffset + uint64(len(newRecord))
		if endOffset > maxOffset {
			maxOffset = endOffset
		}
	}

	db.alloc.Reset(maxOffset, nil)

	return nil
}
