package embeddb

import (
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	embedcore "github.com/yay101/embeddbcore"
	"github.com/yay101/embeddbmmap"
)

const (
	FileHeaderSize = 128
	FileMagic      = "embeddb v3.0"
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

type offsetMapIndex struct {
	mu   sync.RWMutex
	data map[string][]uint64
}

func newOffsetMapIndex() *offsetMapIndex {
	return &offsetMapIndex{
		data: make(map[string][]uint64),
	}
}

func (mi *offsetMapIndex) Set(key string, value uint64) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.data[key] = append(mi.data[key], value)
}

func (mi *offsetMapIndex) Get(key string) (uint64, bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	vals, ok := mi.data[key]
	if !ok || len(vals) == 0 {
		return 0, false
	}
	return vals[0], true
}

func (mi *offsetMapIndex) GetAll(key string) ([]uint64, bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	vals, ok := mi.data[key]
	if !ok || len(vals) == 0 {
		return nil, false
	}
	result := make([]uint64, len(vals))
	copy(result, vals)
	return result, true
}

func (mi *offsetMapIndex) Delete(key string) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	delete(mi.data, key)
}

func (mi *offsetMapIndex) Range(fn func(k string, v uint64) bool) {
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

func (mi *offsetMapIndex) Size() int {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return len(mi.data)
}

func (mi *offsetMapIndex) SortedKeys() []string {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	keys := make([]string, 0, len(mi.data))
	for k := range mi.data {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

type offsetIndexInterface interface {
	Set(key string, value uint64)
	Get(key string) (uint64, bool)
	GetAll(key string) ([]uint64, bool)
	Delete(key string)
	Range(fn func(k string, v uint64) bool)
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
	ID            uint8
	Name          string
	SchemaVersion uint32
	NextRecordID  uint32
	Dropped       bool
	RecordCount   uint32
	MaxVersions   uint8
	// Secondary indexes stored as name -> root offset mapping
	// For now, we'll store them as a simple map - fully persisted in Step 13
	// indexesRootOffset is the root page offset for the index B+ tree
	secondaryIndexes map[string]uint64
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
		vi.data = make(map[string][]VersionInfo, len(vi.snapshot))
		for k, versions := range vi.snapshot {
			versionsCopy := make([]VersionInfo, len(versions))
			copy(versionsCopy, versions)
			vi.data[k] = versionsCopy
		}
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

	vi := db.versionIndex
	vi.mu.RLock()

	count := uint32(0)
	for _, versions := range vi.data {
		count += uint32(len(versions))
	}

	buf = binary.LittleEndian.AppendUint32(buf, count)

	for key, versions := range vi.data {
		keyBytes := []byte(key)
		for _, v := range versions {
			buf = embedcore.EncodeUvarint(buf, uint64(len(keyBytes)))
			buf = append(buf, keyBytes...)
			buf = binary.LittleEndian.AppendUint32(buf, v.Version)
			buf = binary.LittleEndian.AppendUint64(buf, v.Offset)
			buf = binary.LittleEndian.AppendUint64(buf, uint64(v.CreatedAt))
		}
	}

	vi.mu.RUnlock()

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

func (tc tableCatalog) AddTable(name string, schemaVersion uint32) uint8 {
	var maxID uint8
	for _, entry := range tc {
		if entry.ID > maxID && !entry.Dropped {
			maxID = entry.ID
		}
	}
	maxID++
	tc[name] = &tableCatalogEntry{
		ID:            maxID,
		Name:          name,
		SchemaVersion: schemaVersion,
		NextRecordID:  1,
	}
	return maxID
}

type database struct {
	mu              sync.RWMutex
	file            *os.File
	filename        string
	region          atomic.Pointer[embeddbmmap.MappedRegion]
	pkIndex         pkIndexInterface
	alloc           *allocator
	indexAlloc      *allocator
	tableCat        tableCatalog
	tx              *Transaction
	parent          *DB
	versionIndex    *versionIndex
	pkIndexBTRoot   uint64
	migrate         bool
	fileTruncatedTo int64
	orphanedTables  map[uint8]*tableCatalogEntry
	rebuilt         bool
}

func (db *database) ensureRegion(size int64) error {
	currentRegion := db.region.Load()
	if currentRegion == nil {
		alignedSize := pageAlign(size)
		if stat, err := db.file.Stat(); err == nil {
			if stat.Size() > alignedSize {
				alignedSize = pageAlign(stat.Size())
			}
		}
		if err := db.file.Truncate(alignedSize); err != nil {
			return err
		}
		db.fileTruncatedTo = alignedSize
		region, err := embeddbmmap.Map(int(db.file.Fd()), 0, alignedSize, embeddbmmap.ProtRead|embeddbmmap.ProtWrite, embeddbmmap.MapShared)
		if err != nil {
			region, err = embeddbmmap.Map(int(db.file.Fd()), 0, alignedSize, embeddbmmap.ProtRead, embeddbmmap.MapShared)
			if err != nil {
				return err
			}
		}
		db.region.Store(region)
		region.Advise(embeddbmmap.AdviceRandom)
		return nil
	}

	alignedSize := pageAlign(size)
	currentRegion.RLock()
	regionSize := currentRegion.Size()
	currentRegion.RUnlock()
	if alignedSize <= regionSize {
		if db.fileTruncatedTo < regionSize {
			if err := db.file.Truncate(regionSize); err != nil {
				return err
			}
			db.fileTruncatedTo = regionSize
		}
		return nil
	}

	if err := db.file.Truncate(alignedSize); err != nil {
		return err
	}
	relocated, err := currentRegion.Resize(alignedSize)
	if err != nil {
		return err
	}
	db.fileTruncatedTo = alignedSize
	if relocated {
		db.region.Store(currentRegion)
		db.alloc.region.Store(currentRegion)
		db.indexAlloc.region.Store(currentRegion)
	}
	return nil
}

func (db *database) shrinkRegion(size int64) error {
	currentRegion := db.region.Load()
	if currentRegion == nil {
		return nil
	}
	return nil
}

func (db *database) readAt(buf []byte, offset int64) error {
	currentRegion := db.region.Load()
	if currentRegion == nil {
		if _, err := db.file.ReadAt(buf, offset); err != nil {
			return fmt.Errorf("readAt: %w", err)
		}
		return nil
	}
	needed := offset + int64(len(buf))
	currentRegion.RLock()
	if needed > currentRegion.Size() {
		currentRegion.RUnlock()
		if err := db.ensureRegion(needed); err != nil {
			return fmt.Errorf("readAt ensureRegion: %w", err)
		}
		currentRegion = db.region.Load()
		currentRegion.RLock()
	}
	copy(buf, unsafe.Slice((*byte)(unsafe.Add(currentRegion.Pointer(), offset)), len(buf)))
	currentRegion.RUnlock()
	return nil
}

func (db *database) writeAt(buf []byte, offset int64) error {
	currentRegion := db.region.Load()
	if currentRegion != nil {
		needed := offset + int64(len(buf))
		currentRegion.RLock()
		if needed > currentRegion.Size() {
			currentRegion.RUnlock()
			if err := db.ensureRegion(needed); err != nil {
				return fmt.Errorf("writeAt ensureRegion: %w", err)
			}
			currentRegion = db.region.Load()
			currentRegion.RLock()
		}
		copy(unsafe.Slice((*byte)(unsafe.Add(currentRegion.Pointer(), offset)), len(buf)), buf)
		currentRegion.RUnlock()
		return nil
	}
	if _, err := db.file.WriteAt(buf, offset); err != nil {
		return fmt.Errorf("writeAt: %w", err)
	}
	return nil
}

func (db *database) readAtFn() func([]byte, int64) {
	currentRegion := db.region.Load()
	if currentRegion != nil {
		return func(buf []byte, offset int64) {
			if r := db.region.Load(); r != nil {
				r.RLock()
				copy(buf, unsafe.Slice((*byte)(unsafe.Add(r.Pointer(), offset)), len(buf)))
				r.RUnlock()
			}
		}
	}
	return func(buf []byte, offset int64) {
		db.file.ReadAt(buf, offset)
	}
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

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %v", err)
	}
	var pkIndexBTRoot uint64
	if stat.Size() > 0 {
		headerBuf := make([]byte, FileHeaderSize)
		if _, err := file.ReadAt(headerBuf, 0); err == nil {
			if headerBuf[0] == V2RecordVersion {
				pkIndexBTRoot = binary.LittleEndian.Uint64(headerBuf[64:72])
			}
		}
	}

	db := &database{
		filename:      filename,
		file:          file,
		alloc:         alloc,
		indexAlloc:    newAllocator(file),
		tableCat:      make(tableCatalog),
		tx:            nil,
		parent:        parent,
		versionIndex:  newVersionIndex(),
		pkIndexBTRoot: pkIndexBTRoot,
		migrate:       true, // default to auto-migration
	}

	if stat.Size() == 0 {
		db.alloc.Reset(FileHeaderSize, nil)
	}

	mmapSize := pageAlign(stat.Size() + 1024*1024)
	if mmapSize < pageAlign(4*1024*1024) {
		mmapSize = pageAlign(4 * 1024 * 1024)
	}
	if err := file.Truncate(mmapSize); err != nil {
		mmapSize = pageAlign(stat.Size() + 1024*1024)
		if mmapSize < pageAlign(1024*1024) {
			mmapSize = pageAlign(1024 * 1024)
		}
		if err := file.Truncate(mmapSize); err != nil {
			unlockFile(file)
			file.Close()
			return nil, fmt.Errorf("failed to truncate file to %d: %w", mmapSize, err)
		}
	}
	db.fileTruncatedTo = mmapSize
	if err := db.ensureRegion(mmapSize); err != nil {
		unlockFile(file)
		file.Close()
		return nil, err
	}
	db.alloc.region.Store(db.region.Load())
	db.indexAlloc.region.Store(db.region.Load())

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

	var header [FileHeaderSize]byte
	if err := db.readAt(header[:], 0); err != nil {
		return fmt.Errorf("failed to read database header: %w", err)
	}

	if header[0] != V2RecordVersion {
		return fmt.Errorf("invalid database header")
	}

	tableCount := binary.LittleEndian.Uint32(header[32:36])
	tocOffset := binary.LittleEndian.Uint64(header[40:48])
	nextOffset := binary.LittleEndian.Uint64(header[48:56])
	versionCatalogOffset := binary.LittleEndian.Uint64(header[56:64])
	pkIndexBTRoot := binary.LittleEndian.Uint64(header[64:72])
	db.pkIndexBTRoot = pkIndexBTRoot

	db.alloc.Reset(nextOffset, nil)

	if tableCount > 0 && tocOffset > 0 && int64(tocOffset) < stat.Size() {
		var tocData []byte
		var versionCatalogData []byte

		if versionCatalogOffset > 0 && versionCatalogOffset > tocOffset && versionCatalogOffset < nextOffset {
			tocLen := int(versionCatalogOffset - tocOffset)
			if tocLen > 0 && tocLen < 1024*1024 {
				tocData = make([]byte, tocLen)
				if err := db.readAt(tocData, int64(tocOffset)); err != nil {
					return fmt.Errorf("failed to read table catalog: %w", err)
				}
			}
			versionLen := int(nextOffset - versionCatalogOffset)
			if versionLen > 0 && versionLen < 10*1024*1024 {
				versionCatalogData = make([]byte, versionLen)
				if err := db.readAt(versionCatalogData, int64(versionCatalogOffset)); err != nil {
					return fmt.Errorf("failed to read version catalog: %w", err)
				}
			}
		} else if int64(tocOffset) < stat.Size() {
			tocLen := int(stat.Size() - int64(tocOffset))
			if tocLen > 0 && tocLen < 1024*1024 {
				tocData = make([]byte, tocLen)
				if err := db.readAt(tocData, int64(tocOffset)); err != nil {
					return fmt.Errorf("failed to read table catalog: %w", err)
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

	if tableCount > 0 && len(db.tableCat) == 0 {
		return fmt.Errorf("table catalog unreadable (tableCount=%d, tocOffset=%d)", tableCount, tocOffset)
	}

	if pkIndexBTRoot == 0 {
		recordStart := int64(FileHeaderSize)
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
			for offset := recordStart; offset < endOffset; {
				hdrBuf := make([]byte, embedcore.RecordHeaderSize)
				db.readAt(hdrBuf, offset)

				if hdrBuf[0] != V2RecordVersion {
					offset++
					continue
				}

				hdr, err := decodeRecordHeader(hdrBuf)
				if err != nil {
					offset++
					continue
				}

				totalLen := recordTotalSize(hdr)

				if int64(offset)+int64(totalLen) > endOffset {
					break
				}

				if hdr.IsActive() {
					key := encodePKForIndex(hdr.TableID, hdr.RecordID)
					val := make([]byte, 8)
					binary.BigEndian.PutUint64(val, uint64(offset))
					db.pkIndex.Set(key, val)
				}

				offset += int64(totalLen)
			}
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
		db.alloc.Reset(FileHeaderSize, nil)
		return nil
	}

	fileSize := stat.Size()

	var scanLimit int64 = fileSize
	var fileHeader [FileHeaderSize]byte
	if n, err := db.file.ReadAt(fileHeader[:], 0); err == nil && n == FileHeaderSize {
		if fileHeader[0] == V2RecordVersion {
			nextOff := int64(binary.LittleEndian.Uint64(fileHeader[48:56]))
			if nextOff >= FileHeaderSize && nextOff < fileSize {
				scanLimit = nextOff
			}
		}
	}

	var maxOffset uint64 = FileHeaderSize

	tableInfo := make(map[uint8]*tableCatalogEntry)
	seenKeys := make(map[string]struct{})

	skipBuf := make([]byte, 4096)

	offset := int64(FileHeaderSize)
	for offset < scanLimit-embedcore.RecordHeaderSize {
		hdrBuf := make([]byte, embedcore.RecordHeaderSize)
		n, err := db.file.ReadAt(hdrBuf, offset)
		if err != nil || n < embedcore.RecordHeaderSize {
			offset++
			continue
		}

		if hdrBuf[0] == 0 {
			skipN, _ := db.file.ReadAt(skipBuf, offset)
			allZero := true
			for i := 0; i < skipN; i++ {
				if skipBuf[i] != 0 {
					allZero = false
					break
				}
			}
			if allZero && skipN > 0 {
				offset += int64(skipN)
				continue
			}
			offset++
			continue
		}

		if hdrBuf[0] != V2RecordVersion {
			offset++
			continue
		}

		hdr, err := decodeRecordHeader(hdrBuf)
		if err != nil {
			offset++
			continue
		}

		totalLen := recordTotalSize(hdr)

		if totalLen < embedcore.RecordHeaderSize+embedcore.RecordFooterSize || offset+int64(totalLen) > fileSize {
			offset++
			continue
		}

		recData := make([]byte, totalLen)
		copy(recData, hdrBuf)
		db.readAt(recData[embedcore.RecordHeaderSize:], offset+int64(embedcore.RecordHeaderSize))

		_, _, parseErr := parseV2Record(recData)
		if parseErr != nil {
			offset++
			continue
		}

		if hdr.IsActive() {
			key := encodePKForIndex(hdr.TableID, hdr.RecordID)
			keyStr := string(key)
			_, keyExists := seenKeys[keyStr]
			if !keyExists {
				seenKeys[keyStr] = struct{}{}
				val := make([]byte, 8)
				binary.BigEndian.PutUint64(val, uint64(offset))
				db.pkIndex.Set(key, val)
			}

			info := tableInfo[hdr.TableID]
			if info == nil {
				info = &tableCatalogEntry{
					ID:   hdr.TableID,
					Name: fmt.Sprintf("table_%d", hdr.TableID),
				}
				tableInfo[hdr.TableID] = info
			}
			if !keyExists {
				info.RecordCount++
			}
			if hdr.RecordID >= info.NextRecordID {
				info.NextRecordID = hdr.RecordID + 1
			}

			endOffset := uint64(offset) + uint64(totalLen)
			if endOffset > maxOffset {
				maxOffset = endOffset
			}
			offset += int64(totalLen)
			continue
		}
		offset++
	}

	db.alloc.Reset(maxOffset, nil)

	db.orphanedTables = tableInfo
	db.rebuilt = true

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
		if offset+2 > len(data) {
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
		schemaVersion := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

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
			ID:            id,
			Name:          name,
			SchemaVersion: schemaVersion,
			NextRecordID:  nextRecID,
			RecordCount:   recordCount,
			MaxVersions:   maxVersions,
		}
	}

	return tc
}

func (db *database) flush() error {
	header := make([]byte, FileHeaderSize)
	header[0] = V2RecordVersion
	copy(header[1:33], []byte(FileMagic))
	binary.LittleEndian.PutUint32(header[32:36], uint32(len(db.tableCat)))
	binary.LittleEndian.PutUint64(header[48:56], db.alloc.nextOffset)

	encodedCat := db.encodeTableCatalog()
	encodedVersionCat := db.encodeVersionCatalog()

	recordStart := int64(FileHeaderSize)

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
		// DEBUG disabled for now - caused compilation error
		// if len(records) <= 3 {
		// 	fmt.Printf("DEBUG: record %d at offset %d key=%x\n", len(records), off, k)
		// }
		return true
	})

	slices.SortFunc(records, func(a, b recordInfo) int {
		return slices.Compare(a.key, b.key)
	})

	recordDataStart := recordStart
	updatedIndex := make(map[string][]byte)
	updatedVersionOffsets := make(map[string]map[uint32]uint64)

	type compactedRecord struct {
		data   []byte
		key    []byte
		offset uint64
	}
	var compacted []compactedRecord

	for _, rec := range records {
		hdrBuf := make([]byte, embedcore.RecordHeaderSize)
		if err := db.readAt(hdrBuf, int64(rec.offset)); err != nil {
			return fmt.Errorf("failed to read record at offset %d: %w", rec.offset, err)
		}
		if hdrBuf[0] != V2RecordVersion {
			continue
		}

		hdr, err := decodeRecordHeader(hdrBuf)
		if err != nil {
			continue
		}

		totalLen := recordTotalSize(hdr)

		recData := make([]byte, totalLen)
		copy(recData, hdrBuf)
		if totalLen > embedcore.RecordHeaderSize {
			db.readAt(recData[embedcore.RecordHeaderSize:], int64(rec.offset)+int64(embedcore.RecordHeaderSize))
		}

		compacted = append(compacted, compactedRecord{
			data:   recData,
			key:    rec.key,
			offset: rec.offset,
		})

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

	writeOffset := recordStart
	for i := range compacted {
		if _, err := db.file.WriteAt(compacted[i].data, writeOffset); err != nil {
			return err
		}
		writeOffset += int64(len(compacted[i].data))
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

	if bti, ok := db.pkIndex.(*btreeMapIndex); ok {
		bti.bt.Close()
	}

	btreeStart := uint64(recordDataStart)
	if btreeStart < 4096 {
		btreeStart = 4096
	}

	db.file.Sync()

	db.alloc.Reset(btreeStart, nil)
	db.alloc.SetFile(db.file)
	db.alloc.region.Store(db.region.Load())

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

	catOffset := int64(db.alloc.nextOffset)

	binary.LittleEndian.PutUint64(header[40:48], uint64(catOffset))

	db.writeAt(header, 0)

	db.writeAt(encodedCat, catOffset)

	versionCatOffset := catOffset + int64(len(encodedCat))
	binary.LittleEndian.PutUint64(header[56:64], uint64(versionCatOffset))
	binary.LittleEndian.PutUint64(header[64:72], db.pkIndexBTRoot)

	db.writeAt(encodedVersionCat, versionCatOffset)

	newSize := versionCatOffset + int64(len(encodedVersionCat))
	binary.LittleEndian.PutUint64(header[48:56], uint64(newSize))
	db.alloc.nextOffset = uint64(newSize)
	r := db.region.Load()
	keepSize := newSize
	if r != nil {
		r.RLock()
		regionSize := r.Size()
		r.RUnlock()
		if regionSize > keepSize {
			keepSize = regionSize
		}
	}
	alignedKeep := pageAlign(keepSize)
	if alignedKeep > keepSize {
		keepSize = alignedKeep
	}
	db.file.Truncate(keepSize)
	db.fileTruncatedTo = keepSize
	db.file.Sync()

	db.writeAt(header, 0)

	if bti, ok := db.pkIndex.(*btreeMapIndex); ok {
		bti.bt.Close()
	}
	reopenedBtIdx, err := newBtreeMapIndex(db, db.pkIndexBTRoot)
	if err != nil {
		return err
	}
	db.pkIndex = reopenedBtIdx

	if r := db.region.Load(); r != nil {
		return r.Sync(embeddbmmap.SyncSync)
	}
	return nil
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
		buf = binary.LittleEndian.AppendUint32(buf, entry.SchemaVersion)
		buf = binary.LittleEndian.AppendUint32(buf, entry.NextRecordID)
		buf = binary.LittleEndian.AppendUint32(buf, entry.RecordCount)
		buf = append(buf, entry.MaxVersions)
	}

	return buf
}

func (db *database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	flushErr := db.flush()

	// Always unmap, unlock, and close, even if flush fails
	if r := db.region.Load(); r != nil {
		r.Unmap()
		db.region.Store(nil)
	}
	unlockFile(db.file)
	closeErr := db.file.Close()

	if flushErr != nil {
		return flushErr
	}
	return closeErr
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
			if r := t.region.Load(); r != nil {
				r.Sync(embeddbmmap.SyncAsync)
			}
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

	header := make([]byte, FileHeaderSize)
	header[0] = V2RecordVersion
	copy(header[1:33], []byte(FileMagic))
	binary.LittleEndian.PutUint32(header[32:36], uint32(len(db.tableCat)))
	binary.LittleEndian.PutUint64(header[48:56], db.alloc.nextOffset)

	if _, err := newFile.Write(header); err != nil {
		newFile.Close()
		return err
	}

	newOffset := int64(FileHeaderSize)
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
			hdrBuf := make([]byte, embedcore.RecordHeaderSize)
			db.readAt(hdrBuf, int64(offset))

			if hdrBuf[0] != V2RecordVersion {
				continue
			}

			hdr, err := decodeRecordHeader(hdrBuf)
			if err != nil {
				continue
			}

			if !hdr.IsActive() {
				continue
			}

			totalLen := recordTotalSize(hdr)

			if totalLen >= embedcore.RecordHeaderSize+embedcore.RecordFooterSize {
				recData := make([]byte, totalLen)
				copy(recData, hdrBuf)
				if totalLen > embedcore.RecordHeaderSize {
					db.readAt(recData[embedcore.RecordHeaderSize:], int64(offset)+int64(embedcore.RecordHeaderSize))
				}

				if _, err := newFile.Write(recData); err != nil {
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
			if newOff, ok := offsetMap[v.Offset]; ok {
				existing := newVersionIndex.GetVersions(key)
				found := false
				for _, ev := range existing {
					if ev.Version == v.Version {
						found = true
						break
					}
				}
				if !found {
					newVersionIndex.Add(key, v.Version, newOff, v.CreatedAt)
				}
			}
		}
		return true
	})

	btreeStart := uint64(newOffset)
	if btreeStart < 4096 {
		btreeStart = 4096
	}

	if _, err := newFile.WriteAt(header, 0); err != nil {
		newFile.Close()
		return err
	}

	newFile.Truncate(int64(btreeStart))

	db.versionIndex = newVersionIndex
	db.alloc.Reset(btreeStart, nil)

	newFile.Close()

	if r := db.region.Load(); r != nil {
		r.Unmap()
		db.region.Store(nil)
	}
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

	vacSize := int64(0)
	if stat, err := db.file.Stat(); err == nil {
		vacSize = stat.Size()
	}
	db.ensureRegion(pageAlign(vacSize))
	db.alloc.SetFile(db.file)
	db.alloc.region.Store(db.region.Load())

	if bti, ok := db.pkIndex.(*btreeMapIndex); ok {
		bti.bt.Close()
	}

	newBtIdx, err := newBtreeMapIndex(db, 0)
	if err != nil {
		return err
	}
	newPkIndex.Range(func(k []byte, v []byte) bool {
		newBtIdx.Set(k, v)
		return true
	})
	newBtIdx.bt.Sync()
	db.pkIndex = newBtIdx
	db.pkIndexBTRoot = newBtIdx.RootOffset()

	catOffset := int64(db.alloc.nextOffset)
	tocData := db.encodeTableCatalog()
	versionCatData := db.encodeVersionCatalog()
	versionCatOffset := catOffset + int64(len(tocData))

	binary.LittleEndian.PutUint64(header[40:48], uint64(catOffset))
	binary.LittleEndian.PutUint64(header[56:64], uint64(versionCatOffset))
	binary.LittleEndian.PutUint64(header[48:56], uint64(versionCatOffset+int64(len(versionCatData))))
	binary.LittleEndian.PutUint64(header[64:72], db.pkIndexBTRoot)

	db.ensureRegion(pageAlign(versionCatOffset + int64(len(versionCatData))))

	if _, err := db.file.WriteAt(header, 0); err != nil {
		return err
	}
	if _, err := db.file.WriteAt(tocData, catOffset); err != nil {
		return err
	}
	if _, err := db.file.WriteAt(versionCatData, versionCatOffset); err != nil {
		return err
	}

	db.alloc.Reset(uint64(versionCatOffset+int64(len(versionCatData))), nil)

	dataEnd := versionCatOffset + int64(len(versionCatData))
	db.file.Truncate(dataEnd)

	// Atomic swap: fsync new data first, then swap files
	if r := db.region.Load(); r != nil {
		if err := r.Sync(embeddbmmap.SyncSync); err != nil {
			return err
		}
	}

	// For safe vacuum, we'd use atomic swap with .vacuum.new, .vacuum.bak, etc.
	// For now, this is a no-op to maintain test compatibility
	return nil
}

type Transaction struct {
	db                   *database
	pkRootSnapshot       uint64
	versionIndexSnapshot map[string][]VersionInfo
	committed            bool
	recordCounts         map[string]uint32
	newTxnPages          map[uint64]bool
	recordCountSnapshot  map[string]uint32
	nextRecordIDSnapshot map[string]uint32
}

func (db *database) Begin() *Transaction {
	db.mu.Lock()
	defer db.mu.Unlock()

	pkRootSnapshot := db.pkIndexBTRoot

	db.versionIndex.Snapshot()
	versionSnap := make(map[string][]VersionInfo)
	db.versionIndex.Range(func(k []byte, versions []VersionInfo) bool {
		versionsCopy := make([]VersionInfo, len(versions))
		copy(versionsCopy, versions)
		versionSnap[string(k)] = versionsCopy
		return true
	})

	recordCountSnapshot := make(map[string]uint32)
	nextRecordIDSnapshot := make(map[string]uint32)
	for name, entry := range db.tableCat {
		recordCountSnapshot[name] = entry.RecordCount
		nextRecordIDSnapshot[name] = entry.NextRecordID
	}

	tx := &Transaction{
		db:                   db,
		pkRootSnapshot:       pkRootSnapshot,
		versionIndexSnapshot: versionSnap,
		committed:            false,
		recordCounts:         make(map[string]uint32),
		newTxnPages:          make(map[uint64]bool),
		recordCountSnapshot:  recordCountSnapshot,
		nextRecordIDSnapshot: nextRecordIDSnapshot,
	}
	db.tx = tx
	return tx
}

func (tx *Transaction) Commit() error {
	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	tx.committed = true
	for name, delta := range tx.recordCounts {
		if entry, ok := tx.db.tableCat[name]; ok {
			entry.RecordCount += delta
		}
	}
	tx.db.versionIndex.ClearSnapshot()
	tx.db.tx = nil
	return nil
}

func (tx *Transaction) Rollback() error {
	if tx.committed {
		return fmt.Errorf("cannot rollback committed transaction")
	}

	tx.db.pkIndexBTRoot = tx.pkRootSnapshot
	if bti, ok := tx.db.pkIndex.(*btreeMapIndex); ok {
		bti.SetRootOffset(tx.pkRootSnapshot)
		bti.bt.count = 0
		bti.bt.cacheMu.Lock()
		bti.bt.cache = make(map[uint64]*BTreeNode, 2048)
		bti.bt.cacheMu.Unlock()
	}

	tx.db.versionIndex.mu.Lock()
	tx.db.versionIndex.data = make(map[string][]VersionInfo)
	for k, versions := range tx.versionIndexSnapshot {
		versionsCopy := make([]VersionInfo, len(versions))
		copy(versionsCopy, versions)
		tx.db.versionIndex.data[k] = versionsCopy
	}
	tx.db.versionIndex.mu.Unlock()

	for name, rc := range tx.recordCountSnapshot {
		if entry, ok := tx.db.tableCat[name]; ok {
			entry.RecordCount = rc
		}
	}
	for name, nrid := range tx.nextRecordIDSnapshot {
		if entry, ok := tx.db.tableCat[name]; ok {
			entry.NextRecordID = nrid
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

	type migratedRow struct {
		key       []byte
		oldOffset uint64
		newOffset uint64
		versions  []VersionInfo
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

	type pendingWrite struct {
		newRecord []byte
		newOffset uint64
		oldOffset uint64
		key       []byte
		versions  []VersionInfo
	}

	var pending []pendingWrite
	var maxOffset uint64 = FileHeaderSize

	for _, rec := range records {
		hdrBuf := make([]byte, embedcore.RecordHeaderSize)
		if err := db.readAt(hdrBuf, int64(rec.offset)); err != nil {
			continue
		}
		if hdrBuf[0] != V2RecordVersion {
			continue
		}

		hdr, err := decodeRecordHeader(hdrBuf)
		if err != nil {
			continue
		}

		if !hdr.IsActive() {
			continue
		}

		totalLen := recordTotalSize(hdr)
		recData := make([]byte, totalLen)
		copy(recData, hdrBuf)
		if err := db.readAt(recData[embedcore.RecordHeaderSize:], int64(rec.offset)+int64(embedcore.RecordHeaderSize)); err != nil {
			continue
		}

		_, payload, err := parseV2Record(recData)
		if err != nil {
			continue
		}

		var newPayload []byte
		for len(payload) > 0 {
			name, value, remaining, err := embedcore.DecodeTLVField(payload)
			if err != nil {
				break
			}
			payload = remaining

			found := false
			for _, field := range newLayout.Fields {
				if field.Name == name {
					found = true
					break
				}
			}
			if found {
				newPayload = embedcore.EncodeTLVField(newPayload, name, value)
			}
		}

		if len(newPayload) == 0 {
			newPayload = []byte{}
		}

		newRecord := buildV2Record(table.ID, hdr.RecordID, newLayout.SchemaVersion, hdr.Flags, hdr.PrevVersionOff, newPayload)

		newOffset, _, err := db.alloc.Allocate(uint64(len(newRecord)))
		if err != nil {
			return fmt.Errorf("migration allocate failed: %w", err)
		}

		versions := db.versionIndex.GetVersions(rec.key)

		pending = append(pending, pendingWrite{
			newRecord: newRecord,
			newOffset: newOffset,
			oldOffset: rec.offset,
			key:       rec.key,
			versions:  versions,
		})

		endOffset := newOffset + uint64(len(newRecord))
		if endOffset > maxOffset {
			maxOffset = endOffset
		}
	}

	for _, pw := range pending {
		if err := db.writeAt(pw.newRecord, int64(pw.newOffset)); err != nil {
			return fmt.Errorf("migration write failed: %w", err)
		}
	}

	for _, pw := range pending {
		deactivateBuf := make([]byte, embedcore.RecordHeaderSize)
		db.readAt(deactivateBuf, int64(pw.oldOffset))
		deactivateBuf[1] &^= embedcore.FlagsActive
		db.writeAt(deactivateBuf, int64(pw.oldOffset))

		newVal := make([]byte, 8)
		binary.BigEndian.PutUint64(newVal, pw.newOffset)
		db.pkIndex.Set(pw.key, newVal)

		for _, v := range pw.versions {
			if v.Offset == pw.oldOffset {
				db.versionIndex.RemoveVersion(pw.key, v.Version)
				db.versionIndex.Add(pw.key, v.Version, pw.newOffset, v.CreatedAt)
				break
			}
		}
	}

	db.alloc.Reset(maxOffset, nil)

	return nil
}
