package embeddb

import (
	"bytes"
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
}

type tableCatalog map[string]*tableCatalogEntry

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
	index           *BTree
	indexRoot       uint64
	alloc           *allocator
	tableCat        tableCatalog
	tx              *Transaction
	parent          *DB
	migrate         bool
	fileTruncatedTo int64
	droppedIndexes  map[string]map[string]bool
	explicitIndexes map[string][]string
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

	}
	return nil
}

func (db *database) readAt(buf []byte, offset int64) error {
	for {
		currentRegion := db.region.Load()
		if currentRegion == nil {
			if _, err := db.file.ReadAt(buf, offset); err != nil {
				return fmt.Errorf("readAt: %w", err)
			}
			return nil
		}
		needed := offset + int64(len(buf))
		currentRegion.RLock()
		if needed <= currentRegion.Size() {
			copy(buf, unsafe.Slice((*byte)(unsafe.Add(currentRegion.Pointer(), offset)), len(buf)))
			currentRegion.RUnlock()
			return nil
		}
		currentRegion.RUnlock()
		if err := db.ensureRegion(needed); err != nil {
			return fmt.Errorf("readAt ensureRegion: %w", err)
		}
	}
}

func (db *database) writeAt(buf []byte, offset int64) error {
	for {
		currentRegion := db.region.Load()
		if currentRegion == nil {
			if _, err := db.file.WriteAt(buf, offset); err != nil {
				return fmt.Errorf("writeAt: %w", err)
			}
			return nil
		}
		needed := offset + int64(len(buf))
		currentRegion.RLock()
		if needed <= currentRegion.Size() {
			copy(unsafe.Slice((*byte)(unsafe.Add(currentRegion.Pointer(), offset)), len(buf)), buf)
			currentRegion.RUnlock()
			return nil
		}
		currentRegion.RUnlock()
		if err := db.ensureRegion(needed); err != nil {
			return fmt.Errorf("writeAt ensureRegion: %w", err)
		}
	}
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

	var indexRoot uint64
	if stat.Size() > 0 {
		headerBuf := make([]byte, FileHeaderSize)
		if _, err := file.ReadAt(headerBuf, 0); err == nil {
			if headerBuf[0] == V2RecordVersion {
				indexRoot = binary.LittleEndian.Uint64(headerBuf[56:64])
			}
		}
	}

	db := &database{
		filename:        filename,
		file:            file,
		alloc:           alloc,
		indexRoot:       indexRoot,
		tableCat:        make(tableCatalog),
		tx:              nil,
		parent:          parent,
		migrate:         true,
		droppedIndexes:  make(map[string]map[string]bool),
		explicitIndexes: make(map[string][]string),
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

	db.index, err = db.openBTree(db.indexRoot)
	if err != nil {
		unlockFile(file)
		file.Close()
		return nil, err
	}
	db.indexRoot = db.index.RootOffset()

	if err := db.load(); err != nil {
		if rebuildErr := db.rebuildIndexFromScan(); rebuildErr != nil {
			unlockFile(file)
			file.Close()
			return nil, fmt.Errorf("failed to load index: %w, failed to rebuild: %v", err, rebuildErr)
		}
	}

	db.indexRoot = db.index.RootOffset()

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
	db.indexRoot = binary.LittleEndian.Uint64(header[56:64])

	db.alloc.Reset(nextOffset, nil)

	if tableCount > 0 && tocOffset > 0 && int64(tocOffset) < stat.Size() {
		tocEnd := int64(nextOffset)
		if tocEnd <= 0 || tocEnd > stat.Size() {
			tocEnd = stat.Size()
		}
		tocLen := int(tocEnd - int64(tocOffset))
		if tocLen > 0 && tocLen < 1024*1024 {
			tocData := make([]byte, tocLen)
			if err := db.readAt(tocData, int64(tocOffset)); err != nil {
				return fmt.Errorf("failed to read table catalog: %w", err)
			}
			db.tableCat = decodeTableCatalog(tocData)
		}
	}

	if tableCount > 0 && len(db.tableCat) == 0 {
		return fmt.Errorf("table catalog unreadable (tableCount=%d, tocOffset=%d)", tableCount, tocOffset)
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
	seenKeys := make(map[[5]byte]struct{})

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
			dedupKey := [5]byte{hdr.TableID}
			binary.LittleEndian.PutUint32(dedupKey[1:], hdr.RecordID)
			if _, seen := seenKeys[dedupKey]; seen {
				offset++
				continue
			}
			seenKeys[dedupKey] = struct{}{}

			key := encodePrimaryKey(hdr.TableID, hdr.RecordID)
			db.index.Insert(key, uint64(offset))

			if hdr.HasPrevVersion() && hdr.PrevVersionOff >= FileHeaderSize {
				verKey := encodeVersionKey(hdr.TableID, hdr.RecordID, 1)
				db.index.Insert(verKey, uint64(offset))
			}

			payloadEnd := totalLen - embedcore.RecordFooterSize
			if payloadEnd > embedcore.RecordHeaderSize {
				payload := recData[embedcore.RecordHeaderSize:payloadEnd]
				for len(payload) > 0 {
					fieldName, fieldValue, remaining, err := embedcore.DecodeTLVField(payload)
					if err != nil {
						break
					}
					payload = remaining

					if fieldName == "" {
						continue
					}

					secKey := encodeSecondaryKey(hdr.TableID, fieldName, fieldValue, uint64(offset))
					db.index.Insert(secKey, uint64(offset))
				}
			}

			info := tableInfo[hdr.TableID]
			if info == nil {
				info = &tableCatalogEntry{
					ID:   hdr.TableID,
					Name: fmt.Sprintf("table_%d", hdr.TableID),
				}
				tableInfo[hdr.TableID] = info
			}
			info.RecordCount++
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

	for _, info := range tableInfo {
		db.tableCat[info.Name] = info
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

	recordStart := int64(FileHeaderSize)

	type recordInfo struct {
		offset uint64
		key    []byte
	}

	var records []recordInfo
	db.index.Scan(func(key []byte, value uint64) bool {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		records = append(records, recordInfo{
			offset: value,
			key:    keyCopy,
		})
		return true
	})

	slices.SortFunc(records, func(a, b recordInfo) int {
		return slices.Compare(a.key, b.key)
	})

	recordDataStart := recordStart
	type compactedRecord struct {
		data []byte
		key  []byte
	}
	var compacted []compactedRecord
	updatedOffsets := make(map[string]uint64)

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
			data: recData,
			key:  rec.key,
		})
		updatedOffsets[string(rec.key)] = uint64(recordDataStart)

		recordDataStart += int64(totalLen)
	}

	writeOffset := recordStart
	for i := range compacted {
		if _, err := db.file.WriteAt(compacted[i].data, writeOffset); err != nil {
			return err
		}
		writeOffset += int64(len(compacted[i].data))
	}

	db.file.Sync()

	db.index.Close()

	btreeStart := uint64(recordDataStart)
	if btreeStart < 4096 {
		btreeStart = 4096
	}

	db.file.Sync()

	db.alloc.Reset(btreeStart, nil)
	db.alloc.SetFile(db.file)
	db.alloc.region.Store(db.region.Load())

	newIndex, err := db.openBTree(0)
	if err != nil {
		return err
	}
	for keyStr, newOff := range updatedOffsets {
		if err := newIndex.Insert([]byte(keyStr), newOff); err != nil {
			return err
		}
	}
	newIndex.Sync()

	btreeRoot := newIndex.RootOffset()

	catOffset := int64(db.alloc.nextOffset)

	binary.LittleEndian.PutUint64(header[40:48], uint64(catOffset))

	db.writeAt(header, 0)

	db.writeAt(encodedCat, catOffset)

	binary.LittleEndian.PutUint64(header[56:64], btreeRoot)

	newSize := catOffset + int64(len(encodedCat))
	binary.LittleEndian.PutUint64(header[48:56], uint64(newSize))
	db.alloc.nextOffset = uint64(newSize)
	db.writeAt(header, 0)
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

	db.index = newIndex
	db.indexRoot = btreeRoot

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

	entries := make([]*tableCatalogEntry, 0)
	for _, entry := range db.tableCat {
		if !entry.Dropped {
			entries = append(entries, entry)
		}
	}

	offsetMap := make(map[uint64]uint64)

	for _, entry := range entries {
		pkPrefix := []byte{indexNSPrimary, entry.ID}
		db.index.Scan(func(key []byte, value uint64) bool {
			if !bytes.HasPrefix(key, pkPrefix) {
				return true
			}

			hdrBuf := make([]byte, embedcore.RecordHeaderSize)
			db.readAt(hdrBuf, int64(value))

			if hdrBuf[0] != V2RecordVersion {
				return true
			}

			hdr, err := decodeRecordHeader(hdrBuf)
			if err != nil {
				return true
			}

			if !hdr.IsActive() {
				return true
			}

			totalLen := recordTotalSize(hdr)

			if totalLen >= embedcore.RecordHeaderSize+embedcore.RecordFooterSize {
				recData := make([]byte, totalLen)
				copy(recData, hdrBuf)
				if totalLen > embedcore.RecordHeaderSize {
					db.readAt(recData[embedcore.RecordHeaderSize:], int64(value)+int64(embedcore.RecordHeaderSize))
				}

				if _, err := newFile.Write(recData); err != nil {
					newFile.Close()
					return false
				}

				offsetMap[value] = uint64(newOffset)
				newOffset += int64(totalLen)
			}

			return true
		})
	}

	btreeStart := uint64(newOffset)
	if btreeStart < 4096 {
		btreeStart = 4096
	}

	if _, err := newFile.WriteAt(header, 0); err != nil {
		newFile.Close()
		return err
	}

	newFile.Truncate(int64(btreeStart))

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

	db.index.Close()
	newIndex, err := db.openBTree(0)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		pkPrefix := []byte{indexNSPrimary, entry.ID}
		db.index.Scan(func(key []byte, value uint64) bool {
			if !bytes.HasPrefix(key, pkPrefix) {
				return true
			}
			newOff, ok := offsetMap[value]
			if ok {
				newIndex.Insert(key, newOff)
			}
			return true
		})

		verPrefix := []byte{indexNSVersion, entry.ID}
		db.index.Scan(func(key []byte, value uint64) bool {
			if !bytes.HasPrefix(key, verPrefix) {
				return true
			}
			newOff, ok := offsetMap[value]
			if ok {
				newIndex.Insert(key, newOff)
			}
			return true
		})

		secPrefix := []byte{indexNSSecondary, entry.ID}
		db.index.Scan(func(key []byte, value uint64) bool {
			if !bytes.HasPrefix(key, secPrefix) {
				return true
			}
			newValue, ok := offsetMap[value]
			if !ok {
				return true
			}
			_, _, _, oldRecordOffset, parsed := parseSecondaryKey(key)
			if !parsed {
				return true
			}
			newRecordOffset, ok := offsetMap[oldRecordOffset]
			if !ok {
				return true
			}
			newKey := make([]byte, len(key))
			copy(newKey, key)
			binary.BigEndian.PutUint64(newKey[len(newKey)-8:], newRecordOffset)
			newIndex.Insert(newKey, newValue)
			return true
		})
	}

	newIndex.Sync()
	btreeRoot := newIndex.RootOffset()

	catOffset := int64(db.alloc.nextOffset)
	tocData := db.encodeTableCatalog()

	binary.LittleEndian.PutUint64(header[40:48], uint64(catOffset))
	binary.LittleEndian.PutUint64(header[56:64], btreeRoot)

	db.ensureRegion(pageAlign(catOffset + int64(len(tocData))))

	if _, err := db.file.WriteAt(header, 0); err != nil {
		return err
	}
	if _, err := db.file.WriteAt(tocData, catOffset); err != nil {
		return err
	}

	newSize := catOffset + int64(len(tocData))
	binary.LittleEndian.PutUint64(header[48:56], uint64(newSize))
	db.alloc.Reset(uint64(newSize), nil)

	db.file.Truncate(newSize)

	if r := db.region.Load(); r != nil {
		if err := r.Sync(embeddbmmap.SyncSync); err != nil {
			return err
		}
	}

	db.index = newIndex
	db.indexRoot = btreeRoot

	return nil
}

type Transaction struct {
	db                   *database
	indexRootSnapshot    uint64
	committed            bool
	recordCounts         map[string]uint32
	newTxnPages          map[uint64]bool
	recordCountSnapshot  map[string]uint32
	nextRecordIDSnapshot map[string]uint32
}

func (db *database) Begin() *Transaction {
	db.mu.Lock()
	defer db.mu.Unlock()

	indexRootSnapshot := db.index.RootOffset()

	recordCountSnapshot := make(map[string]uint32)
	nextRecordIDSnapshot := make(map[string]uint32)
	for name, entry := range db.tableCat {
		recordCountSnapshot[name] = entry.RecordCount
		nextRecordIDSnapshot[name] = entry.NextRecordID
	}

	tx := &Transaction{
		db:                   db,
		indexRootSnapshot:    indexRootSnapshot,
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
	tx.db.tx = nil
	return nil
}

func (tx *Transaction) Rollback() error {
	if tx.committed {
		return fmt.Errorf("cannot rollback committed transaction")
	}

	tx.db.index.SetRootOffset(tx.indexRootSnapshot)
	tx.db.index.count = 0
	tx.db.index.cacheMu.Lock()
	tx.db.index.cache = make(map[uint64]*BTreeNode, 2048)
	tx.db.index.cacheMu.Unlock()

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

	var records []migrateRecord

	pkPrefix := []byte{indexNSPrimary, table.ID}
	db.index.Scan(func(key []byte, value uint64) bool {
		if bytes.HasPrefix(key, pkPrefix) {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			records = append(records, migrateRecord{
				key:    keyCopy,
				offset: value,
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

		pending = append(pending, pendingWrite{
			newRecord: newRecord,
			newOffset: newOffset,
			oldOffset: rec.offset,
			key:       rec.key,
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

		db.index.Insert(pw.key, pw.newOffset)
	}

	db.alloc.Reset(maxOffset, nil)

	return nil
}
