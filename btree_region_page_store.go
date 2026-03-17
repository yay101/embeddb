package embeddb

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
)

const (
	regionCatalogFooterMagic = "S2RGN2!!"
	regionCatalogMagic       = "RGCATV1!"
	regionCatalogVersion     = 1

	regionCatalogFooterSize = 32
	regionCatalogHeaderSize = 24
	regionCatalogEntrySize  = 40
	regionCatalogMaxEntries = 256

	regionCatalogMetaSize      = regionCatalogHeaderSize + (regionCatalogEntrySize * regionCatalogMaxEntries)
	regionCatalogInitialGrowth = 512 * 1024
	regionIndexGrowPages       = 64

	regionHeaderStartOff    = 43
	regionHeaderCapacityOff = 47
	regionHeaderUsedOff     = 51
	regionHeaderFlagsOff    = 55
)

var regionCatalogMu sync.Mutex

type regionCatalogFooter struct {
	start    uint64
	capacity uint64
	used     uint64
}

type regionCatalogEntry struct {
	keyHash  uint64
	offset   uint64
	capacity uint64
	used     uint64
}

// SecondaryIndexStoreStats reports the embedded secondary index region state.
type SecondaryIndexStoreStats struct {
	Exists       bool
	Start        uint64
	Capacity     uint64
	Used         uint64
	EntryCount   uint32
	PayloadMagic string
}

type regionBackedBTreePageStore struct {
	file     *os.File
	keyHash  uint64
	base     uint64
	capacity uint64
	fileName string
}

func shouldUseRegionStore(tableName, fieldName string) bool {
	if !useExperimentalRegionIndex() {
		return false
	}

	target := os.Getenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX_KEY")
	current := fieldName
	if tableName != "" {
		current = tableName + "." + fieldName
	}

	if target == "" {
		return true
	}

	return target == current
}

func regionIndexUnsafeMode() bool {
	return os.Getenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX_UNSAFE") == "1"
}

func newRegionBackedBTreePageStore(dbFileName, tableName, fieldName string) (*regionBackedBTreePageStore, bool, error) {
	file, err := os.OpenFile(dbFileName, os.O_RDWR, 0644)
	if err != nil {
		return nil, false, err
	}

	store := &regionBackedBTreePageStore{
		file:     file,
		fileName: dbFileName,
		keyHash:  hashIndexKey(tableName, fieldName),
	}

	regionCatalogMu.Lock()
	defer regionCatalogMu.Unlock()

	exists, err := ensureRegionCatalogAndEntry(file, store.keyHash)
	if err != nil {
		file.Close()
		return nil, false, err
	}

	if err := store.refreshLocationLocked(); err != nil {
		file.Close()
		return nil, false, err
	}

	return store, exists, nil
}

func (s *regionBackedBTreePageStore) readRaw(offset int64, buf []byte) error {
	regionCatalogMu.Lock()
	defer regionCatalogMu.Unlock()

	if err := s.refreshLocationLocked(); err != nil {
		return err
	}

	_, err := s.file.ReadAt(buf, int64(s.base)+offset)
	return err
}

func (s *regionBackedBTreePageStore) writeRaw(offset int64, data []byte) error {
	regionCatalogMu.Lock()
	defer regionCatalogMu.Unlock()

	if err := s.refreshLocationLocked(); err != nil {
		return err
	}

	_, err := s.file.WriteAt(data, int64(s.base)+offset)
	return err
}

func (s *regionBackedBTreePageStore) readPage(pageNum uint32) ([]byte, error) {
	buf := make([]byte, BTreePageSize)
	offset := int64(BTreeHeaderLen + (pageNum * BTreePageSize))
	if err := s.readRaw(offset, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *regionBackedBTreePageStore) writePage(pageNum uint32, data []byte) error {
	if len(data) < BTreePageSize {
		return fmt.Errorf("page write too small: %d", len(data))
	}
	offset := int64(BTreeHeaderLen + (pageNum * BTreePageSize))
	return s.writeRaw(offset, data[:BTreePageSize])
}

func (s *regionBackedBTreePageStore) ensurePageCapacity(pageCount uint32) error {
	required := uint64(BTreeHeaderLen + (pageCount * BTreePageSize))

	regionCatalogMu.Lock()
	defer regionCatalogMu.Unlock()

	if err := s.refreshLocationLocked(); err != nil {
		return err
	}
	if required <= s.capacity {
		return nil
	}

	return growRegionCatalogEntry(s.file, s.keyHash, required)
}

func (s *regionBackedBTreePageStore) sync() error {
	return s.file.Sync()
}

func (s *regionBackedBTreePageStore) close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *regionBackedBTreePageStore) refreshLocationLocked() error {
	footer, data, err := loadRegionCatalog(s.file)
	if err != nil {
		return err
	}

	entryIdx, entry, ok := findRegionCatalogEntry(data, s.keyHash)
	if !ok {
		_, err := ensureRegionCatalogAndEntry(s.file, s.keyHash)
		if err != nil {
			return err
		}
		footer, data, err = loadRegionCatalog(s.file)
		if err != nil {
			return err
		}
		entryIdx, entry, ok = findRegionCatalogEntry(data, s.keyHash)
		if !ok {
			return fmt.Errorf("region catalog entry not found after create")
		}
	}

	_ = entryIdx
	s.base = footer.start + entry.offset
	s.capacity = entry.capacity
	return nil
}

func hashIndexKey(tableName, fieldName string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tableName))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(fieldName))
	return h.Sum64()
}

// GetSecondaryIndexStoreStats returns embedded secondary index region stats.
func GetSecondaryIndexStoreStats(dbFileName string) (*SecondaryIndexStoreStats, error) {
	file, err := os.Open(dbFileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	footer, hasFooter, err := readRegionCatalogFooter(file)
	if err != nil {
		return nil, err
	}
	if !hasFooter {
		return &SecondaryIndexStoreStats{Exists: false}, nil
	}

	stats := &SecondaryIndexStoreStats{
		Exists:       true,
		Start:        footer.start,
		Capacity:     footer.capacity,
		Used:         footer.used,
		PayloadMagic: regionCatalogMagic,
	}

	data, err := readRegionCatalogData(file, footer)
	if err == nil {
		var count uint32
		for i := 0; i < regionCatalogMaxEntries; i++ {
			e := decodeRegionCatalogEntry(data, i)
			if e.keyHash != 0 {
				count++
			}
		}
		stats.EntryCount = count
	}

	return stats, nil
}

func ensureRegionCatalogAndEntry(file *os.File, keyHash uint64) (bool, error) {
	footer, hasFooter, err := readRegionCatalogFooter(file)
	if err != nil {
		return false, err
	}

	if !hasFooter {
		if err := createRegionCatalog(file); err != nil {
			return false, err
		}
		footer, hasFooter, err = readRegionCatalogFooter(file)
		if err != nil {
			return false, err
		}
		if !hasFooter {
			return false, fmt.Errorf("failed to create region catalog footer")
		}
	}

	data, err := readRegionCatalogData(file, footer)
	if err != nil {
		return false, err
	}

	if _, _, ok := findRegionCatalogEntry(data, keyHash); ok {
		return true, nil
	}

	entryIdx := -1
	for i := 0; i < regionCatalogMaxEntries; i++ {
		e := decodeRegionCatalogEntry(data, i)
		if e.keyHash == 0 {
			entryIdx = i
			break
		}
	}
	if entryIdx < 0 {
		return false, fmt.Errorf("region catalog full")
	}

	nextOff := alignRegionOffset(readRegionCatalogUsed(data), BTreePageSize)
	initialCap := uint64(BTreeHeaderLen + (regionIndexGrowPages * BTreePageSize))
	requiredUsed := nextOff + initialCap

	if requiredUsed > footer.capacity {
		if err := growRegionCatalog(file, requiredUsed+regionCatalogInitialGrowth, 0); err != nil {
			return false, err
		}
		footer, data, err = loadRegionCatalog(file)
		if err != nil {
			return false, err
		}
		nextOff = alignRegionOffset(readRegionCatalogUsed(data), BTreePageSize)
		requiredUsed = nextOff + initialCap
		if requiredUsed > footer.capacity {
			return false, fmt.Errorf("failed to allocate region catalog entry capacity")
		}
	}

	entry := regionCatalogEntry{keyHash: keyHash, offset: nextOff, capacity: initialCap, used: 0}
	encodeRegionCatalogEntry(data, entryIdx, entry)
	setRegionCatalogUsed(data, requiredUsed)

	if err := writeRegionCatalogData(file, footer, data); err != nil {
		return false, err
	}

	return false, nil
}

func growRegionCatalogEntry(file *os.File, keyHash uint64, required uint64) error {
	footer, data, err := loadRegionCatalog(file)
	if err != nil {
		return err
	}

	entryIdx, entry, ok := findRegionCatalogEntry(data, keyHash)
	if !ok {
		return fmt.Errorf("missing region catalog entry")
	}
	if required <= entry.capacity {
		return nil
	}

	newCap := required + uint64(regionIndexGrowPages*BTreePageSize)
	if newCap < required+(required/2) {
		newCap = required + (required / 2)
	}

	nextOff := alignRegionOffset(readRegionCatalogUsed(data), BTreePageSize)
	requiredUsed := nextOff + newCap
	if requiredUsed > footer.capacity {
		if err := growRegionCatalog(file, requiredUsed+regionCatalogInitialGrowth, 0); err != nil {
			return err
		}
		footer, data, err = loadRegionCatalog(file)
		if err != nil {
			return err
		}
		entryIdx, entry, ok = findRegionCatalogEntry(data, keyHash)
		if !ok {
			return fmt.Errorf("missing region catalog entry after grow")
		}
		nextOff = alignRegionOffset(readRegionCatalogUsed(data), BTreePageSize)
		requiredUsed = nextOff + newCap
	}

	oldBytes := make([]byte, entry.capacity)
	if _, err := file.ReadAt(oldBytes, int64(footer.start+entry.offset)); err != nil {
		return err
	}
	if _, err := file.WriteAt(oldBytes, int64(footer.start+nextOff)); err != nil {
		return err
	}

	entry.offset = nextOff
	entry.capacity = newCap
	encodeRegionCatalogEntry(data, entryIdx, entry)
	setRegionCatalogUsed(data, requiredUsed)

	if err := writeRegionCatalogData(file, footer, data); err != nil {
		return err
	}

	return nil
}

func loadRegionCatalog(file *os.File) (regionCatalogFooter, []byte, error) {
	footer, hasFooter, err := readRegionCatalogFooter(file)
	if err != nil {
		return regionCatalogFooter{}, nil, err
	}
	if !hasFooter {
		return regionCatalogFooter{}, nil, fmt.Errorf("region catalog footer missing")
	}
	data, err := readRegionCatalogData(file, footer)
	if err != nil {
		return regionCatalogFooter{}, nil, err
	}
	return footer, data, nil
}

func createRegionCatalog(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}

	start := uint64(info.Size())
	capacity := uint64(regionCatalogMetaSize + regionCatalogInitialGrowth)
	newSize := int64(start + capacity + regionCatalogFooterSize)
	if err := file.Truncate(newSize); err != nil {
		return err
	}

	data := make([]byte, regionCatalogMetaSize)
	copy(data[:8], []byte(regionCatalogMagic))
	binary.LittleEndian.PutUint32(data[8:12], regionCatalogVersion)
	binary.LittleEndian.PutUint32(data[12:16], regionCatalogMaxEntries)
	binary.LittleEndian.PutUint64(data[16:24], uint64(regionCatalogMetaSize))

	if _, err := file.WriteAt(data, int64(start)); err != nil {
		return err
	}

	footer := regionCatalogFooter{start: start, capacity: capacity, used: uint64(regionCatalogMetaSize)}
	if err := writeRegionCatalogFooter(file, footer); err != nil {
		return err
	}

	return nil
}

func growRegionCatalog(file *os.File, minCapacity uint64, minStart int64) error {
	footer, hasFooter, err := readRegionCatalogFooter(file)
	if err != nil {
		return err
	}
	if !hasFooter {
		return fmt.Errorf("cannot grow missing region catalog")
	}

	info, err := file.Stat()
	if err != nil {
		return err
	}

	newCap := footer.capacity
	if newCap < minCapacity {
		newCap = minCapacity
	}
	if newCap < footer.capacity+(footer.capacity/2) {
		newCap = footer.capacity + (footer.capacity / 2)
	}

	sectionEnd := int64(footer.start + footer.capacity + regionCatalogFooterSize)
	canGrowInPlace := sectionEnd == info.Size() && int64(footer.start) >= minStart
	if canGrowInPlace {
		if err := file.Truncate(int64(footer.start + newCap + regionCatalogFooterSize)); err != nil {
			return err
		}
		footer.capacity = newCap
		return writeRegionCatalogFooter(file, footer)
	}

	old := make([]byte, footer.capacity)
	if _, err := file.ReadAt(old, int64(footer.start)); err != nil {
		return err
	}

	baseEnd := info.Size() - regionCatalogFooterSize
	newStart := uint64(baseEnd)
	if int64(newStart) < minStart {
		newStart = uint64(minStart)
	}

	if err := file.Truncate(int64(newStart + newCap + regionCatalogFooterSize)); err != nil {
		return err
	}
	if _, err := file.WriteAt(old, int64(newStart)); err != nil {
		return err
	}

	footer.start = newStart
	footer.capacity = newCap
	return writeRegionCatalogFooter(file, footer)
}

func ensureRegionIndexBlobAfter(file *os.File, minEnd int64) error {
	regionCatalogMu.Lock()
	defer regionCatalogMu.Unlock()

	footer, hasFooter, err := readRegionCatalogFooter(file)
	if err != nil {
		return err
	}
	if !hasFooter {
		return nil
	}

	if int64(footer.start) >= minEnd {
		return nil
	}

	if err := growRegionCatalog(file, footer.capacity, minEnd); err != nil {
		return err
	}

	return nil
}

func readRegionCatalogFooter(file *os.File) (regionCatalogFooter, bool, error) {
	info, err := file.Stat()
	if err != nil {
		return regionCatalogFooter{}, false, err
	}
	if info.Size() < regionCatalogFooterSize {
		return regionCatalogFooter{}, false, nil
	}

	buf := make([]byte, regionCatalogFooterSize)
	if _, err := file.ReadAt(buf, info.Size()-regionCatalogFooterSize); err != nil {
		return regionCatalogFooter{}, false, err
	}
	footer, ok := decodeRegionCatalogFooter(buf)
	if ok && int64(footer.start+footer.capacity+regionCatalogFooterSize) <= info.Size() {
		return footer, true, nil
	}

	if regionIndexUnsafeMode() {
		return regionCatalogFooter{}, false, nil
	}

	hf, hasHeader, err := readRegionCatalogHeaderPointers(file)
	if err != nil {
		return regionCatalogFooter{}, false, err
	}
	if !hasHeader {
		return regionCatalogFooter{}, false, nil
	}

	hb := make([]byte, regionCatalogFooterSize)
	if _, err := file.ReadAt(hb, int64(hf.start+hf.capacity)); err == nil {
		footer, ok := decodeRegionCatalogFooter(hb)
		if ok && footer.start == hf.start && footer.capacity == hf.capacity {
			if footer.used == 0 {
				footer.used = hf.used
			}
			return footer, true, nil
		}
	}

	return hf, true, nil
}

func writeRegionCatalogFooter(file *os.File, footer regionCatalogFooter) error {
	buf := make([]byte, regionCatalogFooterSize)
	copy(buf[:8], []byte(regionCatalogFooterMagic))
	binary.LittleEndian.PutUint64(buf[8:16], footer.start)
	binary.LittleEndian.PutUint64(buf[16:24], footer.capacity)
	binary.LittleEndian.PutUint64(buf[24:32], footer.used)
	_, err := file.WriteAt(buf, int64(footer.start+footer.capacity))
	return err
}

func decodeRegionCatalogFooter(buf []byte) (regionCatalogFooter, bool) {
	if len(buf) < regionCatalogFooterSize {
		return regionCatalogFooter{}, false
	}
	if string(buf[:8]) != regionCatalogFooterMagic {
		return regionCatalogFooter{}, false
	}

	footer := regionCatalogFooter{
		start:    binary.LittleEndian.Uint64(buf[8:16]),
		capacity: binary.LittleEndian.Uint64(buf[16:24]),
		used:     binary.LittleEndian.Uint64(buf[24:32]),
	}
	if footer.capacity < uint64(regionCatalogMetaSize) || footer.used < uint64(regionCatalogMetaSize) || footer.used > footer.capacity {
		return regionCatalogFooter{}, false
	}
	return footer, true
}

func readRegionCatalogHeaderPointers(file *os.File) (regionCatalogFooter, bool, error) {
	h := make([]byte, headerSize)
	if _, err := file.ReadAt(h, 0); err != nil {
		return regionCatalogFooter{}, false, nil
	}

	start := binary.BigEndian.Uint32(h[regionHeaderStartOff:regionHeaderCapacityOff])
	capacity := binary.BigEndian.Uint32(h[regionHeaderCapacityOff:regionHeaderUsedOff])
	used := binary.BigEndian.Uint32(h[regionHeaderUsedOff:regionHeaderFlagsOff])

	if start == 0 || capacity == 0 {
		return regionCatalogFooter{}, false, nil
	}

	return regionCatalogFooter{start: uint64(start), capacity: uint64(capacity), used: uint64(used)}, true, nil
}

func writeRegionCatalogHeaderPointers(file *os.File, footer regionCatalogFooter, flags uint32) error {
	h := make([]byte, headerSize)
	if _, err := file.ReadAt(h, 0); err != nil {
		return err
	}

	binary.BigEndian.PutUint32(h[regionHeaderStartOff:regionHeaderCapacityOff], uint32(footer.start))
	binary.BigEndian.PutUint32(h[regionHeaderCapacityOff:regionHeaderUsedOff], uint32(footer.capacity))
	binary.BigEndian.PutUint32(h[regionHeaderUsedOff:regionHeaderFlagsOff], uint32(footer.used))
	binary.BigEndian.PutUint32(h[regionHeaderFlagsOff:59], flags)

	_, err := file.WriteAt(h, 0)
	return err
}

func syncRegionCatalogHeaderPointersFromFooter(file *os.File, header *DBHeader) error {
	if file == nil || header == nil {
		return nil
	}

	footer, hasFooter, err := readRegionCatalogFooter(file)
	if err != nil {
		return err
	}
	if !hasFooter {
		header.secondaryIndexRegionStart = 0
		header.secondaryIndexRegionCapacity = 0
		header.secondaryIndexRegionUsed = 0
		header.secondaryIndexRegionFlags = 0
		return nil
	}

	header.secondaryIndexRegionStart = uint32(footer.start)
	header.secondaryIndexRegionCapacity = uint32(footer.capacity)
	header.secondaryIndexRegionUsed = uint32(footer.used)
	header.secondaryIndexRegionFlags = regionCatalogVersion

	if regionIndexUnsafeMode() {
		return nil
	}

	return writeRegionCatalogHeaderPointers(file, footer, regionCatalogVersion)
}

func readRegionCatalogData(file *os.File, footer regionCatalogFooter) ([]byte, error) {
	data := make([]byte, regionCatalogMetaSize)
	if _, err := file.ReadAt(data, int64(footer.start)); err != nil {
		return nil, err
	}
	if string(data[:8]) != regionCatalogMagic {
		return nil, fmt.Errorf("invalid region catalog magic")
	}
	if binary.LittleEndian.Uint32(data[8:12]) != regionCatalogVersion {
		return nil, fmt.Errorf("unsupported region catalog version")
	}
	if binary.LittleEndian.Uint32(data[12:16]) != regionCatalogMaxEntries {
		return nil, fmt.Errorf("unexpected region catalog max entries")
	}
	return data, nil
}

func writeRegionCatalogData(file *os.File, footer regionCatalogFooter, data []byte) error {
	if len(data) < regionCatalogMetaSize {
		return fmt.Errorf("catalog metadata too small")
	}
	if _, err := file.WriteAt(data[:regionCatalogMetaSize], int64(footer.start)); err != nil {
		return err
	}
	footer.used = readRegionCatalogUsed(data)
	if err := writeRegionCatalogFooter(file, footer); err != nil {
		return err
	}
	return nil
}

func findRegionCatalogEntry(data []byte, keyHash uint64) (int, regionCatalogEntry, bool) {
	for i := 0; i < regionCatalogMaxEntries; i++ {
		e := decodeRegionCatalogEntry(data, i)
		if e.keyHash == keyHash {
			return i, e, true
		}
	}
	return -1, regionCatalogEntry{}, false
}

func decodeRegionCatalogEntry(data []byte, idx int) regionCatalogEntry {
	off := regionCatalogHeaderSize + (idx * regionCatalogEntrySize)
	return regionCatalogEntry{
		keyHash:  binary.LittleEndian.Uint64(data[off : off+8]),
		offset:   binary.LittleEndian.Uint64(data[off+8 : off+16]),
		capacity: binary.LittleEndian.Uint64(data[off+16 : off+24]),
		used:     binary.LittleEndian.Uint64(data[off+24 : off+32]),
	}
}

func encodeRegionCatalogEntry(data []byte, idx int, e regionCatalogEntry) {
	off := regionCatalogHeaderSize + (idx * regionCatalogEntrySize)
	binary.LittleEndian.PutUint64(data[off:off+8], e.keyHash)
	binary.LittleEndian.PutUint64(data[off+8:off+16], e.offset)
	binary.LittleEndian.PutUint64(data[off+16:off+24], e.capacity)
	binary.LittleEndian.PutUint64(data[off+24:off+32], e.used)
}

func readRegionCatalogUsed(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data[16:24])
}

func setRegionCatalogUsed(data []byte, used uint64) {
	binary.LittleEndian.PutUint64(data[16:24], used)
}

func alignRegionOffset(offset uint64, align int) uint64 {
	a := uint64(align)
	if a == 0 {
		return offset
	}
	rem := offset % a
	if rem == 0 {
		return offset
	}
	return offset + (a - rem)
}

func regionCatalogEntryExists(file *os.File, tableName, fieldName string) (bool, error) {
	if file == nil {
		return false, nil
	}

	regionCatalogMu.Lock()
	defer regionCatalogMu.Unlock()

	footer, hasFooter, err := readRegionCatalogFooter(file)
	if err != nil {
		return false, err
	}
	if !hasFooter {
		return false, nil
	}

	data, err := readRegionCatalogData(file, footer)
	if err != nil {
		return false, err
	}

	_, _, ok := findRegionCatalogEntry(data, hashIndexKey(tableName, fieldName))
	return ok, nil
}
