package embeddb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	secondaryIndexFooterMagic  = "S2IDXV1!"
	secondaryIndexPayloadMagic = "S2PAYV1!"
	secondaryIndexFooterSize   = 32
	secondaryIndexGrowBuffer   = 64 * 1024
)

type secondaryIndexFooter struct {
	start    uint64
	capacity uint64
	used     uint64
}

// SecondaryIndexStoreStats reports the embedded secondary index blob state.
// Exists=false means no embedded secondary index section is present.
type SecondaryIndexStoreStats struct {
	Exists       bool
	Start        uint64
	Capacity     uint64
	Used         uint64
	EntryCount   uint32
	PayloadMagic string
}

// GetSecondaryIndexStoreStats returns current embedded-secondary-index section stats.
func GetSecondaryIndexStoreStats(dbFileName string) (*SecondaryIndexStoreStats, error) {
	file, err := os.Open(dbFileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	footer, hasFooter, err := readSecondaryFooter(file, info.Size())
	if err != nil {
		return nil, err
	}
	if !hasFooter {
		return &SecondaryIndexStoreStats{Exists: false}, nil
	}

	stats := &SecondaryIndexStoreStats{
		Exists:   true,
		Start:    footer.start,
		Capacity: footer.capacity,
		Used:     footer.used,
	}

	if footer.used >= 8 {
		magic := make([]byte, 8)
		if _, err := file.ReadAt(magic, int64(footer.start)); err == nil {
			stats.PayloadMagic = string(magic)
		}
	}

	if footer.used >= 12 {
		hdr := make([]byte, 12)
		if _, err := file.ReadAt(hdr, int64(footer.start)); err == nil {
			if string(hdr[:8]) == secondaryIndexPayloadMagic {
				stats.EntryCount = binary.LittleEndian.Uint32(hdr[8:12])
			}
		}
	}

	return stats, nil
}

func persistSecondaryIndexesToDB(dbFileName string) error {
	indexFiles, err := listSecondaryIndexFiles(dbFileName)
	if err != nil {
		return err
	}

	payload, err := encodeSecondaryIndexPayload(indexFiles)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(dbFileName, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	footer, hasFooter, err := readSecondaryFooter(file, info.Size())
	if err != nil {
		return err
	}

	required := uint64(len(payload))
	if required == 0 {
		required = 1
	}

	if hasFooter && footer.capacity >= required {
		if _, err := file.WriteAt(payload, int64(footer.start)); err != nil {
			return err
		}
		if err := writeSecondaryFooter(file, footer.start, footer.capacity, uint64(len(payload))); err != nil {
			return err
		}
		return file.Sync()
	}

	baseEnd := info.Size()
	if hasFooter {
		baseEnd = info.Size() - secondaryIndexFooterSize
	}

	capacity := required + secondaryIndexGrowBuffer
	if capacity < required+(required/2) {
		capacity = required + (required / 2)
	}

	start := uint64(baseEnd)
	newSize := int64(start + capacity + secondaryIndexFooterSize)
	if err := file.Truncate(newSize); err != nil {
		return err
	}

	if _, err := file.WriteAt(payload, int64(start)); err != nil {
		return err
	}
	if err := writeSecondaryFooter(file, start, capacity, uint64(len(payload))); err != nil {
		return err
	}

	return file.Sync()
}

func restoreSecondaryIndexesFromDB(dbFileName string) error {
	file, err := os.Open(dbFileName)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	footer, hasFooter, err := readSecondaryFooter(file, info.Size())
	if err != nil {
		return err
	}
	if !hasFooter || footer.used == 0 {
		return nil
	}

	buf := make([]byte, footer.used)
	if _, err := file.ReadAt(buf, int64(footer.start)); err != nil {
		return err
	}

	dir := filepath.Dir(dbFileName)
	dbBase := filepath.Base(dbFileName)

	if len(buf) < 12 || string(buf[:8]) != secondaryIndexPayloadMagic {
		return nil
	}

	count := binary.LittleEndian.Uint32(buf[8:12])
	offset := 12
	for i := uint32(0); i < count; i++ {
		if offset+6 > len(buf) {
			break
		}
		nameLen := int(binary.LittleEndian.Uint16(buf[offset : offset+2]))
		offset += 2
		dataLen := int(binary.LittleEndian.Uint32(buf[offset : offset+4]))
		offset += 4

		if nameLen <= 0 || dataLen < 0 || offset+nameLen+dataLen > len(buf) {
			break
		}

		name := string(buf[offset : offset+nameLen])
		offset += nameLen
		data := buf[offset : offset+dataLen]
		offset += dataLen

		if strings.Contains(name, "/") || strings.Contains(name, "\\") {
			continue
		}
		if !strings.HasSuffix(name, ".idx") {
			continue
		}
		if !strings.HasPrefix(name, dbBase+".") {
			continue
		}

		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0644); err != nil {
			return err
		}
	}

	return nil
}

func listSecondaryIndexFiles(dbFileName string) ([]string, error) {
	dir := filepath.Dir(dbFileName)
	base := filepath.Base(dbFileName)

	patterns := []string{
		filepath.Join(dir, fmt.Sprintf("%s.*.idx", base)),
		filepath.Join(dir, fmt.Sprintf("%s.idx", base)),
	}

	seen := make(map[string]struct{})
	paths := make([]string, 0)
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		for _, m := range matches {
			if _, ok := seen[m]; ok {
				continue
			}
			seen[m] = struct{}{}
			paths = append(paths, m)
		}
	}

	sort.Strings(paths)
	return paths, nil
}

func cleanupSecondaryIndexFiles(dbFileName string) error {
	paths, err := listSecondaryIndexFiles(dbFileName)
	if err != nil {
		return err
	}
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func ensureSecondaryIndexBlobAfter(file *os.File, minEnd int64) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}

	footer, hasFooter, err := readSecondaryFooter(file, info.Size())
	if err != nil {
		return err
	}
	if !hasFooter {
		return nil
	}

	if int64(footer.start) >= minEnd {
		return nil
	}

	oldData := make([]byte, footer.capacity)
	if _, err := file.ReadAt(oldData, int64(footer.start)); err != nil {
		return err
	}

	newStart := uint64(minEnd)
	newSize := int64(newStart + footer.capacity + secondaryIndexFooterSize)
	if err := file.Truncate(newSize); err != nil {
		return err
	}

	if _, err := file.WriteAt(oldData, int64(newStart)); err != nil {
		return err
	}
	if err := writeSecondaryFooter(file, newStart, footer.capacity, footer.used); err != nil {
		return err
	}

	return file.Sync()
}

func encodeSecondaryIndexPayload(indexFiles []string) ([]byte, error) {
	payload := make([]byte, 0, 1024)
	payload = append(payload, []byte(secondaryIndexPayloadMagic)...)

	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(indexFiles)))
	payload = append(payload, countBuf...)

	for _, path := range indexFiles {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}

		name := filepath.Base(path)
		nameLen := len(name)
		if nameLen == 0 || nameLen > 65535 {
			continue
		}

		nameLenBuf := make([]byte, 2)
		binary.LittleEndian.PutUint16(nameLenBuf, uint16(nameLen))
		payload = append(payload, nameLenBuf...)

		dataLenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(dataLenBuf, uint32(len(data)))
		payload = append(payload, dataLenBuf...)

		payload = append(payload, []byte(name)...)
		payload = append(payload, data...)
	}

	return payload, nil
}

func readSecondaryFooter(file *os.File, fileSize int64) (secondaryIndexFooter, bool, error) {
	if fileSize < secondaryIndexFooterSize {
		return secondaryIndexFooter{}, false, nil
	}

	buf := make([]byte, secondaryIndexFooterSize)
	if _, err := file.ReadAt(buf, fileSize-secondaryIndexFooterSize); err != nil {
		return secondaryIndexFooter{}, false, err
	}

	if string(buf[:8]) != secondaryIndexFooterMagic {
		return secondaryIndexFooter{}, false, nil
	}

	footer := secondaryIndexFooter{
		start:    binary.LittleEndian.Uint64(buf[8:16]),
		capacity: binary.LittleEndian.Uint64(buf[16:24]),
		used:     binary.LittleEndian.Uint64(buf[24:32]),
	}

	if footer.capacity == 0 || footer.used > footer.capacity {
		return secondaryIndexFooter{}, false, nil
	}

	footerEnd := int64(footer.start + footer.capacity + secondaryIndexFooterSize)
	if footerEnd > fileSize {
		return secondaryIndexFooter{}, false, nil
	}

	return footer, true, nil
}

func writeSecondaryFooter(file *os.File, start, capacity, used uint64) error {
	buf := make([]byte, secondaryIndexFooterSize)
	copy(buf[:8], []byte(secondaryIndexFooterMagic))
	binary.LittleEndian.PutUint64(buf[8:16], start)
	binary.LittleEndian.PutUint64(buf[16:24], capacity)
	binary.LittleEndian.PutUint64(buf[24:32], used)

	footerOffset := int64(start + capacity)
	_, err := file.WriteAt(buf, footerOffset)
	return err
}
