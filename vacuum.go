package embeddb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	embedcore "github.com/yay101/embeddbcore"
	"github.com/yay101/embeddbmmap"
)

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
