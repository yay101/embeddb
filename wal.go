package embeddb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

const (
	WALMagic    = "embeddb wal"
	WALVersion  = byte(1)
	WALHeaderSz = 12

	WALWrite    = byte(1)
	WALTruncate = byte(2)

	WALEntryHeaderSz = 13
)

type wal struct {
	file     *os.File
	filename string
	buf      []byte
}

func openWAL(dbFilename string) (*wal, error) {
	walFilename := dbFilename + ".wal"
	f, err := os.OpenFile(walFilename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("openWAL: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if stat.Size() == 0 {
		header := make([]byte, WALHeaderSz)
		copy(header, []byte(WALMagic))
		header[11] = WALVersion
		if _, err := f.Write(header); err != nil {
			f.Close()
			return nil, err
		}
		f.Sync()
	} else {
		header := make([]byte, WALHeaderSz)
		if _, err := f.ReadAt(header, 0); err != nil {
			f.Close()
			return nil, fmt.Errorf("openWAL: read header: %w", err)
		}
		if string(header[:11]) != WALMagic {
			f.Close()
			return nil, fmt.Errorf("openWAL: invalid magic")
		}
		if header[11] != WALVersion {
			f.Close()
			return nil, fmt.Errorf("openWAL: unsupported version %d", header[11])
		}
	}

	return &wal{
		file:     f,
		filename: walFilename,
		buf:      make([]byte, 0, 65536),
	}, nil
}

func (w *wal) writeEntry(entryType byte, offset uint64, data []byte) error {
	w.buf = w.buf[:0]
	w.buf = append(w.buf, entryType)
	w.buf = binary.LittleEndian.AppendUint64(w.buf, offset)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(len(data)))
	w.buf = append(w.buf, data...)
	crc := crc32.ChecksumIEEE(w.buf)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, crc)

	_, err := w.file.Write(w.buf)
	return err
}

func (w *wal) Write(offset uint64, data []byte) error {
	return w.writeEntry(WALWrite, offset, data)
}

func (w *wal) Truncate(size uint64) error {
	return w.writeEntry(WALTruncate, size, nil)
}

func (w *wal) Sync() error {
	return w.file.Sync()
}

func (w *wal) Replay(mainFile *os.File) error {
	stat, err := w.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() <= WALHeaderSz {
		return nil
	}

	walData, err := os.ReadFile(w.filename)
	if err != nil {
		return err
	}

	pos := WALHeaderSz
	for pos < len(walData) {
		if pos+1 > len(walData) {
			break
		}
		entryType := walData[pos]
		pos++

		if pos+12 > len(walData) {
			break
		}
		offset := binary.LittleEndian.Uint64(walData[pos : pos+8])
		length := binary.LittleEndian.Uint32(walData[pos+8 : pos+12])
		pos += 12

		if pos+int(length)+4 > len(walData) {
			break
		}
		data := walData[pos : pos+int(length)]
		storedCRC := binary.LittleEndian.Uint32(walData[pos+int(length) : pos+int(length)+4])
		pos += int(length) + 4

		headerLen := 1 + 8 + 4
		crcData := walData[pos-int(length)-4-headerLen : pos-4]
		computedCRC := crc32.ChecksumIEEE(crcData)
		if storedCRC != computedCRC {
			break
		}

		switch entryType {
		case WALWrite:
			_, err := mainFile.WriteAt(data, int64(offset))
			if err != nil {
				return fmt.Errorf("wal replay write at %d: %w", offset, err)
			}
		case WALTruncate:
			if err := mainFile.Truncate(int64(offset)); err != nil {
				return fmt.Errorf("wal replay truncate to %d: %w", offset, err)
			}
		}
	}

	if err := mainFile.Sync(); err != nil {
		return err
	}

	w.file.Close()
	if err := os.Remove(w.filename); err != nil {
		return err
	}

	w.file = nil
	return nil
}

func (w *wal) Checkpoint(mainFile *os.File) error {
	if w == nil || w.file == nil {
		return nil
	}

	if err := w.Sync(); err != nil {
		return err
	}
	if err := mainFile.Sync(); err != nil {
		return err
	}

	w.file.Close()
	if err := os.Remove(w.filename); err != nil {
		return err
	}

	f, err := os.OpenFile(w.filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	header := make([]byte, WALHeaderSz)
	copy(header, []byte(WALMagic))
	header[11] = WALVersion
	if _, err := f.Write(header); err != nil {
		f.Close()
		return err
	}
	f.Sync()

	w.file = f
	return nil
}

func (w *wal) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	return w.file.Close()
}
