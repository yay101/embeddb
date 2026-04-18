package embeddb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

const (
	WALMagic           uint32 = 0xEDB1A111
	WALFrameHeaderSize        = 17 // 4 magic + 8 txID + 1 type + 4 CRC
)

const (
	FrameTypeInsert  byte = 0x01
	FrameTypeUpdate  byte = 0x02
	FrameTypeDelete  byte = 0x03
	FrameTypeIndex   byte = 0x04
	FrameTypeCatalog byte = 0x05
	FrameTypeCommit  byte = 0xFF
)

type WALFrame struct {
	Type    byte
	TxID    uint64
	Payload []byte
	Offset  int64
}

func encodeWALFrame(frame *WALFrame) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, WALMagic)

	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0) // txID (8 bytes)
	binary.LittleEndian.PutUint64(buf[4:12], frame.TxID)

	buf = append(buf, frame.Type)

	payloadLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(payloadLen, uint32(len(frame.Payload)))
	buf = append(buf, payloadLen...)
	buf = append(buf, frame.Payload...)

	crc := crc32.ChecksumIEEE(buf)
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	buf = append(buf, crcBuf...)

	return buf
}

func decodeWALFrame(data []byte) (*WALFrame, error) {
	if len(data) < WALFrameHeaderSize+4 {
		return nil, fmt.Errorf("WAL frame too short")
	}

	magic := binary.LittleEndian.Uint32(data[:4])
	if magic != WALMagic {
		return nil, fmt.Errorf("invalid WAL magic: %08x", magic)
	}

	txID := binary.LittleEndian.Uint64(data[4:12])
	frameType := data[12]

	payloadLen := binary.LittleEndian.Uint32(data[13:17])
	if len(data) < int(17+payloadLen)+4 {
		return nil, fmt.Errorf("WAL frame payload length mismatch")
	}

	payload := data[17 : 17+payloadLen]

	storedCRC := binary.LittleEndian.Uint32(data[17+payloadLen : 17+payloadLen+4])
	computedCRC := crc32.ChecksumIEEE(data[:17+payloadLen])
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("WAL frame CRC mismatch")
	}

	return &WALFrame{
		Type:    frameType,
		TxID:    txID,
		Payload: payload,
	}, nil
}

type WALWriter struct {
	file     *os.File
	nextTxID uint64
}

func NewWALWriter(file *os.File) *WALWriter {
	return &WALWriter{
		file:     file,
		nextTxID: 1,
	}
}

func (w *WALWriter) WriteFrame(txID uint64, frameType byte, payload []byte) error {
	frame := &WALFrame{
		Type:    frameType,
		TxID:    txID,
		Payload: payload,
	}
	data := encodeWALFrame(frame)

	_, err := w.file.Write(data)
	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WALWriter) GetNextTxID() uint64 {
	txID := w.nextTxID
	w.nextTxID++
	return txID
}

type WALReader struct {
	file   *os.File
	offset int64
}

func NewWALReader(file *os.File) *WALReader {
	return &WALReader{
		file:   file,
		offset: 0,
	}
}

func (r *WALReader) ReadFrame() (*WALFrame, error) {
	header := make([]byte, 17)
	n, err := r.file.ReadAt(header, r.offset)
	if err != nil {
		return nil, err
	}
	if n < 17 {
		return nil, fmt.Errorf("WAL: incomplete frame header at offset %d", r.offset)
	}

	payloadLen := binary.LittleEndian.Uint32(header[13:17])
	frameData := make([]byte, 17+payloadLen+4)
	copy(frameData[:17], header)
	n, err = r.file.ReadAt(frameData[17:], r.offset+17)
	if err != nil {
		return nil, err
	}

	r.offset += int64(17 + payloadLen + 4)

	return decodeWALFrame(frameData)
}

func (r *WALReader) ReadAllFrames() ([]*WALFrame, error) {
	var frames []*WALFrame
	for {
		frame, err := r.ReadFrame()
		if err != nil {
			if err.Error() == "WAL: incomplete frame header at offset" {
				break
			}
			return nil, err
		}
		if frame == nil {
			break
		}
		frames = append(frames, frame)
	}
	return frames, nil
}
