package embeddb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"
	"strings"
	"time"

	embedcore "github.com/yay101/embeddbcore"
)

const (
	V2RecordVersion byte = 0x01
)

var ErrInvalidRecord = errors.New("invalid record format")

type RecordHeader struct {
	Version        byte
	Flags          byte
	TableID        uint8
	RecordID       uint32
	PrevVersionOff uint64
	SchemaVersion  uint32
	PayloadLen     uint32
}

func decodeRecordHeader(data []byte) (RecordHeader, error) {
	if len(data) < embedcore.RecordHeaderSize {
		return RecordHeader{}, fmt.Errorf("%w: header too short (%d bytes)", ErrInvalidRecord, len(data))
	}
	h := RecordHeader{
		Version:        data[0],
		Flags:          data[1],
		TableID:        data[2],
		RecordID:       binary.LittleEndian.Uint32(data[3:7]),
		PrevVersionOff: binary.LittleEndian.Uint64(data[7:15]),
		SchemaVersion:  binary.LittleEndian.Uint32(data[15:19]),
		PayloadLen:     binary.LittleEndian.Uint32(data[19:23]),
	}
	return h, nil
}

func (h RecordHeader) IsActive() bool {
	return h.Flags&embedcore.FlagsActive != 0
}

func (h RecordHeader) HasPrevVersion() bool {
	return h.Flags&embedcore.FlagsHasPrevVersion != 0
}

func encodeFieldPayload(record interface{}, layout *embedcore.StructLayout) ([]byte, error) {
	var buf []byte
	for _, field := range layout.Fields {
		if field.IsStruct && !field.IsTime && !field.IsSlice {
			continue
		}
		if len(field.Parent) > 0 && field.IsStruct && !field.IsTime {
			continue
		}

		var valBuf []byte
		var err error

		switch field.Type {
		case reflect.Int:
			valBuf = embedcore.EncodeVarint(nil, int64(embedcore.GetIntField(record, field)))
		case reflect.Int8:
			valBuf = embedcore.EncodeVarint(nil, int64(embedcore.GetInt8Field(record, field)))
		case reflect.Int16:
			valBuf = embedcore.EncodeVarint(nil, int64(embedcore.GetInt16Field(record, field)))
		case reflect.Int32:
			valBuf = embedcore.EncodeVarint(nil, int64(embedcore.GetInt32Field(record, field)))
		case reflect.Int64:
			valBuf = embedcore.EncodeVarint(nil, embedcore.GetInt64Field(record, field))
		case reflect.Uint:
			valBuf = embedcore.EncodeUvarint(nil, uint64(embedcore.GetUintField(record, field)))
		case reflect.Uint8:
			valBuf = embedcore.EncodeUvarint(nil, uint64(embedcore.GetUint8Field(record, field)))
		case reflect.Uint16:
			valBuf = embedcore.EncodeUvarint(nil, uint64(embedcore.GetUint16Field(record, field)))
		case reflect.Uint32:
			valBuf = embedcore.EncodeUvarint(nil, uint64(embedcore.GetUint32Field(record, field)))
		case reflect.Uint64:
			valBuf = embedcore.EncodeUvarint(nil, embedcore.GetUint64Field(record, field))
		case reflect.String:
			valBuf = embedcore.EncodeString(nil, embedcore.GetStringField(record, field))
		case reflect.Bool:
			valBuf = embedcore.EncodeBool(nil, embedcore.GetBoolField(record, field))
		case reflect.Float64:
			valBuf = embedcore.EncodeFloat64(nil, embedcore.GetFloat64Field(record, field))
		case reflect.Float32:
			valBuf = embedcore.EncodeFloat64(nil, float64(embedcore.GetFloat32Field(record, field)))
		case reflect.Struct:
			if field.IsTime {
				valBuf = embedcore.EncodeVarint(nil, embedcore.GetTimeField(record, field).UnixNano())
			} else {
				continue
			}
		case reflect.Slice:
			if field.IsBytes {
				bytesVal, _ := embedcore.GetBytesField(record, field)
				valBuf = embedcore.EncodeBytes(nil, bytesVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.String {
				sliceVal := embedcore.GetStringSlice(record, field)
				valBuf = embedcore.EncodeSlice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int {
				sliceVal := embedcore.GetIntSlice(record, field)
				valBuf = embedcore.EncodeIntSlice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Struct {
				valBuf, err = encodeSliceOfStructs(record, field)
				if err != nil {
					continue
				}
			} else {
				continue
			}
		default:
			continue
		}

		if err != nil {
			continue
		}
		buf = embedcore.EncodeTLVField(buf, field.Name, valBuf)
	}
	return buf, nil
}

func decodeFieldPayload(data []byte, record interface{}, layout *embedcore.StructLayout) error {
	fieldMap := make(map[string]embedcore.FieldOffset)
	for _, f := range layout.Fields {
		fieldMap[f.Name] = f
	}

	for len(data) > 0 {
		name, value, remaining, err := embedcore.DecodeTLVField(data)
		if err != nil {
			break
		}
		data = remaining

		field, ok := fieldMap[name]
		if !ok {
			continue
		}

		var val interface{}
		var decodeErr error

		switch field.Type {
		case reflect.Int:
			var v int64
			v, _, decodeErr = embedcore.DecodeVarint(value)
			if decodeErr == nil {
				val = int(v)
			}
		case reflect.Int8:
			var v int64
			v, _, decodeErr = embedcore.DecodeVarint(value)
			if decodeErr == nil {
				val = int8(v)
			}
		case reflect.Int16:
			var v int64
			v, _, decodeErr = embedcore.DecodeVarint(value)
			if decodeErr == nil {
				val = int16(v)
			}
		case reflect.Int32:
			var v int64
			v, _, decodeErr = embedcore.DecodeVarint(value)
			if decodeErr == nil {
				val = int32(v)
			}
		case reflect.Int64:
			val, _, decodeErr = embedcore.DecodeVarint(value)
		case reflect.Uint:
			var v uint64
			v, _, decodeErr = embedcore.DecodeUvarint(value)
			if decodeErr == nil {
				val = uint(v)
			}
		case reflect.Uint8:
			var v uint64
			v, _, decodeErr = embedcore.DecodeUvarint(value)
			if decodeErr == nil {
				val = uint8(v)
			}
		case reflect.Uint16:
			var v uint64
			v, _, decodeErr = embedcore.DecodeUvarint(value)
			if decodeErr == nil {
				val = uint16(v)
			}
		case reflect.Uint32:
			var v uint64
			v, _, decodeErr = embedcore.DecodeUvarint(value)
			if decodeErr == nil {
				val = uint32(v)
			}
		case reflect.Uint64:
			val, _, decodeErr = embedcore.DecodeUvarint(value)
		case reflect.String:
			val, _, decodeErr = embedcore.DecodeString(value)
		case reflect.Bool:
			val, _, decodeErr = embedcore.DecodeBool(value)
		case reflect.Float64:
			val, _, decodeErr = embedcore.DecodeFloat64(value)
		case reflect.Float32:
			var v float64
			v, _, decodeErr = embedcore.DecodeFloat64(value)
			if decodeErr == nil {
				val = float32(v)
			}
		case reflect.Struct:
			if field.IsTime {
				var nanoVal int64
				nanoVal, _, decodeErr = embedcore.DecodeVarint(value)
				if decodeErr == nil {
					val = time.Unix(0, nanoVal).UTC()
				}
			}
		case reflect.Slice:
			if field.IsBytes {
				val, _, decodeErr = embedcore.DecodeBytes(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.String {
				val, _, decodeErr = embedcore.DecodeSlice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int {
				val, _, decodeErr = embedcore.DecodeIntSlice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Struct {
				sliceVal, sliceErr := decodeSliceOfStructs(value, field)
				if sliceErr != nil {
					decodeErr = sliceErr
				} else {
					val = sliceVal
				}
			}
		}

		if decodeErr != nil || val == nil {
			continue
		}

		embedcore.SetFieldValue(record, field, val)
	}
	return nil
}

func encodeSliceOfStructs(record interface{}, field embedcore.FieldOffset) ([]byte, error) {
	rootVal := reflect.ValueOf(record).Elem()

	var val reflect.Value
	if len(field.Parent) > 0 {
		val = rootVal
		for _, part := range field.Parent {
			val = val.FieldByName(part)
			if !val.IsValid() {
				return nil, nil
			}
		}
		fieldParts := strings.Split(field.Name, ".")
		lastPart := fieldParts[len(fieldParts)-1]
		val = val.FieldByName(lastPart)
	} else {
		val = rootVal.FieldByName(field.Name)
	}

	if !val.IsValid() || val.IsNil() {
		return nil, nil
	}

	numElems := val.Len()
	elemBuf := embedcore.EncodeUvarint(nil, uint64(numElems))

	if numElems == 0 {
		return elemBuf, nil
	}

	elementType := field.SliceElem
	elemLayout, err := embedcore.ComputeStructLayout(reflect.New(elementType).Interface())
	if err != nil {
		return nil, err
	}

	for i := 0; i < numElems; i++ {
		elem := val.Index(i)
		elemPtr := elem.Addr().Interface()

		var elemData []byte
		for _, f := range elemLayout.Fields {
			if f.IsStruct && !f.IsTime && !f.IsSlice {
				continue
			}

			var fieldVal []byte
			switch f.Type {
			case reflect.Int:
				fieldVal = embedcore.EncodeVarint(nil, int64(embedcore.GetIntField(elemPtr, f)))
			case reflect.Int8:
				fieldVal = embedcore.EncodeVarint(nil, int64(embedcore.GetInt8Field(elemPtr, f)))
			case reflect.Int16:
				fieldVal = embedcore.EncodeVarint(nil, int64(embedcore.GetInt16Field(elemPtr, f)))
			case reflect.Int32:
				fieldVal = embedcore.EncodeVarint(nil, int64(embedcore.GetInt32Field(elemPtr, f)))
			case reflect.Int64:
				fieldVal = embedcore.EncodeVarint(nil, embedcore.GetInt64Field(elemPtr, f))
			case reflect.Uint:
				fieldVal = embedcore.EncodeUvarint(nil, uint64(embedcore.GetUintField(elemPtr, f)))
			case reflect.Uint8:
				fieldVal = embedcore.EncodeUvarint(nil, uint64(embedcore.GetUint8Field(elemPtr, f)))
			case reflect.Uint16:
				fieldVal = embedcore.EncodeUvarint(nil, uint64(embedcore.GetUint16Field(elemPtr, f)))
			case reflect.Uint32:
				fieldVal = embedcore.EncodeUvarint(nil, uint64(embedcore.GetUint32Field(elemPtr, f)))
			case reflect.Uint64:
				fieldVal = embedcore.EncodeUvarint(nil, embedcore.GetUint64Field(elemPtr, f))
			case reflect.String:
				fieldVal = embedcore.EncodeString(nil, embedcore.GetStringField(elemPtr, f))
			case reflect.Bool:
				fieldVal = embedcore.EncodeBool(nil, embedcore.GetBoolField(elemPtr, f))
			case reflect.Float64:
				fieldVal = embedcore.EncodeFloat64(nil, embedcore.GetFloat64Field(elemPtr, f))
			case reflect.Float32:
				fieldVal = embedcore.EncodeFloat64(nil, float64(embedcore.GetFloat32Field(elemPtr, f)))
			case reflect.Struct:
				if f.IsTime {
					fieldVal = embedcore.EncodeVarint(nil, embedcore.GetTimeField(elemPtr, f).UnixNano())
				} else {
					continue
				}
			default:
				continue
			}
			elemData = embedcore.EncodeTLVField(elemData, f.Name, fieldVal)
		}
		elemBuf = append(elemBuf, embedcore.EncodeUvarint(nil, uint64(len(elemData)))...)
		elemBuf = append(elemBuf, elemData...)
	}

	return elemBuf, nil
}

func decodeSliceOfStructs(data []byte, field embedcore.FieldOffset) (interface{}, error) {
	length, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, errors.New("invalid slice length")
	}
	data = data[n:]

	elementType := field.SliceElem
	elemLayout, err := embedcore.ComputeStructLayout(reflect.New(elementType).Interface())
	if err != nil {
		return nil, err
	}

	result := reflect.MakeSlice(reflect.SliceOf(elementType), 0, int(length))

	for i := 0; i < int(length) && len(data) > 0; i++ {
		elemLen, ln := binary.Uvarint(data)
		if ln <= 0 {
			break
		}
		data = data[ln:]
		if int(elemLen) > len(data) {
			break
		}
		elemData := data[:elemLen]
		data = data[elemLen:]

		elem := reflect.New(elementType).Interface()
		fieldMap := make(map[string]embedcore.FieldOffset)
		for _, f := range elemLayout.Fields {
			fieldMap[f.Name] = f
		}

		for len(elemData) > 0 {
			name, value, remaining, err := embedcore.DecodeTLVField(elemData)
			if err != nil {
				break
			}
			elemData = remaining

			f, ok := fieldMap[name]
			if !ok {
				continue
			}

			var val interface{}
			var decodeErr error

			switch f.Type {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				var v int64
				v, _, decodeErr = embedcore.DecodeVarint(value)
				if decodeErr == nil {
					switch f.Type {
					case reflect.Int:
						val = int(v)
					case reflect.Int8:
						val = int8(v)
					case reflect.Int16:
						val = int16(v)
					case reflect.Int32:
						val = int32(v)
					case reflect.Int64:
						val = v
					}
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				var v uint64
				v, _, decodeErr = embedcore.DecodeUvarint(value)
				if decodeErr == nil {
					switch f.Type {
					case reflect.Uint:
						val = uint(v)
					case reflect.Uint8:
						val = uint8(v)
					case reflect.Uint16:
						val = uint16(v)
					case reflect.Uint32:
						val = uint32(v)
					case reflect.Uint64:
						val = v
					}
				}
			case reflect.String:
				val, _, decodeErr = embedcore.DecodeString(value)
			case reflect.Bool:
				val, _, decodeErr = embedcore.DecodeBool(value)
			case reflect.Float64:
				val, _, decodeErr = embedcore.DecodeFloat64(value)
			case reflect.Float32:
				var v float64
				v, _, decodeErr = embedcore.DecodeFloat64(value)
				if decodeErr == nil {
					val = float32(v)
				}
			case reflect.Struct:
				if f.IsTime {
					var nanoVal int64
					nanoVal, _, decodeErr = embedcore.DecodeVarint(value)
					if decodeErr == nil {
						val = time.Unix(0, nanoVal).UTC()
					}
				}
			}

			if decodeErr != nil || val == nil {
				continue
			}

			embedcore.SetFieldValue(elem, f, val)
		}

		result = reflect.Append(result, reflect.ValueOf(elem).Elem())
	}

	return result.Interface(), nil
}

func buildV2Record(tableID uint8, recordID uint32, schemaVersion uint32, flags byte, prevVersionOff uint64, payload []byte) []byte {
	headerSize := embedcore.RecordHeaderSize
	footerSize := embedcore.RecordFooterSize
	totalSize := headerSize + len(payload) + footerSize

	rec := make([]byte, totalSize)
	rec[0] = V2RecordVersion
	rec[1] = flags
	rec[2] = byte(tableID)
	binary.LittleEndian.PutUint32(rec[3:7], recordID)
	binary.LittleEndian.PutUint64(rec[7:15], prevVersionOff)
	binary.LittleEndian.PutUint32(rec[15:19], schemaVersion)
	binary.LittleEndian.PutUint32(rec[19:23], uint32(len(payload)))
	copy(rec[headerSize:], payload)

	checksum := crc32.ChecksumIEEE(rec[:headerSize+len(payload)])
	binary.LittleEndian.PutUint32(rec[headerSize+len(payload):], checksum)

	return rec
}

func parseV2Record(data []byte) (RecordHeader, []byte, error) {
	hdr, err := decodeRecordHeader(data)
	if err != nil {
		return hdr, nil, err
	}

	totalLen := embedcore.RecordHeaderSize + int(hdr.PayloadLen) + embedcore.RecordFooterSize
	if len(data) < totalLen {
		return hdr, nil, fmt.Errorf("%w: record truncated (have %d need %d)", ErrInvalidRecord, len(data), totalLen)
	}

	payload := data[embedcore.RecordHeaderSize : embedcore.RecordHeaderSize+hdr.PayloadLen]
	storedCRC := binary.LittleEndian.Uint32(data[embedcore.RecordHeaderSize+hdr.PayloadLen:])
	computedCRC := crc32.ChecksumIEEE(data[:embedcore.RecordHeaderSize+hdr.PayloadLen])
	if storedCRC != computedCRC {
		return hdr, nil, fmt.Errorf("%w: CRC mismatch (stored=%08x computed=%08x)", ErrInvalidRecord, storedCRC, computedCRC)
	}

	return hdr, payload, nil
}

func recordTotalSize(hdr RecordHeader) int {
	return embedcore.RecordHeaderSize + int(hdr.PayloadLen) + embedcore.RecordFooterSize
}

func isV2Record(data []byte) bool {
	if len(data) < 1 {
		return false
	}
	return data[0] == V2RecordVersion
}
