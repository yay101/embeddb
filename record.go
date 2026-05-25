package embeddb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"
	"time"
	"unsafe"

	"github.com/yay101/embeddbcore"
)

const (
	V2RecordVersion byte = 0x01
)

var ErrInvalidRecord = errors.New("invalid record format")

// RecordHeader contains the metadata for a V2 record stored in the database file.
// The header is followed by the TLV-encoded payload and a CRC32 footer.
type RecordHeader struct {
	Version        byte   // record format version
	Flags          byte   // active flag, compression flag, previous version flag
	TableID        uint8  // table this record belongs to
	RecordID       uint32 // unique record identifier within the table
	PrevVersionOff uint64 // file offset of the previous version (0 if none)
	SchemaVersion  uint32 // schema version at time of write
	PayloadLen     uint32 // length of the TLV-encoded payload in bytes
}

func decodeRecordHeader(data []byte) (RecordHeader, error) {
	if len(data) < embeddbcore.RecordHeaderSize {
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

// IsActive returns true if the record has not been deleted.
func (h RecordHeader) IsActive() bool {
	return h.Flags&embeddbcore.FlagsActive != 0
}

// HasPrevVersion returns true if this record has a previous version stored.
func (h RecordHeader) HasPrevVersion() bool {
	return h.Flags&embeddbcore.FlagsHasPrevVersion != 0
}

func encodeFieldPayload(record interface{}, layout *embeddbcore.StructLayout, cipher *fieldCipher) ([]byte, error) {
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
			valBuf = embeddbcore.EncodeVarint(nil, int64(embeddbcore.GetIntField(record, field)))
		case reflect.Int8:
			valBuf = embeddbcore.EncodeVarint(nil, int64(embeddbcore.GetInt8Field(record, field)))
		case reflect.Int16:
			valBuf = embeddbcore.EncodeVarint(nil, int64(embeddbcore.GetInt16Field(record, field)))
		case reflect.Int32:
			valBuf = embeddbcore.EncodeVarint(nil, int64(embeddbcore.GetInt32Field(record, field)))
		case reflect.Int64:
			valBuf = embeddbcore.EncodeVarint(nil, embeddbcore.GetInt64Field(record, field))
		case reflect.Uint:
			valBuf = embeddbcore.EncodeUvarint(nil, uint64(embeddbcore.GetUintField(record, field)))
		case reflect.Uint8:
			valBuf = embeddbcore.EncodeUvarint(nil, uint64(embeddbcore.GetUint8Field(record, field)))
		case reflect.Uint16:
			valBuf = embeddbcore.EncodeUvarint(nil, uint64(embeddbcore.GetUint16Field(record, field)))
		case reflect.Uint32:
			valBuf = embeddbcore.EncodeUvarint(nil, uint64(embeddbcore.GetUint32Field(record, field)))
		case reflect.Uint64:
			valBuf = embeddbcore.EncodeUvarint(nil, embeddbcore.GetUint64Field(record, field))
		case reflect.String:
			valBuf = embeddbcore.EncodeString(nil, embeddbcore.GetStringField(record, field))
		case reflect.Bool:
			valBuf = embeddbcore.EncodeBool(nil, embeddbcore.GetBoolField(record, field))
		case reflect.Float64:
			valBuf = embeddbcore.EncodeFloat64(nil, embeddbcore.GetFloat64Field(record, field))
		case reflect.Float32:
			valBuf = embeddbcore.EncodeFloat32(nil, embeddbcore.GetFloat32Field(record, field))
		case reflect.Struct:
			if field.IsTime {
				valBuf = embeddbcore.EncodeVarint(nil, embeddbcore.GetTimeField(record, field).UnixNano())
			} else {
				continue
			}
		case reflect.Slice:
			if field.IsBytes {
				bytesVal, _ := embeddbcore.GetBytesField(record, field)
				valBuf = embeddbcore.EncodeBytes(nil, bytesVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.String {
				sliceVal := embeddbcore.GetStringSlice(record, field)
				valBuf = embeddbcore.EncodeSlice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int {
				sliceVal := embeddbcore.GetIntSlice(record, field)
				valBuf = embeddbcore.EncodeIntSlice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int8 {
				sliceVal := embeddbcore.GetInt8Slice(record, field)
				valBuf = embeddbcore.EncodeInt8Slice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int16 {
				sliceVal := embeddbcore.GetInt16Slice(record, field)
				valBuf = embeddbcore.EncodeInt16Slice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int32 {
				sliceVal := embeddbcore.GetInt32Slice(record, field)
				valBuf = embeddbcore.EncodeInt32Slice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int64 {
				sliceVal := embeddbcore.GetInt64Slice(record, field)
				valBuf = embeddbcore.EncodeInt64Slice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Uint {
				sliceVal := embeddbcore.GetUintSlice(record, field)
				valBuf = embeddbcore.EncodeUintSlice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Uint16 {
				sliceVal := embeddbcore.GetUint16Slice(record, field)
				valBuf = embeddbcore.EncodeUint16Slice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Uint32 {
				sliceVal := embeddbcore.GetUint32Slice(record, field)
				valBuf = embeddbcore.EncodeUint32Slice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Uint64 {
				sliceVal := embeddbcore.GetUint64Slice(record, field)
				valBuf = embeddbcore.EncodeUint64Slice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Float32 {
				sliceVal := embeddbcore.GetFloat32Slice(record, field)
				valBuf = embeddbcore.EncodeFloat32Slice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Float64 {
				sliceVal := embeddbcore.GetFloat64Slice(record, field)
				valBuf = embeddbcore.EncodeFloat64Slice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Bool {
				sliceVal := embeddbcore.GetBoolSlice(record, field)
				valBuf = embeddbcore.EncodeBoolSlice(nil, sliceVal)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Struct {
				valBuf, err = encodeSliceOfStructs(record, field, cipher)
				if err != nil {
					continue
				}
			} else {
				continue
			}
		case reflect.Map:
			valBuf = encodeMapField(record, field, cipher)
		default:
			continue
		}

		if err != nil {
			continue
		}

		if field.Encrypted && cipher != nil {
			valBuf, err = cipher.encrypt(nil, valBuf)
			if err != nil {
				continue
			}
		}

		buf = embeddbcore.EncodeTLVField(buf, field.Name, valBuf)
	}
	return buf, nil
}

func decodeFieldPayload(data []byte, record interface{}, layout *embeddbcore.StructLayout, cipher *fieldCipher) error {
	fieldMap := make(map[string]embeddbcore.FieldOffset)
	for _, f := range layout.Fields {
		fieldMap[f.Name] = f
	}

	for len(data) > 0 {
		name, value, remaining, err := embeddbcore.DecodeTLVField(data)
		if err != nil {
			break
		}
		data = remaining

		field, ok := fieldMap[name]
		if !ok {
			continue
		}

		if field.Encrypted && cipher != nil {
			decrypted, decryptErr := cipher.decrypt(value)
			if decryptErr != nil {
				continue
			}
			value = decrypted
		}

		var val interface{}
		var decodeErr error

		switch field.Type {
		case reflect.Int:
			var v int64
			v, _, decodeErr = embeddbcore.DecodeVarint(value)
			if decodeErr == nil {
				val = int(v)
			}
		case reflect.Int8:
			var v int64
			v, _, decodeErr = embeddbcore.DecodeVarint(value)
			if decodeErr == nil {
				val = int8(v)
			}
		case reflect.Int16:
			var v int64
			v, _, decodeErr = embeddbcore.DecodeVarint(value)
			if decodeErr == nil {
				val = int16(v)
			}
		case reflect.Int32:
			var v int64
			v, _, decodeErr = embeddbcore.DecodeVarint(value)
			if decodeErr == nil {
				val = int32(v)
			}
		case reflect.Int64:
			val, _, decodeErr = embeddbcore.DecodeVarint(value)
		case reflect.Uint:
			var v uint64
			v, _, decodeErr = embeddbcore.DecodeUvarint(value)
			if decodeErr == nil {
				val = uint(v)
			}
		case reflect.Uint8:
			var v uint64
			v, _, decodeErr = embeddbcore.DecodeUvarint(value)
			if decodeErr == nil {
				val = uint8(v)
			}
		case reflect.Uint16:
			var v uint64
			v, _, decodeErr = embeddbcore.DecodeUvarint(value)
			if decodeErr == nil {
				val = uint16(v)
			}
		case reflect.Uint32:
			var v uint64
			v, _, decodeErr = embeddbcore.DecodeUvarint(value)
			if decodeErr == nil {
				val = uint32(v)
			}
		case reflect.Uint64:
			val, _, decodeErr = embeddbcore.DecodeUvarint(value)
		case reflect.String:
			val, _, decodeErr = embeddbcore.DecodeString(value)
		case reflect.Bool:
			val, _, decodeErr = embeddbcore.DecodeBool(value)
		case reflect.Float64:
			val, _, decodeErr = embeddbcore.DecodeFloat64(value)
		case reflect.Float32:
			if len(value) == 4 {
				var v float32
				v, _, decodeErr = embeddbcore.DecodeFloat32(value)
				if decodeErr == nil {
					val = v
				}
			} else {
				var v float64
				v, _, decodeErr = embeddbcore.DecodeFloat64(value)
				if decodeErr == nil {
					val = float32(v)
				}
			}
		case reflect.Struct:
			if field.IsTime {
				var nanoVal int64
				nanoVal, _, decodeErr = embeddbcore.DecodeVarint(value)
				if decodeErr == nil {
					val = time.Unix(0, nanoVal).UTC()
				}
			}
		case reflect.Slice:
			if field.IsBytes {
				val, _, decodeErr = embeddbcore.DecodeBytes(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.String {
				val, _, decodeErr = embeddbcore.DecodeSlice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int {
				val, _, decodeErr = embeddbcore.DecodeIntSlice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int8 {
				val, _, decodeErr = embeddbcore.DecodeInt8Slice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int16 {
				val, _, decodeErr = embeddbcore.DecodeInt16Slice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int32 {
				val, _, decodeErr = embeddbcore.DecodeInt32Slice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Int64 {
				val, _, decodeErr = embeddbcore.DecodeInt64Slice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Uint {
				val, _, decodeErr = embeddbcore.DecodeUintSlice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Uint16 {
				val, _, decodeErr = embeddbcore.DecodeUint16Slice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Uint32 {
				val, _, decodeErr = embeddbcore.DecodeUint32Slice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Uint64 {
				val, _, decodeErr = embeddbcore.DecodeUint64Slice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Float32 {
				val, _, decodeErr = embeddbcore.DecodeFloat32Slice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Float64 {
				val, _, decodeErr = embeddbcore.DecodeFloat64Slice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Bool {
				val, _, decodeErr = embeddbcore.DecodeBoolSlice(value)
			} else if field.IsSlice && field.SliceElem.Kind() == reflect.Struct {
				sliceVal, sliceErr := decodeSliceOfStructs(value, field, cipher)
				if sliceErr != nil {
					decodeErr = sliceErr
				} else {
					val = sliceVal
				}
			}
		case reflect.Map:
			mapVal, _, mapErr := decodeMapField(value, field, cipher)
			if mapErr != nil {
				decodeErr = mapErr
			} else {
				val = mapVal
			}
		}

		if decodeErr != nil || val == nil {
			continue
		}

		embeddbcore.SetFieldValue(record, field, val)
	}
	return nil
}

func encodeSliceOfStructs(record interface{}, field embeddbcore.FieldOffset, cipher *fieldCipher) ([]byte, error) {
	ptr := unsafe.Pointer(reflect.ValueOf(record).Pointer())
	sliceHeader := (*reflect.SliceHeader)(unsafe.Add(ptr, field.Offset))

	numElems := sliceHeader.Len
	elemBuf := embeddbcore.EncodeUvarint(nil, uint64(numElems))
	if numElems == 0 {
		return elemBuf, nil
	}

	elementType := field.SliceElem
	elemLayout, err := embeddbcore.ComputeStructLayout(reflect.New(elementType).Interface())
	if err != nil {
		return nil, err
	}

	elemSize := elementType.Size()
	for i := 0; i < numElems; i++ {
		elemPtr := unsafe.Pointer(sliceHeader.Data + uintptr(i)*uintptr(elemSize))
		elemAddr := reflect.NewAt(elementType, elemPtr).Interface()

		var elemData []byte
		for _, f := range elemLayout.Fields {
			if f.IsStruct && !f.IsTime && !f.IsSlice {
				continue
			}

			var fieldVal []byte
			switch f.Type {
			case reflect.Int:
				fieldVal = embeddbcore.EncodeVarint(nil, int64(embeddbcore.GetIntField(elemAddr, f)))
			case reflect.Int8:
				fieldVal = embeddbcore.EncodeVarint(nil, int64(embeddbcore.GetInt8Field(elemAddr, f)))
			case reflect.Int16:
				fieldVal = embeddbcore.EncodeVarint(nil, int64(embeddbcore.GetInt16Field(elemAddr, f)))
			case reflect.Int32:
				fieldVal = embeddbcore.EncodeVarint(nil, int64(embeddbcore.GetInt32Field(elemAddr, f)))
			case reflect.Int64:
				fieldVal = embeddbcore.EncodeVarint(nil, embeddbcore.GetInt64Field(elemAddr, f))
			case reflect.Uint:
				fieldVal = embeddbcore.EncodeUvarint(nil, uint64(embeddbcore.GetUintField(elemAddr, f)))
			case reflect.Uint8:
				fieldVal = embeddbcore.EncodeUvarint(nil, uint64(embeddbcore.GetUint8Field(elemAddr, f)))
			case reflect.Uint16:
				fieldVal = embeddbcore.EncodeUvarint(nil, uint64(embeddbcore.GetUint16Field(elemAddr, f)))
			case reflect.Uint32:
				fieldVal = embeddbcore.EncodeUvarint(nil, uint64(embeddbcore.GetUint32Field(elemAddr, f)))
			case reflect.Uint64:
				fieldVal = embeddbcore.EncodeUvarint(nil, embeddbcore.GetUint64Field(elemAddr, f))
			case reflect.String:
				fieldVal = embeddbcore.EncodeString(nil, embeddbcore.GetStringField(elemAddr, f))
			case reflect.Bool:
				fieldVal = embeddbcore.EncodeBool(nil, embeddbcore.GetBoolField(elemAddr, f))
			case reflect.Float64:
				fieldVal = embeddbcore.EncodeFloat64(nil, embeddbcore.GetFloat64Field(elemAddr, f))
			case reflect.Float32:
				fieldVal = embeddbcore.EncodeFloat32(nil, embeddbcore.GetFloat32Field(elemAddr, f))
			case reflect.Slice:
				if f.IsBytes {
					bytesVal, _ := embeddbcore.GetBytesField(elemAddr, f)
					fieldVal = embeddbcore.EncodeBytes(nil, bytesVal)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.String {
					fieldVal = embeddbcore.EncodeSlice(nil, embeddbcore.GetStringSlice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Int {
					fieldVal = embeddbcore.EncodeIntSlice(nil, embeddbcore.GetIntSlice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Int8 {
					fieldVal = embeddbcore.EncodeInt8Slice(nil, embeddbcore.GetInt8Slice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Int16 {
					fieldVal = embeddbcore.EncodeInt16Slice(nil, embeddbcore.GetInt16Slice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Int32 {
					fieldVal = embeddbcore.EncodeInt32Slice(nil, embeddbcore.GetInt32Slice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Int64 {
					fieldVal = embeddbcore.EncodeInt64Slice(nil, embeddbcore.GetInt64Slice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Uint {
					fieldVal = embeddbcore.EncodeUintSlice(nil, embeddbcore.GetUintSlice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Uint16 {
					fieldVal = embeddbcore.EncodeUint16Slice(nil, embeddbcore.GetUint16Slice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Uint32 {
					fieldVal = embeddbcore.EncodeUint32Slice(nil, embeddbcore.GetUint32Slice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Uint64 {
					fieldVal = embeddbcore.EncodeUint64Slice(nil, embeddbcore.GetUint64Slice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Float32 {
					fieldVal = embeddbcore.EncodeFloat32Slice(nil, embeddbcore.GetFloat32Slice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Float64 {
					fieldVal = embeddbcore.EncodeFloat64Slice(nil, embeddbcore.GetFloat64Slice(elemAddr, f))
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Bool {
					fieldVal = embeddbcore.EncodeBoolSlice(nil, embeddbcore.GetBoolSlice(elemAddr, f))
				} else {
					continue
				}
			case reflect.Struct:
				if f.IsTime {
					fieldVal = embeddbcore.EncodeVarint(nil, embeddbcore.GetTimeField(elemAddr, f).UnixNano())
				} else {
					continue
				}
			case reflect.Map:
				fieldVal = encodeMapField(elemAddr, f, cipher)
			default:
				continue
			}
			if f.Encrypted && cipher != nil {
				var encErr error
				fieldVal, encErr = cipher.encrypt(nil, fieldVal)
				if encErr != nil {
					continue
				}
			}
			elemData = embeddbcore.EncodeTLVField(elemData, f.Name, fieldVal)
		}
		elemBuf = append(elemBuf, embeddbcore.EncodeUvarint(nil, uint64(len(elemData)))...)
		elemBuf = append(elemBuf, elemData...)
	}

	return elemBuf, nil
}

func decodeSliceOfStructs(data []byte, field embeddbcore.FieldOffset, cipher *fieldCipher) (interface{}, error) {
	length, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, errors.New("invalid slice length")
	}
	data = data[n:]

	elementType := field.SliceElem
	elemLayout, err := embeddbcore.ComputeStructLayout(reflect.New(elementType).Interface())
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
		fieldMap := make(map[string]embeddbcore.FieldOffset)
		for _, f := range elemLayout.Fields {
			fieldMap[f.Name] = f
		}

		for len(elemData) > 0 {
			name, value, remaining, err := embeddbcore.DecodeTLVField(elemData)
			if err != nil {
				break
			}
			elemData = remaining

			f, ok := fieldMap[name]
			if !ok {
				continue
			}

			if f.Encrypted && cipher != nil {
				decrypted, decryptErr := cipher.decrypt(value)
				if decryptErr != nil {
					continue
				}
				value = decrypted
			}

			var val interface{}
			var decodeErr error

			switch f.Type {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				var v int64
				v, _, decodeErr = embeddbcore.DecodeVarint(value)
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
				v, _, decodeErr = embeddbcore.DecodeUvarint(value)
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
				val, _, decodeErr = embeddbcore.DecodeString(value)
			case reflect.Bool:
				val, _, decodeErr = embeddbcore.DecodeBool(value)
			case reflect.Float64:
				val, _, decodeErr = embeddbcore.DecodeFloat64(value)
			case reflect.Float32:
				if len(value) == 4 {
					var v float32
					v, _, decodeErr = embeddbcore.DecodeFloat32(value)
					if decodeErr == nil {
						val = v
					}
				} else {
					var v float64
					v, _, decodeErr = embeddbcore.DecodeFloat64(value)
					if decodeErr == nil {
						val = float32(v)
					}
				}
			case reflect.Slice:
				if f.IsBytes {
					val, _, decodeErr = embeddbcore.DecodeBytes(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.String {
					val, _, decodeErr = embeddbcore.DecodeSlice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Int {
					val, _, decodeErr = embeddbcore.DecodeIntSlice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Int8 {
					val, _, decodeErr = embeddbcore.DecodeInt8Slice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Int16 {
					val, _, decodeErr = embeddbcore.DecodeInt16Slice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Int32 {
					val, _, decodeErr = embeddbcore.DecodeInt32Slice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Int64 {
					val, _, decodeErr = embeddbcore.DecodeInt64Slice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Uint {
					val, _, decodeErr = embeddbcore.DecodeUintSlice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Uint16 {
					val, _, decodeErr = embeddbcore.DecodeUint16Slice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Uint32 {
					val, _, decodeErr = embeddbcore.DecodeUint32Slice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Uint64 {
					val, _, decodeErr = embeddbcore.DecodeUint64Slice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Float32 {
					val, _, decodeErr = embeddbcore.DecodeFloat32Slice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Float64 {
					val, _, decodeErr = embeddbcore.DecodeFloat64Slice(value)
				} else if f.IsSlice && f.SliceElem.Kind() == reflect.Bool {
					val, _, decodeErr = embeddbcore.DecodeBoolSlice(value)
				}
			case reflect.Struct:
				if f.IsTime {
					var nanoVal int64
					nanoVal, _, decodeErr = embeddbcore.DecodeVarint(value)
					if decodeErr == nil {
						val = time.Unix(0, nanoVal).UTC()
					}
				}
			}

			if decodeErr != nil || val == nil {
				continue
			}

			embeddbcore.SetFieldValue(elem, f, val)
		}

		result = reflect.Append(result, reflect.ValueOf(elem).Elem())
	}

	return result.Interface(), nil
}

func encodeMapField(record interface{}, field embeddbcore.FieldOffset, cipher *fieldCipher) []byte {
	ptr := unsafe.Pointer(reflect.ValueOf(record).Pointer())
	fieldPtr := unsafe.Add(ptr, field.Offset)

	var buf []byte
	switch field.MapValType.Kind() {
	case reflect.String:
		m := *(*map[string]string)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeString(nil, v)
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Int:
		m := *(*map[string]int)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeVarint(nil, int64(v))
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Int64:
		m := *(*map[string]int64)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeVarint(nil, v)
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Int8:
		m := *(*map[string]int8)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeVarint(nil, int64(v))
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Int16:
		m := *(*map[string]int16)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeVarint(nil, int64(v))
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Int32:
		m := *(*map[string]int32)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeVarint(nil, int64(v))
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Uint:
		m := *(*map[string]uint)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeUvarint(nil, uint64(v))
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Uint8:
		m := *(*map[string]uint8)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeUvarint(nil, uint64(v))
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Uint16:
		m := *(*map[string]uint16)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeUvarint(nil, uint64(v))
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Uint32:
		m := *(*map[string]uint32)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeUvarint(nil, uint64(v))
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Uint64:
		m := *(*map[string]uint64)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeUvarint(nil, v)
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Float32:
		m := *(*map[string]float32)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeFloat32(nil, v)
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Float64:
		m := *(*map[string]float64)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeFloat64(nil, v)
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	case reflect.Bool:
		m := *(*map[string]bool)(fieldPtr)
		buf = embeddbcore.EncodeUvarint(buf, uint64(len(m)))
		for k, v := range m {
			buf = embeddbcore.EncodeString(buf, k)
			elem := embeddbcore.EncodeBool(nil, v)
			buf = embeddbcore.EncodeUvarint(buf, uint64(len(elem)))
			buf = append(buf, elem...)
		}
	}
	return buf
}

func decodeMapField(data []byte, field embeddbcore.FieldOffset, cipher *fieldCipher) (interface{}, int, error) {
	length, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, 0, errors.New("invalid map length")
	}
	data = data[n:]
	totalRead := n

	mapType := field.MapType
	result := reflect.MakeMapWithSize(mapType, int(length))
	keyType := field.MapKeyType
	valType := field.MapValType

	for i := 0; i < int(length) && len(data) > 0; i++ {
		k, trailing, err := embeddbcore.DecodeString(data)
		if err != nil {
			break
		}
		kLenRead := len(data) - len(trailing)
		data = trailing
		totalRead += kLenRead

		valLen, ln := binary.Uvarint(data)
		if ln <= 0 {
			break
		}
		data = data[ln:]
		totalRead += ln

		if int(valLen) > len(data) {
			break
		}
		valBytes := data[:valLen]
		data = data[valLen:]
		totalRead += int(valLen)

		kv := reflect.New(keyType).Elem()
		kv.SetString(k)

		vv := reflect.New(valType).Elem()
		switch valType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v, _, _ := embeddbcore.DecodeVarint(valBytes)
			vv.SetInt(v)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			v, _, _ := embeddbcore.DecodeUvarint(valBytes)
			vv.SetUint(v)
		case reflect.Float32, reflect.Float64:
			if valType.Kind() == reflect.Float32 && len(valBytes) == 4 {
				v, _, _ := embeddbcore.DecodeFloat32(valBytes)
				vv.SetFloat(float64(v))
			} else {
				v, _, _ := embeddbcore.DecodeFloat64(valBytes)
				vv.SetFloat(v)
			}
		case reflect.String:
			s, _, _ := embeddbcore.DecodeString(valBytes)
			vv.SetString(s)
		case reflect.Bool:
			b, _, _ := embeddbcore.DecodeBool(valBytes)
			vv.SetBool(b)
		}

		result.SetMapIndex(kv, vv)
	}

	return result.Interface(), totalRead, nil
}

func buildV2Record(tableID uint8, recordID uint32, schemaVersion uint32, flags byte, prevVersionOff uint64, payload []byte) []byte {
	headerSize := embeddbcore.RecordHeaderSize
	footerSize := embeddbcore.RecordFooterSize
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

	totalLen := embeddbcore.RecordHeaderSize + int(hdr.PayloadLen) + embeddbcore.RecordFooterSize
	if len(data) < totalLen {
		return hdr, nil, fmt.Errorf("%w: record truncated (have %d need %d)", ErrInvalidRecord, len(data), totalLen)
	}

	payload := data[embeddbcore.RecordHeaderSize : embeddbcore.RecordHeaderSize+hdr.PayloadLen]
	storedCRC := binary.LittleEndian.Uint32(data[embeddbcore.RecordHeaderSize+hdr.PayloadLen:])
	computedCRC := crc32.ChecksumIEEE(data[:embeddbcore.RecordHeaderSize+hdr.PayloadLen])
	if storedCRC != computedCRC {
		return hdr, nil, fmt.Errorf("%w: CRC mismatch (stored=%08x computed=%08x)", ErrInvalidRecord, storedCRC, computedCRC)
	}

	return hdr, payload, nil
}

func recordTotalSize(hdr RecordHeader) int {
	return embeddbcore.RecordHeaderSize + int(hdr.PayloadLen) + embeddbcore.RecordFooterSize
}

func isV2Record(data []byte) bool {
	if len(data) < 1 {
		return false
	}
	return data[0] == V2RecordVersion
}
