package embeddb

import (
	"encoding/binary"
	"math"
	"time"

	embedcore "github.com/yay101/embeddbcore"
)

const (
	indexNSPrimary   byte = 0x00
	indexNSSecondary byte = 0x01
	indexNSVersion   byte = 0x02
)

func encodePrimaryKey(tableID uint8, pkValue any) []byte {
	var key []byte
	key = append(key, indexNSPrimary)
	key = append(key, tableID)
	key = encodeIndexValue(key, pkValue)
	return key
}

const maxIndexValueLen = 252

func encodeIndexValueBytes(value any) []byte {
	return encodeIndexValue(nil, value)
}

func truncIndexValue(v []byte) []byte {
	if len(v) > maxIndexValueLen {
		return v[:maxIndexValueLen]
	}
	return v
}

func encodeSecondaryKey(tableID uint8, fieldName string, fieldValue any, recordOffset uint64) []byte {
	var key []byte
	key = append(key, indexNSSecondary)
	key = append(key, tableID)
	key = embedcore.EncodeUvarint(key, uint64(len(fieldName)))
	key = append(key, fieldName...)
	key = truncIndexValue(encodeIndexValueBytes(fieldValue))
	key = binary.BigEndian.AppendUint64(key, recordOffset)
	return key
}

func encodeSecondaryKeyPrefix(tableID uint8, fieldName string) []byte {
	var prefix []byte
	prefix = append(prefix, indexNSSecondary)
	prefix = append(prefix, tableID)
	prefix = embedcore.EncodeUvarint(prefix, uint64(len(fieldName)))
	prefix = append(prefix, fieldName...)
	return prefix
}

func encodeSecondaryKeyPrefixWithValue(tableID uint8, fieldName string, fieldValue any) []byte {
	var key []byte
	key = append(key, indexNSSecondary)
	key = append(key, tableID)
	key = embedcore.EncodeUvarint(key, uint64(len(fieldName)))
	key = append(key, fieldName...)
	key = truncIndexValue(encodeIndexValueBytes(fieldValue))
	return key
}

func encodeVersionKeyPrefix(tableID uint8, recordID uint32) []byte {
	var key []byte
	key = append(key, indexNSVersion)
	key = append(key, tableID)
	key = embedcore.EncodeUvarint(key, uint64(recordID))
	return key
}

func encodeVersionKey(tableID uint8, recordID uint32, version uint32) []byte {
	var key []byte
	key = append(key, indexNSVersion)
	key = append(key, tableID)
	key = embedcore.EncodeUvarint(key, uint64(recordID))
	key = embedcore.EncodeUvarint(key, uint64(version))
	return key
}

func encodeVersionValue(recordOffset uint64, createdAt int64) []byte {
	var val []byte
	val = binary.BigEndian.AppendUint64(val, recordOffset)
	val = binary.BigEndian.AppendUint64(val, uint64(createdAt))
	return val
}

func decodeVersionValue(val []byte) (recordOffset uint64, createdAt int64) {
	if len(val) < 16 {
		return 0, 0
	}
	recordOffset = binary.BigEndian.Uint64(val[0:8])
	createdAt = int64(binary.BigEndian.Uint64(val[8:16]))
	return
}

func parsePrimaryKey(key []byte) (tableID uint8, pkValue []byte) {
	if len(key) < 2 || key[0] != indexNSPrimary {
		return 0, nil
	}
	return key[1], key[2:]
}

func parseSecondaryKey(key []byte) (tableID uint8, fieldName string, fieldValue []byte, recordOffset uint64, ok bool) {
	if len(key) < 3 || key[0] != indexNSSecondary {
		return 0, "", nil, 0, false
	}
	tableID = key[1]
	nameLen, n := binary.Uvarint(key[2:])
	if n <= 0 {
		return 0, "", nil, 0, false
	}
	off := 2 + n
	if int(off)+int(nameLen) > len(key) {
		return 0, "", nil, 0, false
	}
	fieldName = string(key[off : off+int(nameLen)])
	off += int(nameLen)

	valueEnd := len(key) - 8
	if valueEnd < off {
		return 0, "", nil, 0, false
	}
	fieldValue = key[off:valueEnd]
	recordOffset = binary.BigEndian.Uint64(key[valueEnd:])
	return tableID, fieldName, fieldValue, recordOffset, true
}

func parseVersionKey(key []byte) (tableID uint8, recordID uint32, version uint32, ok bool) {
	if len(key) < 3 || key[0] != indexNSVersion {
		return 0, 0, 0, false
	}
	tableID = key[1]
	rid, n := binary.Uvarint(key[2:])
	if n <= 0 {
		return 0, 0, 0, false
	}
	ver, m := binary.Uvarint(key[2+n:])
	if m <= 0 {
		return 0, 0, 0, false
	}
	return tableID, uint32(rid), uint32(ver), true
}

func encodeIndexValue(buf []byte, value any) []byte {
	switch v := value.(type) {
	case int:
		return embedcore.EncodeVarint(buf, int64(v))
	case int8:
		return embedcore.EncodeVarint(buf, int64(v))
	case int16:
		return embedcore.EncodeVarint(buf, int64(v))
	case int32:
		return embedcore.EncodeVarint(buf, int64(v))
	case int64:
		return embedcore.EncodeVarint(buf, v)
	case uint:
		return embedcore.EncodeUvarint(buf, uint64(v))
	case uint8:
		return embedcore.EncodeUvarint(buf, uint64(v))
	case uint16:
		return embedcore.EncodeUvarint(buf, uint64(v))
	case uint32:
		return embedcore.EncodeUvarint(buf, uint64(v))
	case uint64:
		return embedcore.EncodeUvarint(buf, v)
	case float32:
		return embedcore.EncodeUvarint(buf, uint64(math.Float64bits(float64(v))))
	case float64:
		return embedcore.EncodeUvarint(buf, uint64(math.Float64bits(v)))
	case string:
		return embedcore.EncodeString(buf, v)
	case bool:
		if v {
			return embedcore.EncodeUvarint(buf, 1)
		}
		return embedcore.EncodeUvarint(buf, 0)
	case time.Time:
		return embedcore.EncodeVarint(buf, v.UnixNano())
	}
	return embedcore.EncodeUvarint(buf, 0)
}
