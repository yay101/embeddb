package embeddb

import (
	"encoding/binary"
	"math"
	"sync"
	"time"

	"github.com/yay101/embeddbcore"
)

const (
	indexNSPrimary   byte = 0x00
	indexNSSecondary byte = 0x01
	indexNSVersion   byte = 0x02
)

var keyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 64)
		return &b
	},
}

func encodePrimaryKey(tableID uint8, pkValue any) []byte {
	bp := keyBufPool.Get().(*[]byte)
	key := (*bp)[:0]
	key = append(key, indexNSPrimary)
	key = append(key, tableID)
	key = encodeIndexValue(key, pkValue)
	out := make([]byte, len(key))
	copy(out, key)
	*bp = key
	keyBufPool.Put(bp)
	return out
}

const maxIndexValueLen = 252

func encodeIndexValueBytes(value any) []byte {
	bp := keyBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	buf = encodeIndexValue(buf, value)
	out := make([]byte, len(buf))
	copy(out, buf)
	*bp = buf
	keyBufPool.Put(bp)
	return out
}

func truncIndexValue(v []byte) []byte {
	if len(v) > maxIndexValueLen {
		return v[:maxIndexValueLen]
	}
	return v
}

func encodeSecondaryKey(tableID uint8, fieldName string, fieldValue any, recordID uint32) []byte {
	bp := keyBufPool.Get().(*[]byte)
	key := (*bp)[:0]
	key = append(key, indexNSSecondary)
	key = append(key, tableID)
	key = embeddbcore.EncodeUvarint(key, uint64(len(fieldName)))
	key = append(key, fieldName...)
	key = append(key, truncIndexValue(encodeIndexValueBytes(fieldValue))...)
	key = binary.BigEndian.AppendUint32(key, recordID)
	out := make([]byte, len(key))
	copy(out, key)
	*bp = key
	keyBufPool.Put(bp)
	return out
}

func encodeSecondaryKeyPrefix(tableID uint8, fieldName string) []byte {
	bp := keyBufPool.Get().(*[]byte)
	prefix := (*bp)[:0]
	prefix = append(prefix, indexNSSecondary)
	prefix = append(prefix, tableID)
	prefix = embeddbcore.EncodeUvarint(prefix, uint64(len(fieldName)))
	prefix = append(prefix, fieldName...)
	out := make([]byte, len(prefix))
	copy(out, prefix)
	*bp = prefix
	keyBufPool.Put(bp)
	return out
}

func encodeSecondaryKeyPrefixWithValue(tableID uint8, fieldName string, fieldValue any) []byte {
	bp := keyBufPool.Get().(*[]byte)
	key := (*bp)[:0]
	key = append(key, indexNSSecondary)
	key = append(key, tableID)
	key = embeddbcore.EncodeUvarint(key, uint64(len(fieldName)))
	key = append(key, fieldName...)
	key = append(key, truncIndexValue(encodeIndexValueBytes(fieldValue))...)
	out := make([]byte, len(key))
	copy(out, key)
	*bp = key
	keyBufPool.Put(bp)
	return out
}

func encodeSecondaryKeyEndPrefix(tableID uint8, fieldName string) []byte {
	bp := keyBufPool.Get().(*[]byte)
	key := (*bp)[:0]
	key = append(key, indexNSSecondary)
	nextID := tableID + 1
	key = append(key, nextID)
	key = embeddbcore.EncodeUvarint(key, uint64(len(fieldName)))
	key = append(key, fieldName...)
	out := make([]byte, len(key))
	copy(out, key)
	*bp = key
	keyBufPool.Put(bp)
	return out
}

func encodeVersionKeyPrefix(tableID uint8, recordID uint32) []byte {
	bp := keyBufPool.Get().(*[]byte)
	key := (*bp)[:0]
	key = append(key, indexNSVersion)
	key = append(key, tableID)
	key = embeddbcore.EncodeUvarint(key, uint64(recordID))
	out := make([]byte, len(key))
	copy(out, key)
	*bp = key
	keyBufPool.Put(bp)
	return out
}

func encodeVersionKey(tableID uint8, recordID uint32, version uint32) []byte {
	bp := keyBufPool.Get().(*[]byte)
	key := (*bp)[:0]
	key = append(key, indexNSVersion)
	key = append(key, tableID)
	key = embeddbcore.EncodeUvarint(key, uint64(recordID))
	key = embeddbcore.EncodeUvarint(key, uint64(version))
	out := make([]byte, len(key))
	copy(out, key)
	*bp = key
	keyBufPool.Put(bp)
	return out
}

func parsePrimaryKey(key []byte) (tableID uint8, pkValue []byte) {
	if len(key) < 2 || key[0] != indexNSPrimary {
		return 0, nil
	}
	return key[1], key[2:]
}

func parseSecondaryKey(key []byte) (tableID uint8, fieldName string, fieldValue []byte, recordID uint32, ok bool) {
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

	valueEnd := len(key) - 4
	if valueEnd < off {
		return 0, "", nil, 0, false
	}
	fieldValue = key[off:valueEnd]
	recordID = binary.BigEndian.Uint32(key[valueEnd:])
	return tableID, fieldName, fieldValue, recordID, true
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
		return embeddbcore.EncodeVarint(buf, int64(v))
	case int8:
		return embeddbcore.EncodeVarint(buf, int64(v))
	case int16:
		return embeddbcore.EncodeVarint(buf, int64(v))
	case int32:
		return embeddbcore.EncodeVarint(buf, int64(v))
	case int64:
		return embeddbcore.EncodeVarint(buf, v)
	case uint:
		return embeddbcore.EncodeUvarint(buf, uint64(v))
	case uint8:
		return embeddbcore.EncodeUvarint(buf, uint64(v))
	case uint16:
		return embeddbcore.EncodeUvarint(buf, uint64(v))
	case uint32:
		return embeddbcore.EncodeUvarint(buf, uint64(v))
	case uint64:
		return embeddbcore.EncodeUvarint(buf, v)
	case float32:
		return embeddbcore.EncodeUvarint(buf, uint64(math.Float64bits(float64(v))))
	case float64:
		return embeddbcore.EncodeUvarint(buf, uint64(math.Float64bits(v)))
	case string:
		return embeddbcore.EncodeString(buf, v)
	case bool:
		if v {
			return embeddbcore.EncodeUvarint(buf, 1)
		}
		return embeddbcore.EncodeUvarint(buf, 0)
	case time.Time:
		return embeddbcore.EncodeVarint(buf, v.UnixNano())
	}
	return embeddbcore.EncodeUvarint(buf, 0)
}
