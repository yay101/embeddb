package embeddb

import (
	"encoding/binary"
	"fmt"
)

func (db *Database[T]) decodeVarint(data []byte) (int64, []byte, error) {
	val, n := binary.Varint(data)
	if n <= 0 {
		return 0, data, fmt.Errorf("invalid varint")
	}
	return val, data[n:], nil

}

func (db *Database[T]) decodeUvarint(data []byte) (uint64, []byte, error) {
	val, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, data, fmt.Errorf("invalid uvarint")
	}
	return val, data[n:], nil
}

func (db *Database[T]) decodeString(data []byte) (string, []byte, error) {
	length, n := binary.Uvarint(data)
	if n <= 0 {
		return "", data, fmt.Errorf("invalid string length")
	}
	data = data[n:]
	if len(data) < int(length) {
		return "", data, fmt.Errorf("string too short")
	}
	val := string(data[:length])
	data = data[length:]
	return val, data, nil
}

func (db *Database[T]) decodeBool(data []byte) (bool, []byte, error) {
	if len(data) == 0 {
		return false, data, fmt.Errorf("invalid bool")
	}
	val := data[0] != 0
	data = data[1:]
	return val, data, nil
}
