package embeddb

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	ValueStartMarker byte = 0x1E
	ValueEndMarker   byte = 0x1F
)

func EncodeVarint(buffer []byte, value int64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], value)
	return append(buffer, tmp[:n]...)
}

func EncodeUvarint(buffer []byte, value uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], value)
	return append(buffer, tmp[:n]...)
}

func EncodeString(buffer []byte, value string) []byte {
	buffer = EncodeUvarint(buffer, uint64(len(value)))
	return append(buffer, value...)
}

func EncodeBool(buffer []byte, value bool) []byte {
	if value {
		return append(buffer, 1)
	}
	return append(buffer, 0)
}

func EncodeFloat64(buffer []byte, value float64) []byte {
	bits := math.Float64bits(value)
	return EncodeUvarint(buffer, bits)
}

func DecodeVarint(data []byte) (int64, []byte, error) {
	val, n := binary.Varint(data)
	if n <= 0 {
		return 0, data, errors.New("invalid varint encoding")
	}
	return val, data[n:], nil
}

func DecodeUvarint(data []byte) (uint64, []byte, error) {
	val, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, data, errors.New("invalid uvarint encoding")
	}
	return val, data[n:], nil
}

func DecodeString(data []byte) (string, []byte, error) {
	length, n := binary.Uvarint(data)
	if n <= 0 {
		return "", data, errors.New("invalid string length")
	}
	data = data[n:]
	if len(data) < int(length) {
		return "", data, errors.New("string data too short")
	}
	val := string(data[:length])
	data = data[length:]
	return val, data, nil
}

func DecodeBool(data []byte) (bool, []byte, error) {
	if len(data) < 1 {
		return false, data, errors.New("bool data too short")
	}
	val := data[0] != 0
	return val, data[1:], nil
}

func DecodeFloat64(data []byte) (float64, []byte, error) {
	bits, remaining, err := DecodeUvarint(data)
	if err != nil {
		return 0, data, err
	}
	return math.Float64frombits(bits), remaining, nil
}
