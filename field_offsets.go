package embeddb

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

// FieldOffset stores the offset and type information for a field in a struct
type FieldOffset struct {
	Name       string
	Offset     uintptr
	Type       reflect.Kind
	Size       uintptr
	Primary    bool
	Unique     bool
	Key        byte
	IsStruct   bool
	StructType reflect.Type
	Parent     []string // For nested structs
}

// StructLayout contains the mapping of field byte keys to their offsets
type StructLayout struct {
	FieldOffsets map[byte]FieldOffset
	Size         uintptr
	Hash         string // Hash of the struct layout to detect changes
	PrimaryKey   byte   // Byte key of the primary key field (if any)
}

// ComputeStructLayout analyzes the provided struct and returns a StructLayout
// containing offsets for all fields, used for direct memory access
func ComputeStructLayout(data interface{}) (*StructLayout, error) {
	t := reflect.TypeOf(data)
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct type, got %v", t.Kind())
	}

	layout := &StructLayout{
		FieldOffsets: make(map[byte]FieldOffset),
	}

	byteKey := byte(0)
	computeFieldOffsets(t, unsafe.Pointer(nil), &byteKey, []string{}, layout)

	// Generate a simple hash of the struct layout for checking compatibility
	var hashBuilder strings.Builder
	for i := byte(0); i < byteKey; i++ {
		if field, exists := layout.FieldOffsets[i]; exists {
			hashBuilder.WriteString(fmt.Sprintf("%s:%d:%v,", field.Name, field.Offset, field.Type))
		}
	}
	layout.Hash = hashBuilder.String()
	layout.Size = t.Size()

	return layout, nil
}

// computeFieldOffsets recursively computes the offset of each field in the struct
func computeFieldOffsets(t reflect.Type, base unsafe.Pointer, byteKey *byte, parentPath []string, layout *StructLayout) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Create the complete field path for nested structs
		fieldPath := append([]string{}, parentPath...)
		fieldPath = append(fieldPath, field.Name)

		// Check for primary key tag
		isPrimary := field.Tag.Get("db") == "id"
		if isPrimary {
			layout.PrimaryKey = *byteKey
		}

		// Check for unique tag
		isUnique := field.Tag.Get("db") == "unique"

		// Create field offset info
		offset := field.Offset
		fieldOffset := FieldOffset{
			Name:    strings.Join(fieldPath, "."),
			Offset:  offset,
			Type:    field.Type.Kind(),
			Size:    field.Type.Size(),
			Primary: isPrimary,
			Unique:  isUnique,
			Key:     *byteKey,
			Parent:  parentPath,
		}

		// Handle nested structs
		if field.Type.Kind() == reflect.Struct {
			fieldOffset.IsStruct = true
			fieldOffset.StructType = field.Type

			// Store the struct field itself
			layout.FieldOffsets[*byteKey] = fieldOffset
			*byteKey++

			// Recursively process the nested struct fields
			computeFieldOffsets(field.Type, unsafe.Add(base, offset), byteKey, fieldPath, layout)
		} else {
			// Store regular field
			layout.FieldOffsets[*byteKey] = fieldOffset
			*byteKey++
		}
	}
}

// GetFieldValue uses the field offset to directly read a field's value from the struct
// This avoids reflection during database operations
func GetFieldValue(data interface{}, offset FieldOffset) (interface{}, error) {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	fieldPtr := unsafe.Add(ptr, offset.Offset)

	switch offset.Type {
	case reflect.Int:
		return *(*int)(fieldPtr), nil
	case reflect.Int8:
		return *(*int8)(fieldPtr), nil
	case reflect.Int16:
		return *(*int16)(fieldPtr), nil
	case reflect.Int32:
		return *(*int32)(fieldPtr), nil
	case reflect.Int64:
		return *(*int64)(fieldPtr), nil
	case reflect.Uint:
		return *(*uint)(fieldPtr), nil
	case reflect.Uint8:
		return *(*uint8)(fieldPtr), nil
	case reflect.Uint16:
		return *(*uint16)(fieldPtr), nil
	case reflect.Uint32:
		return *(*uint32)(fieldPtr), nil
	case reflect.Uint64:
		return *(*uint64)(fieldPtr), nil
	case reflect.Float32:
		return *(*float32)(fieldPtr), nil
	case reflect.Float64:
		return *(*float64)(fieldPtr), nil
	case reflect.Bool:
		return *(*bool)(fieldPtr), nil
	case reflect.String:
		// Strings need special handling since they're a reference type
		strHeader := (*reflect.StringHeader)(fieldPtr)
		if strHeader.Len == 0 {
			return "", nil
		}
		return *(*string)(fieldPtr), nil
	case reflect.Struct:
		// For embedded structs, return a pointer to the struct
		return fieldPtr, nil
	default:
		return nil, fmt.Errorf("unsupported field type: %v", offset.Type)
	}
}

// SetFieldValue uses the field offset to directly set a field's value in the struct
// This avoids reflection during database operations
func SetFieldValue(data interface{}, offset FieldOffset, value interface{}) error {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	fieldPtr := unsafe.Add(ptr, offset.Offset)

	switch offset.Type {
	case reflect.Int:
		*(*int)(fieldPtr) = value.(int)
	case reflect.Int8:
		*(*int8)(fieldPtr) = value.(int8)
	case reflect.Int16:
		*(*int16)(fieldPtr) = value.(int16)
	case reflect.Int32:
		*(*int32)(fieldPtr) = value.(int32)
	case reflect.Int64:
		*(*int64)(fieldPtr) = value.(int64)
	case reflect.Uint:
		*(*uint)(fieldPtr) = value.(uint)
	case reflect.Uint8:
		*(*uint8)(fieldPtr) = value.(uint8)
	case reflect.Uint16:
		*(*uint16)(fieldPtr) = value.(uint16)
	case reflect.Uint32:
		*(*uint32)(fieldPtr) = value.(uint32)
	case reflect.Uint64:
		*(*uint64)(fieldPtr) = value.(uint64)
	case reflect.Float32:
		*(*float32)(fieldPtr) = value.(float32)
	case reflect.Float64:
		*(*float64)(fieldPtr) = value.(float64)
	case reflect.Bool:
		*(*bool)(fieldPtr) = value.(bool)
	case reflect.String:
		// Strings need special handling since they're a reference type
		*(*string)(fieldPtr) = value.(string)
	default:
		return fmt.Errorf("unsupported field type: %v", offset.Type)
	}

	return nil
}
