package embeddb

import (
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

// FieldOffset stores the offset and type information for a field in a struct
type FieldOffset struct {
	Name       string
	Offset     uintptr // Absolute offset from the start of the root struct
	Type       reflect.Kind
	Size       uintptr
	Primary    bool
	Unique     bool
	Key        byte
	IsStruct   bool
	StructType reflect.Type
	Parent     []string     // For nested structs
	IsTime     bool         // True if the field is time.Time
	IsSlice    bool         // True if the field is a slice
	SliceElem  reflect.Type // Element type of the slice
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
		PrimaryKey:   255, // Use 255 as sentinel value meaning "no primary key"
	}

	byteKey := byte(0)
	computeFieldOffsets(t, 0, &byteKey, []string{}, layout)

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
// baseOffset is the accumulated offset from the root struct to the current struct
func computeFieldOffsets(t reflect.Type, baseOffset uintptr, byteKey *byte, parentPath []string, layout *StructLayout) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Create the complete field path for nested structs
		fieldPath := append([]string{}, parentPath...)
		fieldPath = append(fieldPath, field.Name)

		// Check for primary key tag (handles comma-separated values like "id,primary")
		dbTag := field.Tag.Get("db")
		isPrimary := strings.Contains(dbTag, "id") || strings.Contains(dbTag, "primary")
		if isPrimary {
			layout.PrimaryKey = *byteKey
		}

		// Check for unique tag (handles comma-separated values like "unique,index")
		isUnique := strings.Contains(dbTag, "unique")

		// Check if this is a time.Time field
		isTimeField := field.Type.PkgPath() == "time" && field.Type.Name() == "Time"

		// Check if this is a slice field
		isSliceField := field.Type.Kind() == reflect.Slice
		var sliceElem reflect.Type
		if isSliceField {
			sliceElem = field.Type.Elem()
		}

		// Create field offset info
		// Calculate absolute offset from root struct by adding base offset
		absoluteOffset := baseOffset + field.Offset
		fieldOffset := FieldOffset{
			Name:      strings.Join(fieldPath, "."),
			Offset:    absoluteOffset,
			Type:      field.Type.Kind(),
			Size:      field.Type.Size(),
			Primary:   isPrimary,
			Unique:    isUnique,
			Key:       *byteKey,
			Parent:    parentPath,
			IsTime:    isTimeField,
			IsSlice:   isSliceField,
			SliceElem: sliceElem,
		}

		// Handle nested structs
		if field.Type.Kind() == reflect.Struct {
			fieldOffset.IsStruct = true
			fieldOffset.StructType = field.Type

			// Store the struct field itself
			layout.FieldOffsets[*byteKey] = fieldOffset
			*byteKey++

			// Recursively process the nested struct fields
			// Pass the absolute offset so nested fields have correct offsets from root
			computeFieldOffsets(field.Type, absoluteOffset, byteKey, fieldPath, layout)
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
		// Check if it's time.Time
		if offset.IsTime {
			return *(*time.Time)(fieldPtr), nil
		}
		return fieldPtr, nil
	case reflect.Slice:
		// Return the slice header pointer so we can iterate over elements
		// The slice header contains: ptr, len, cap
		return fieldPtr, nil
	default:
		return nil, fmt.Errorf("unsupported field type: %v", offset.Type)
	}
}

// SetFieldValue uses the field offset to directly set a field's value in the struct
// This avoids reflection during database operations
// Handles type conversions from decoded values (e.g., int64 -> int, uint64 -> uint32)
func SetFieldValue(data interface{}, offset FieldOffset, value interface{}) error {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	fieldPtr := unsafe.Add(ptr, offset.Offset)

	switch offset.Type {
	case reflect.Int:
		// Handle conversion from int64 (varint decode result)
		switch v := value.(type) {
		case int64:
			*(*int)(fieldPtr) = int(v)
		case int:
			*(*int)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to int", value)
		}
	case reflect.Int8:
		switch v := value.(type) {
		case int64:
			*(*int8)(fieldPtr) = int8(v)
		case int8:
			*(*int8)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to int8", value)
		}
	case reflect.Int16:
		switch v := value.(type) {
		case int64:
			*(*int16)(fieldPtr) = int16(v)
		case int16:
			*(*int16)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to int16", value)
		}
	case reflect.Int32:
		switch v := value.(type) {
		case int64:
			*(*int32)(fieldPtr) = int32(v)
		case int32:
			*(*int32)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to int32", value)
		}
	case reflect.Int64:
		switch v := value.(type) {
		case int64:
			*(*int64)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to int64", value)
		}
	case reflect.Uint:
		// Handle conversion from uint64 (uvarint decode result)
		switch v := value.(type) {
		case uint64:
			*(*uint)(fieldPtr) = uint(v)
		case uint:
			*(*uint)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to uint", value)
		}
	case reflect.Uint8:
		switch v := value.(type) {
		case uint64:
			*(*uint8)(fieldPtr) = uint8(v)
		case uint8:
			*(*uint8)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to uint8", value)
		}
	case reflect.Uint16:
		switch v := value.(type) {
		case uint64:
			*(*uint16)(fieldPtr) = uint16(v)
		case uint16:
			*(*uint16)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to uint16", value)
		}
	case reflect.Uint32:
		switch v := value.(type) {
		case uint64:
			*(*uint32)(fieldPtr) = uint32(v)
		case uint32:
			*(*uint32)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to uint32", value)
		}
	case reflect.Uint64:
		switch v := value.(type) {
		case uint64:
			*(*uint64)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to uint64", value)
		}
	case reflect.Float32:
		switch v := value.(type) {
		case float64:
			*(*float32)(fieldPtr) = float32(v)
		case float32:
			*(*float32)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to float32", value)
		}
	case reflect.Float64:
		switch v := value.(type) {
		case float64:
			*(*float64)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to float64", value)
		}
	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			*(*bool)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to bool", value)
		}
	case reflect.String:
		switch v := value.(type) {
		case string:
			*(*string)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to string", value)
		}
	case reflect.Struct:
		// Check if this is a time.Time field
		if offset.IsTime {
			switch v := value.(type) {
			case time.Time:
				*(*time.Time)(fieldPtr) = v
			default:
				return fmt.Errorf("cannot convert %T to time.Time", value)
			}
		} else {
			return fmt.Errorf("unsupported struct field type: %v", offset.Type)
		}
	case reflect.Slice:
		sliceVal, ok := value.([]interface{})
		if !ok {
			return fmt.Errorf("cannot convert %T to slice", value)
		}
		if len(sliceVal) == 0 {
			break
		}
		sliceHeader := (*reflect.SliceHeader)(fieldPtr)
		elemType := offset.SliceElem
		sliceSize := elemType.Size()
		data := make([]byte, len(sliceVal)*int(sliceSize))
		for i, elem := range sliceVal {
			elemPtr := unsafe.Pointer(uintptr(unsafe.Pointer(&data[0])) + uintptr(i)*uintptr(sliceSize))
			switch elemType.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				var intVal int64
				switch v := elem.(type) {
				case int64:
					intVal = v
				case int:
					intVal = int64(v)
				case int32:
					intVal = int64(v)
				case int16:
					intVal = int64(v)
				case int8:
					intVal = int64(v)
				}
				switch elemType.Kind() {
				case reflect.Int:
					*(*int)(elemPtr) = int(intVal)
				case reflect.Int8:
					*(*int8)(elemPtr) = int8(intVal)
				case reflect.Int16:
					*(*int16)(elemPtr) = int16(intVal)
				case reflect.Int32:
					*(*int32)(elemPtr) = int32(intVal)
				case reflect.Int64:
					*(*int64)(elemPtr) = intVal
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				var uintVal uint64
				switch v := elem.(type) {
				case uint64:
					uintVal = v
				case uint:
					uintVal = uint64(v)
				case uint32:
					uintVal = uint64(v)
				case uint16:
					uintVal = uint64(v)
				case uint8:
					uintVal = uint64(v)
				}
				switch elemType.Kind() {
				case reflect.Uint:
					*(*uint)(elemPtr) = uint(uintVal)
				case reflect.Uint8:
					*(*uint8)(elemPtr) = uint8(uintVal)
				case reflect.Uint16:
					*(*uint16)(elemPtr) = uint16(uintVal)
				case reflect.Uint32:
					*(*uint32)(elemPtr) = uint32(uintVal)
				case reflect.Uint64:
					*(*uint64)(elemPtr) = uintVal
				}
			case reflect.Float32, reflect.Float64:
				var floatVal float64
				switch v := elem.(type) {
				case float64:
					floatVal = v
				case float32:
					floatVal = float64(v)
				}
				if elemType.Kind() == reflect.Float32 {
					*(*float32)(elemPtr) = float32(floatVal)
				} else {
					*(*float64)(elemPtr) = floatVal
				}
			case reflect.String:
				strVal, _ := elem.(string)
				strHdr := (*reflect.StringHeader)(elemPtr)
				if len(strVal) > 0 {
					strData := make([]byte, len(strVal))
					copy(strData, strVal)
					strHdr.Data = uintptr(unsafe.Pointer(&strData[0]))
					strHdr.Len = len(strVal)
				}
			case reflect.Bool:
				boolVal, _ := elem.(bool)
				*(*bool)(elemPtr) = boolVal
			}
		}
		sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
		sliceHeader.Len = len(sliceVal)
		sliceHeader.Cap = len(sliceVal)
	default:
		return fmt.Errorf("unsupported field type: %v", offset.Type)
	}

	return nil
}
