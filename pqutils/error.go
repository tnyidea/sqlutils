package pqutils

import "reflect"

type InvalidTypeError struct {
	RequiredType string
	InvalidType  reflect.Type
}

func (e *InvalidTypeError) Error() string {
	if e.RequiredType == "Struct" {
		return "invalid type: must be a non-pointer struct type: +" + e.InvalidType.String()
	}

	if e.RequiredType == "StructPtr" &&
		(e.InvalidType == nil || e.InvalidType.Kind() != reflect.Ptr) {
		return "invalid type: must be a non-nil pointer to a struct: " + e.InvalidType.String()
	}

	if e.RequiredType == "SlicePtr" &&
		e.InvalidType.Kind() != reflect.Slice {
		return "invalid type: must be a pointer to a slice of struct: " + e.InvalidType.String()
	}

	return "invalid type: " + e.InvalidType.String()
}

func checkKindStruct(v interface{}) error {
	rv := reflect.ValueOf(v)

	if rv.Kind() == reflect.Ptr {
		return &InvalidTypeError{"Struct", reflect.TypeOf(v)}
	}
	if reflect.Indirect(rv).Kind() != reflect.Struct {
		return &InvalidTypeError{"Struct", reflect.TypeOf(v)}
	}

	return nil
}

func checkKindStructPtr(v interface{}) error {
	rv := reflect.ValueOf(v)

	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &InvalidTypeError{"StructPtr", reflect.TypeOf(v)}
	}
	if reflect.Indirect(rv).Kind() != reflect.Struct {
		return &InvalidTypeError{"StructPtr", reflect.TypeOf(v)}
	}

	return nil
}

func checkKindSlicePtr(v interface{}) error {
	rv := reflect.ValueOf(v)

	if rv.Kind() != reflect.Slice {
		return &InvalidTypeError{"SlicePtr", reflect.TypeOf(v)}
	}
	if rv.Elem().Kind() != reflect.Struct {
		return &InvalidTypeError{"SlicePtr", reflect.TypeOf(v)}
	}

	return nil
}
