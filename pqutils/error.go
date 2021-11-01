package pqutils

import "reflect"

type InvalidTypeError struct {
	RequiredType string
	InvalidType  reflect.Type
}

func (e *InvalidTypeError) Error() string {
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
