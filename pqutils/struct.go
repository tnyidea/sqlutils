package pqutils

import (
	"errors"
	"reflect"
)

type structMetadata struct {
	fieldNames           []string
	fieldNameValueMap    map[string]interface{}
	jsonNames            []string
	jsonNameFieldNameMap map[string]string
}

// parseStructMetadata reutrns a structMetadata object for the passed value v
//
// NOTE: If v is passed as a pure interface (i.e. caller receives v as an interface
// and forwards v as an interface), then it can't be treated like a struct with a value.
// If it is a pointer to a struct then it can be treated can treat it like a struct with
// a value. More often then not, callers of helper functions will only have a intervace for
// v.  For this reason, we call checkStructPtr() and return an error if v is not a pointer
// to a struct.
func parseStructMetadata(v interface{}) (structMetadata, error) {
	// Check that v is a pointer to a struct
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return structMetadata{}, errors.New("invalid type: must be a non-nil pointer to a struct: " + reflect.TypeOf(v).String())
	}
	if reflect.Indirect(rv).Kind() != reflect.Struct {
		return structMetadata{}, errors.New("invalid type: must be a non-nil pointer to a struct: " + reflect.TypeOf(v).String())
	}

	rve := rv.Elem()
	var sm structMetadata
	for i := 0; i < rve.NumField(); i++ {
		structField := rve.Type().Field(i)
		fieldName := structField.Name

		if sm.fieldNameValueMap == nil {
			sm.fieldNameValueMap = make(map[string]interface{})
		}
		sm.fieldNames = append(sm.fieldNames, fieldName)
		sm.fieldNameValueMap[fieldName] = rve.Field(i).Interface()

		if jsonName := structField.Tag.Get("json"); jsonName != "" {
			if sm.jsonNameFieldNameMap == nil {
				sm.jsonNameFieldNameMap = make(map[string]string)
			}
			sm.jsonNames = append(sm.jsonNames, jsonName)
			sm.jsonNameFieldNameMap[jsonName] = fieldName
		}
	}

	return sm, nil
}
