package pqutils

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"reflect"
	"strings"
)

type structMetadata struct {
	fieldNames          []string
	fieldStringValueMap map[string]string
	fieldColumnMap      map[string]string
	columnNames         []string
	columnFieldMap      map[string]string
	columnKeyTypeMap    map[string]string
}

func parseStructSqlTags(v interface{}) structMetadata {
	// assume v is a pointer to a struct

	var sm structMetadata

	rve := reflect.Indirect(reflect.ValueOf(v)).Elem()
	for i := 0; i < rve.NumField(); i++ {
		field := rve.Type().Field(i)
		if tagValue, ok := field.Tag.Lookup("sql"); ok && tagValue != "" {
			fieldName := field.Name
			tokens := strings.Split(tagValue, ",")
			columnName := tokens[0]

			if sm.fieldColumnMap == nil {
				sm.fieldColumnMap = make(map[string]string)
			}
			if sm.columnFieldMap == nil {
				sm.columnFieldMap = make(map[string]string)
			}

			sm.fieldNames = append(sm.fieldNames, fieldName)
			sm.columnNames = append(sm.columnNames, columnName)
			sm.fieldColumnMap[fieldName] = columnName
			sm.columnFieldMap[columnName] = fieldName

			if len(tokens) > 1 {
				if sm.columnKeyTypeMap == nil {
					sm.columnKeyTypeMap = make(map[string]string)
				}
				keyType := strings.Join(tokens[1:], ":")
				switch keyType {
				case "primarykey":
					fallthrough
				case "primarykey:serial":
					fallthrough
				case "unique":
					sm.columnKeyTypeMap[columnName] = keyType
				}
			}
		}
	}

	return sm
}

func parseStructFields(v interface{}) structMetadata {
	// assume v is a pointer to a struct

	var sm structMetadata

	rve := reflect.Indirect(reflect.ValueOf(v)).Elem()
	for i := 0; i < rve.NumField(); i++ {
		field := rve.Type().Field(i)
		fieldName := field.Name

		if sm.fieldStringValueMap == nil {
			sm.fieldStringValueMap = make(map[string]string)
		}

		sm.fieldNames = append(sm.fieldNames, fieldName)
		sm.fieldStringValueMap[fieldName] = fmt.Sprintf("%v", rve.Field(i))
	}

	return sm
}

func parseNonZeroStructFields(v interface{}) structMetadata {
	// assume v is a pointer to a struct

	var sm structMetadata

	rve := reflect.Indirect(reflect.ValueOf(v)).Elem()
	for i := 0; i < rve.NumField(); i++ {
		field := rve.Field(i)
		if field.IsZero() {
			continue
		}
		fieldName := field.Type().Name()

		if sm.fieldStringValueMap == nil {
			sm.fieldStringValueMap = make(map[string]string)
		}

		sm.fieldNames = append(sm.fieldNames, fieldName)
		sm.fieldStringValueMap[fieldName] = fmt.Sprintf("%v", field)
	}

	return sm

}

func scanDestination(v interface{}) []interface{} {
	// assume v is a pointer to a struct

	structFields := parseStructFields(v)
	rve := reflect.ValueOf(v).Elem()

	var sd []interface{}
	for _, fieldName := range structFields.fieldNames {
		field := rve.FieldByName(fieldName)
		if field.Kind() == reflect.Slice {
			sd = append(sd, pq.Array(field.Addr().Interface()))
		} else {
			sd = append(sd, field.Addr().Interface())
		}
	}

	return sd
}

func unmarshalRowResult(row *sql.Row, v interface{}) error {
	// assume v is a pointer to a struct
	// caller must first use checkKindPtrToStruct

	// Our assumption is that the order for the query
	// matches the natural order of the struct fields
	// and that each db column has a default value that
	// matches Go's default type rules (so we don't need
	// to use Go's Sql Null types)

	sd := scanDestination(v)
	err := row.Scan(sd...)
	if err != nil {
		return err
	}

	return nil
}

func unmarshalRowsResult(rows *sql.Rows, v interface{}) error {
	// assume v is a pointer to a struct

	// Our assumption is that the order for the query
	// matches the natural order of the struct fields
	// and that each db column has a default value that
	// matches Go's default type rules (so we don't need
	// to use Go's Sql Null types)

	sd := scanDestination(v)
	err := rows.Scan(sd...)
	if err != nil {
		return err
	}

	return nil
}
