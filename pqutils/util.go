package pqutils

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"reflect"
	"strings"
)

type structMetadata struct {
	fieldNames              []string
	fieldNameValueMap       map[string]interface{}
	fieldNameStringValueMap map[string]string
	fieldNameColumnNameMap  map[string]string
	columnNames             []string
	columnNameFieldNameMap  map[string]string
	columnNameFieldKindMap  map[string]reflect.Kind
	columnKeyTypeMap        map[string]string
}

func GetSchemaTypeColumnNames(structType interface{}) []string {
	// Assume v is a pointer to a struct

	sm := parseSchemaTypeValue(&structType)
	return sm.columnNames
}

func parseSchemaTypeValue(v interface{}) structMetadata {
	// Assume v is a pointer to a struct

	var sm structMetadata

	rve := reflect.Indirect(reflect.ValueOf(v)).Elem()
	for i := 0; i < rve.NumField(); i++ {
		field := rve.Type().Field(i)
		if tagValue, ok := field.Tag.Lookup("sql"); ok && tagValue != "" {
			fieldName := field.Name
			tokens := strings.Split(tagValue, ",")
			columnName := tokens[0]

			if sm.fieldNameColumnNameMap == nil {
				sm.fieldNameColumnNameMap = make(map[string]string)
			}
			if sm.fieldNameValueMap == nil {
				sm.fieldNameValueMap = make(map[string]interface{})
			}
			if sm.fieldNameStringValueMap == nil {
				sm.fieldNameStringValueMap = make(map[string]string)
			}
			if sm.columnNameFieldNameMap == nil {
				sm.columnNameFieldNameMap = make(map[string]string)
			}
			if sm.columnNameFieldKindMap == nil {
				sm.columnNameFieldKindMap = make(map[string]reflect.Kind)
			}

			sm.fieldNames = append(sm.fieldNames, fieldName)
			sm.fieldNameValueMap[fieldName] = rve.Field(i).Interface()
			sm.fieldNameStringValueMap[fieldName] = fmt.Sprintf("%v", rve.Field(i))
			sm.fieldNameColumnNameMap[fieldName] = columnName
			sm.columnNames = append(sm.columnNames, columnName)
			sm.columnNameFieldNameMap[columnName] = fieldName
			sm.columnNameFieldKindMap[columnName] = field.Type.Kind()

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

func UnmarshalRowsResult(rows *sql.Rows, schemaType interface{}) (interface{}, error) {
	// assume schemaType is a struct

	sm := parseSchemaTypeValue(&schemaType)

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var sd []interface{}
	for range columnTypes {
		// TODO add code for the array test case when we get an array back from postgres
		// sd = append(sd, pq.Array(field.Addr().Interface()))
		var v interface{}
		sd = append(sd, &v)
	}
	err = rows.Scan(sd...)
	if err != nil {
		return nil, err
	}

	rowResult := reflect.New(reflect.ValueOf(schemaType).Type())
	for i, columnType := range columnTypes {
		columnName := columnType.Name()
		columnTypeName := columnType.DatabaseTypeName()
		switch columnTypeName {
		case "INT4":
			rowResult.Elem().FieldByName(sm.columnNameFieldNameMap[columnName]).SetInt((*sd[i].(*interface{})).(int64))
		case "VARCHAR":
			rowResult.Elem().FieldByName(sm.columnNameFieldNameMap[columnName]).SetString((*sd[i].(*interface{})).(string))
		default:
			return nil, errors.New("scan error: unhandled type: " + columnTypeName)
		}
	}
	return rowResult.Elem().Interface(), nil
}
