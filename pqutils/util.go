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
	// TODO... handle the case where a zero value is a legit value
	// assume v is a pointer to a struct

	var sm structMetadata

	rve := reflect.Indirect(reflect.ValueOf(v)).Elem()
	for i := 0; i < rve.NumField(); i++ {
		field := rve.Field(i)
		if field.IsZero() {
			continue
		}
		fieldName := rve.Type().Field(i).Name

		if sm.fieldStringValueMap == nil {
			sm.fieldStringValueMap = make(map[string]string)
		}

		sm.fieldNames = append(sm.fieldNames, fieldName)
		sm.fieldStringValueMap[fieldName] = fmt.Sprintf("%v", field)
	}

	return sm

}

func unmarshalRowsResult(rows *sql.Rows, columnTypes []*sql.ColumnType, schemaType interface{}, sm structMetadata) (interface{}, error) {
	// assume v is a pointer to a struct

	var sd []interface{}
	for range columnTypes {
		// TODO add code for the array test case when we get an array back from postgres
		// sd = append(sd, pq.Array(field.Addr().Interface()))
		var v interface{}
		sd = append(sd, &v)
	}
	err := rows.Scan(sd...)
	if err != nil {
		return nil, err
	}

	rowResult := reflect.New(reflect.ValueOf(schemaType).Type())
	for i, columnType := range columnTypes {
		columnName := columnType.Name()
		columnTypeName := columnType.DatabaseTypeName()
		switch columnTypeName {
		case "INT4":
			rowResult.Elem().FieldByName(sm.columnFieldMap[columnName]).SetInt((*sd[i].(*interface{})).(int64))
		case "VARCHAR":
			rowResult.Elem().FieldByName(sm.columnFieldMap[columnName]).SetString((*sd[i].(*interface{})).(string))
		default:
			return nil, errors.New("scan error: unhandled type: " + columnTypeName)
		}
	}
	return rowResult.Elem().Interface(), nil
}
