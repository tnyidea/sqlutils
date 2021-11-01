package pqutils

import (
	"database/sql"
	"errors"
	_ "github.com/lib/pq"
	"reflect"
	"strings"
)

func SchemaColumnNames(schema interface{}) ([]string, error) {
	// Assumption: schema is pointer to a struct

	sm, err := parseSchemaMetadata(schema)
	if err != nil {
		return nil, err
	}

	return sm.columnNames, nil
}

func unmarshalRowsResult(rows *sql.Rows, schema interface{}) (interface{}, error) {
	// Assumption: v is a pointer to a struct

	scm, err := parseSchemaMetadata(schema)
	if err != nil {
		return nil, err
	}

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

	schemaType := reflect.Indirect(reflect.ValueOf(schema)).Type()
	rowResult := reflect.New(schemaType)
	for i, columnType := range columnTypes {
		columnName := columnType.Name()
		columnTypeName := columnType.DatabaseTypeName()
		switch columnTypeName {
		case "INT4":
			rowResult.Elem().FieldByName(scm.columnNameFieldNameMap[columnName]).SetInt((*sd[i].(*interface{})).(int64))
		case "VARCHAR":
			rowResult.Elem().FieldByName(scm.columnNameFieldNameMap[columnName]).SetString((*sd[i].(*interface{})).(string))
		default:
			return nil, errors.New("scan error: unhandled type: " + columnTypeName)
		}
	}
	return rowResult.Elem().Interface(), nil
}

type schemaMetadata struct {
	fieldNames             []string
	fieldNameColumnNameMap map[string]string
	columnNames            []string
	columnNameFieldNameMap map[string]string
	columnNameFieldKindMap map[string]reflect.Kind
	columnKeyTypeMap       map[string]string
}

// parseSchemaMetadata reutrns a schemaMetadata object for the passed value v
//
// NOTE: If v is truly passed as an interface (i.e. caller receives v as an interface
// and forwards v as an interface), then it can't be treated like a struct with a value.
// If it is a pointer to a struct then you can treat it like a struct with a value.
// More often then not, helper functions will just receive interface, unless we insist
// our caller give us a pointer to a struct. In our internal design it is sufficient not
// to error check for this, but any originating call that ultimately relies on this function
// should insist on a pointer to a struct, so we can treat v as though it were a valued struct.
func parseSchemaMetadata(v interface{}) (schemaMetadata, error) {
	// Check that v is a pointer to a struct
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return schemaMetadata{}, errors.New("invalid type: must be a non-nil pointer to a struct: " + reflect.TypeOf(v).String())
	}
	if reflect.Indirect(rv).Kind() != reflect.Struct {
		return schemaMetadata{}, errors.New("invalid type: must be a non-nil pointer to a struct: " + reflect.TypeOf(v).String())
	}

	rve := rv.Elem()

	var scm schemaMetadata
	for i := 0; i < rve.NumField(); i++ {
		structField := rve.Type().Field(i)
		if tagValue, ok := structField.Tag.Lookup("sql"); ok && tagValue != "" {
			fieldName := structField.Name
			tokens := strings.Split(tagValue, ",")
			columnName := tokens[0]

			if scm.fieldNameColumnNameMap == nil {
				scm.fieldNameColumnNameMap = make(map[string]string)
			}
			if scm.columnNameFieldNameMap == nil {
				scm.columnNameFieldNameMap = make(map[string]string)
			}
			if scm.columnNameFieldKindMap == nil {
				scm.columnNameFieldKindMap = make(map[string]reflect.Kind)
			}

			scm.fieldNames = append(scm.fieldNames, fieldName)
			scm.fieldNameColumnNameMap[fieldName] = columnName
			scm.columnNames = append(scm.columnNames, columnName)
			scm.columnNameFieldNameMap[columnName] = fieldName
			scm.columnNameFieldKindMap[columnName] = structField.Type.Kind()

			if len(tokens) > 1 {
				if scm.columnKeyTypeMap == nil {
					scm.columnKeyTypeMap = make(map[string]string)
				}
				keyType := strings.Join(tokens[1:], ":")
				switch keyType {
				case "primarykey":
					fallthrough
				case "primarykey:serial":
					fallthrough
				case "unique":
					scm.columnKeyTypeMap[columnName] = keyType
				}
			}
		}
	}

	return scm, nil
}
