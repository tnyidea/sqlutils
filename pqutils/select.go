package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

func SelectOne(db *sql.DB, table string, v interface{}) (interface{}, error) {
	err := checkKindStruct(v)
	if err != nil {
		return nil, err
	}

	sm := parseSchemaTypeValue(&v)

	var where map[string]string
	for columnName, keyType := range sm.columnKeyTypeMap {
		if strings.Contains(keyType, "primarykey") {
			if where == nil {
				where = make(map[string]string)
			}
			fieldName := sm.columnNameFieldNameMap[columnName]
			where[fieldName] = sm.fieldNameStringValueMap[fieldName]
		}
	}

	// Test for uniqueness, if valid should only have one record that matches
	emptyResult := reflect.New(reflect.ValueOf(v).Type()).Elem().Interface()
	result, err := selectAllWithOptions(db, table, v, where, QueryOptions{})
	if err != nil {
		return emptyResult, err
	}
	if len(result) != 1 {
		return emptyResult, errors.New("not found: cannot find unique value for primary key values of v")
	}

	return result[0], nil
}

func SelectAll(db *sql.DB, table string, schemaType interface{}) ([]interface{}, error) {
	return selectAllWithOptions(db, table, schemaType, nil, QueryOptions{})
}

func SelectAllWithOptions(db *sql.DB, table string, schemaType interface{}, where map[string]string, options QueryOptions) ([]interface{}, error) {
	return selectAllWithOptions(db, table, schemaType, where, options)
}

func selectAllWithOptions(db *sql.DB, table string, schemaType interface{},
	where map[string]string, options QueryOptions) ([]interface{}, error) {

	sm := parseSchemaTypeValue(&schemaType)

	query := `SELECT ` + strings.Join(sm.columnNames, ", ") + `
		FROM ` + table +
		whereConditionString(schemaType, where) +
		options.String()

	// Execute the Query
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	// Gather column and struct information
	sm = parseSchemaTypeValue(&schemaType)
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Collect the results
	var result []interface{}
	for rows.Next() {
		rowResult, err := unmarshalRowsResult(rows, columnTypes, schemaType, sm)
		if err != nil {
			return nil, err
		}
		result = append(result, rowResult)
	}

	return result, nil
}
