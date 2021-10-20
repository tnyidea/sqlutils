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
	rows, err := selectAllWithOptions(db, table, v, where, QueryOptions{})
	if err != nil {
		return emptyResult, err
	}

	var result interface{}
	if rows.Next() {
		rowResult, err := unmarshalRowsResult(rows, v)
		if err != nil {
			return nil, err
		}
		result = rowResult
	}

	if result == nil {
		return emptyResult, errors.New("not found: cannot find unique value for primary key values of v")
	}

	return result, nil
}

func SelectAll(db *sql.DB, table string, schemaType interface{}) (*sql.Rows, error) {
	return selectAllWithOptions(db, table, schemaType, nil, QueryOptions{})
}

func SelectAllWithOptions(db *sql.DB, table string, schemaType interface{}, where map[string]string, options QueryOptions) (*sql.Rows, error) {
	return selectAllWithOptions(db, table, schemaType, where, options)
}

func selectAllWithOptions(db *sql.DB, table string, schemaType interface{},
	where map[string]string, options QueryOptions) (*sql.Rows, error) {

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

	return rows, nil
}
