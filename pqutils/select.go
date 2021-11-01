package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

func SelectOne(db *sql.DB, table string, v interface{}) (interface{}, error) {
	// Assumption: v is a pointer to a struct

	scm, err := parseSchemaMetadata(v)
	if err != nil {
		return nil, err
	}
	stm, err := parseStructMetadata(v)
	if err != nil {
		return nil, err
	}

	var where map[string]interface{}
	for columnName, keyType := range scm.columnKeyTypeMap {
		if strings.Contains(keyType, "primarykey") {
			if where == nil {
				where = make(map[string]interface{})
			}
			fieldName := scm.columnNameFieldNameMap[columnName]
			fieldValue := stm.fieldNameValueMap[fieldName]

			if reflect.Indirect(reflect.ValueOf(v)).FieldByName(fieldName).IsZero() {
				return nil, errors.New("error: zero value recieved for primary key field: " + fieldName + ". All primary key fields must be non-zero")
			}
			where[fieldName] = fieldValue
		}
	}

	// Test for uniqueness, if valid should only have one record that matches
	schemaType := reflect.Indirect(reflect.ValueOf(v)).Type()
	emptyResult := reflect.New(schemaType).Elem().Interface()
	results, err := selectAllWithOptions(db, table, v, where, QueryOptions{})
	if err != nil {
		return emptyResult, err
	}
	if len(results) == 0 {
		return emptyResult, nil
	}

	return results[0], nil
}

func SelectAll(db *sql.DB, table string, schema interface{}) ([]interface{}, error) {
	return selectAllWithOptions(db, table, schema, nil, QueryOptions{})
}

func SelectAllWithOptions(db *sql.DB, table string, schema interface{}, where map[string]interface{}, options QueryOptions) ([]interface{}, error) {
	return selectAllWithOptions(db, table, schema, where, options)
}

func selectAllWithOptions(db *sql.DB, table string, schema interface{},
	where map[string]interface{}, options QueryOptions) ([]interface{}, error) {
	// Assumption: schema is a pointer to a struct

	// TODO consider passing a context that allows for the setting of metadata to improve performance

	scm, err := parseSchemaMetadata(schema)
	if err != nil {
		return nil, err
	}

	condition, err := queryConditionString(schema, where, options)
	if err != nil {
		return nil, err
	}

	query := `SELECT ` + strings.Join(scm.columnNames, ", ") + `
		FROM ` + table + ` ` +
		condition

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

	// Collect the results
	var results []interface{}
	for rows.Next() {
		var rowResult interface{}
		rowResult, err := unmarshalRowsResult(rows, schema)
		if err != nil {
			return nil, err
		}
		results = append(results, rowResult)
	}

	return results, nil
}
