package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"reflect"
	"strings"
)

func SelectOne(db *sql.DB, table string, v interface{}) (interface{}, error) {
	err := checkKindStruct(v)
	if err != nil {
		return nil, err
	}

	sm := parseSchemaTypeValue(&v)

	var where map[string]interface{}
	for columnName, keyType := range sm.columnKeyTypeMap {
		if strings.Contains(keyType, "primarykey") {
			if where == nil {
				where = make(map[string]interface{})
			}
			fieldName := sm.columnNameFieldNameMap[columnName]
			fieldValue := sm.fieldNameStringValueMap[fieldName]

			if reflect.ValueOf(v).FieldByName(fieldName).IsZero() {
				return nil, errors.New("error: zero value recieved for primary key field: " + fieldName + ". All primary key fields must be non-zero")
			}
			where[fieldName] = fieldValue
		}
	}

	// Test for uniqueness, if valid should only have one record that matches
	emptyResult := reflect.New(reflect.ValueOf(v).Type()).Elem().Interface()
	results, err := selectAllWithOptions(db, table, v, where, QueryOptions{})
	if err != nil {
		return emptyResult, err
	}
	if len(results) == 0 {
		return emptyResult, nil
	}

	return results[0], nil
}

func SelectAll(db *sql.DB, table string, schemaType interface{}) ([]interface{}, error) {
	return selectAllWithOptions(db, table, schemaType, nil, QueryOptions{})
}

func SelectAllWithOptions(db *sql.DB, table string, schemaType interface{}, where map[string]interface{}, options QueryOptions) ([]interface{}, error) {
	return selectAllWithOptions(db, table, schemaType, where, options)
}

func selectAllWithOptions(db *sql.DB, table string, schemaType interface{},
	where map[string]interface{}, options QueryOptions) ([]interface{}, error) {

	sm := parseSchemaTypeValue(&schemaType)

	query := `SELECT ` + strings.Join(sm.columnNames, ", ") + `
		FROM ` + table +
		whereConditionString(schemaType, where) +
		options.String()
	log.Println(query)

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
		rowResult, err := unmarshalRowsResult(rows, schemaType)
		if err != nil {
			return nil, err
		}
		results = append(results, rowResult)
	}

	return results, nil
}
