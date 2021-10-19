package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"reflect"
	"strings"
)

func SelectOne(result interface{}, db *sql.DB, table string, where interface{}) error {
	err := checkKindStructPtr(result)
	if err != nil {
		return err
	}
	err = checkKindStructPtr(where)
	if err != nil {
		return err
	}

	structSqlTags := parseStructSqlTags(where)
	query := `SELECT ` + strings.Join(structSqlTags.columnNames, ", ") +
		`FROM ` + table +
		whereConditionString(where)

	// Execute the Query
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	row := conn.QueryRowContext(ctx, query)
	err = unmarshalRowResult(row, result)
	if err != nil {
		return err
	}

	return nil
}

func SelectAllWithOptions(db *sql.DB, table string,
	where interface{}, options QueryOptions) ([]interface{}, error) {

	//err := checkKindSlicePtr(result)
	//if err != nil {
	//	return err
	//}
	err := checkKindStruct(where)
	if err != nil {
		return nil, err
	}

	structSqlTags := parseStructSqlTags(&where)
	query := `SELECT ` + strings.Join(structSqlTags.columnNames, ", ") + `
		FROM ` + table +
		whereConditionString(where) +
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

	log.Println(rows.Columns())


	// Index column types
	columnTypeMap := make(map[string]string)
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	for _, columnType := range columnTypes {
		columnTypeMap[columnType.Name()] = columnType.
	}

	log.Println(rows.ColumnTypes())
	return nil, nil
/*
	var result []interface{}
	for rows.Next() {
		rowResult := reflect.New(reflect.TypeOf(where))

		columnNames, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		for columnName := range columnNames {


		}

	}

	return result, nil

 */
}

func SelectAll(db *sql.DB, table string, schemaType interface{}) ([]interface{}, error) {
	// TODO should we instead just validate that schemaType is zero and error if not?
	if !reflect.ValueOf(schemaType).IsZero() {
		return nil, errors.New("invalid schemaType: must be a zero-value struct")
	}
	return SelectAllWithOptions(db, table, schemaType, QueryOptions{})
}
