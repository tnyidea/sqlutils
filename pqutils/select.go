package pqutils

import (
	"context"
	"database/sql"
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
	err = unmarshalRow(row, result)
	if err != nil {
		return err
	}

	return nil
}

func SelectAllWithOptions(result *[]interface{}, db *sql.DB, table string,
	where interface{}, options QueryOptions) error {

	err := checkKindSlicePtr(result)
	if err != nil {
		return err
	}
	err = checkKindStruct(where)
	if err != nil {
		return err
	}

	// TODO we should check if this works
	//   hmmm nope.. we will probably pass in a nil slice
	structSqlTags := parseStructSqlTags(reflect.ValueOf(result).Elem())
	query := `SELECT ` + strings.Join(structSqlTags.columnNames, ", ") + `
		FROM ` + table +
		whereConditionString(where) +
		options.String()

	// Execute the Query
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()

	err = unmarshalRows(rows, result)
	if err != nil {
		return err
	}

	return nil
}

func SelectAll(result *[]interface{}, db *sql.DB, table string) error {
	return SelectAllWithOptions(result, db, table, struct{}{}, QueryOptions{})
}
