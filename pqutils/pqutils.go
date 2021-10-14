package pqutils

import (
	"context"
	"database/sql"
	"errors"
	_ "github.com/lib/pq"
	"reflect"
	"strconv"
	"strings"
)

// Columns

func sqlTypeColumns(v interface{}) (cols []string) {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Struct {
		return nil
	}

	valtype := reflect.TypeOf(v)
	for i := 0; i < val.NumField(); i++ {
		field := valtype.Field(i)
		if col, ok := field.Tag.Lookup("sql"); ok {
			if col != "" {
				cols = append(cols, col)
			}
		}
	}

	return cols
}

func sqlTableColumns(db *sql.DB, table string) (cols []string, err error) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	query := `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1
        AND table_schema = current_user`
	rows, err := conn.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var v string
		err := rows.Scan(&v)
		if err != nil {
			return nil, err
		}
		cols = append(cols, v)
	}

	return cols, nil
}

// Marshal and Unmarshal

func marshalWhereCondition(v interface{}) (string, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return "", errors.New("invalid pointer type: Cannot marshal from non-pointer value")
	}

	return "", nil
}

func unmarshalRow(row *sql.Row, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("invalid pointer type: Cannot unmarshal row into non-pointer value")
	}

	var rowValue interface{}
	err := row.Scan(&rowValue)
	if err != nil {
		return err
	}

	return nil
}

func unmarshalRows(rows *sql.Rows, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("invalid pointer type: Cannot unmarshal row into non-slice value")
	}

	// check to make sure that passed v is also a slice

	for rows.Next() {
		var rowValue interface{}
		err := rows.Scan(&rowValue)
		if err != nil {
			return err
		}

		//v = append(v, rowValue)
	}

	return nil
}

// Queries

func SelectAll(db *sql.DB, v interface{}, table string) error {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	columns := sqlTypeColumns(v)
	query := `SELECT ` + strings.Join(columns, ", ") +
		`FROM ` + table

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}

	var r []interface{}
	err = unmarshalRows(rows, &r)
	if err != nil {
		return err
	}

	return nil
}

func SelectAllLimit(db *sql.DB, v interface{}, table string, limit int) error {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	columns := sqlTypeColumns(v)
	query := `SELECT ` + strings.Join(columns, ", ") + `
		FROM ` + table + `
        LIMIT ` + strconv.Itoa(limit)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}

	var r []interface{}
	err = unmarshalRows(rows, &r)
	if err != nil {
		return err
	}

	return nil
}

func SelectAllOffsetLimit(db *sql.DB, v interface{}, table string, offset int, limit int) error {
	return nil
}

func Count(db *sql.DB, table string) (int, error) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	query := `
        SELECT COUNT(*)
        FROM ` + table

	var count int
	row := conn.QueryRowContext(ctx, query)
	err = row.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}
