package pqutils

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"reflect"
	"strings"
)

func SelectAll(result *[]interface{}, db *sql.DB, table string) error {
	return SelectAllWithOptions(result, db, table, struct{}{}, QueryOptions{})
}

func SelectAllWithOptions(result *[]interface{}, db *sql.DB, table string,
	where interface{}, options QueryOptions) error {

	err := checkKindStructPtr(result)
	if err != nil {
		return err
	}
	err = checkKindStructPtr(where)
	if err != nil {
		return err
	}

	// TODO we should check if this works
	//   hmmm nope.. we will probably pass in a nil slice
	columns := parseSqlTagValues(reflect.ValueOf(result).Elem())
	query := `SELECT ` + strings.Join(columns, ", ") + `
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

func SelectOne(result interface{}, db *sql.DB, table string, where interface{}) error {
	err := checkKindStructPtr(result)
	if err != nil {
		return err
	}
	err = checkKindStructPtr(where)
	if err != nil {
		return err
	}

	columns := parseSqlTagValues(where)
	query := `SELECT ` + strings.Join(columns, ", ") +
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

// Helpers

func fieldStringValueMap(v interface{}) ([]string, map[string]string) {
	// assume v is a pointer to a struct
	// caller must first use checkKindPtrToStruct

	rve := reflect.ValueOf(v).Elem()

	var fieldNames []string
	fieldValues := make(map[string]string)
	for i := 0; i < rve.NumField(); i++ {
		field := rve.Type().Field(i)
		fieldNames = append(fieldNames, field.Name)
		fieldValues[field.Name] = fmt.Sprintf("%v", rve.Field(i))
	}

	return fieldNames, fieldValues
}

func scanDestination(v interface{}) []interface{} {
	// assume v is a pointer to a struct
	// caller must first use checkKindPtrToStruct

	fieldNames, _ := fieldStringValueMap(v)
	rve := reflect.ValueOf(v).Elem()

	var sd []interface{}
	for _, fieldName := range fieldNames {
		field := rve.FieldByName(fieldName)
		if field.Kind() == reflect.Slice {
			sd = append(sd, pq.Array(field.Addr().Interface()))
		} else {
			sd = append(sd, field.Addr().Interface())
		}
	}

	return sd
}

func unmarshalRow(row *sql.Row, v interface{}) error {
	// assume v is a pointer to a struct
	// caller must first use checkKindPtrToStruct

	// Our assumption is that the order for the query
	// matches the natural order of the struct fields
	// and that each db column has a default value that
	// matches Go's default type rules (so we don't need
	// to use Go's Sql Null types)

	sd := scanDestination(v)
	err := row.Scan(sd...)
	if err != nil {
		return err
	}

	return nil
}

func unmarshalRows(rows *sql.Rows, v *[]interface{}) error {
	// assume v is a pointer to a slice of struct
	// caller must first use checkKindPtrToSliceOfStruct

	result := *v
	for rows.Next() {
		newRowType := reflect.New(reflect.TypeOf(reflect.Indirect(reflect.ValueOf(v)).Elem()))
		sd := scanDestination(newRowType)
		err := rows.Scan(sd...)
		if err != nil {
			return err
		}

		result = append(result, newRowType)
	}

	v = &result
	return nil
}
