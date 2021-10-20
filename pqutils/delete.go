package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// UpdateOne Assumes that v is the full record to be updated.  That is, UpdateOne will first check to see
// if v can be located using the primarykey on schema type.  If a record can be found, then the value of that
// record will be replaced with v in the database.
func DeleteOne(db *sql.DB, table string, v interface{}) error {
	err := checkKindStruct(v)
	if err != nil {
		return err
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
	rows, err := SelectAllWithOptions(db, table, v, where, QueryOptions{})
	if err != nil {
		return err
	}
	if !rows.Next() {
		return errors.New("invalid record for delete: cannot find unique value for primary key values of v")
	}

	return deleteAllWithOptions(db, table, v, where)
}

func DeleteAllWithOptions(db *sql.DB, table string, schemaType interface{}, where map[string]string) error {
	return deleteAllWithOptions(db, table, schemaType, where)
}
func deleteAllWithOptions(db *sql.DB, table string, schemaType interface{}, where map[string]string) error {
	if where == nil {
		return errors.New("invalid where condition: where must be non-nil")
	}

	err := checkKindStruct(schemaType)
	if err != nil {
		return err
	}

	stmt := `DELETE FROM ` + table +
		whereConditionString(schemaType, where)

	// Execute the Statement
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	_, err = conn.ExecContext(ctx, stmt)
	if err != nil {
		return err
	}

	return nil
}
