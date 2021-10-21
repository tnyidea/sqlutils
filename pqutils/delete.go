package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// DeleteOne will construct a where condition from the primarykey tags on v.  It will then
// perform a delete of the record in the specified table that matches the primary key.  If
// the delete fails, an error will be returned.
func DeleteOne(db *sql.DB, table string, v interface{}) (sql.Result, error) {
	err := checkKindStruct(v)
	if err != nil {
		return nil, err
	}

	sm := parseSchemaTypeValue(&v)

	// TODO: What about composite keys?
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

	return deleteAllWithOptions(db, table, v, where)
}

func DeleteAllWithOptions(db *sql.DB, table string, schemaType interface{}, where map[string]string) (sql.Result, error) {
	if where == nil {
		return nil, errors.New("invalid where condition: where must be non-nil.  Use UnsafeDeleteAll to delete all records")
	}

	return deleteAllWithOptions(db, table, schemaType, where)
}

// UnsafeDeleteAll deletes ALL RECORDS from the specified table. This is marked with the
// prefix Unsafe to remind the user that it is a destructive function and should be used carefully.
func UnsafeDeleteAll(db *sql.DB, table string) (sql.Result, error) {
	return deleteAllWithOptions(db, table, struct{}{}, nil)
}

func deleteAllWithOptions(db *sql.DB, table string, schemaType interface{}, where map[string]string) (sql.Result, error) {
	err := checkKindStruct(schemaType)
	if err != nil {
		return nil, err
	}

	stmt := `DELETE FROM ` + table +
		whereConditionString(schemaType, where)

	// Execute the Statement
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	return conn.ExecContext(ctx, stmt)
}
