package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// UpdateOne will construct a where condition from the primarykey tags on v.  It will then
// perform an update of the record in the specified table that matches the primary key, using
// the ENTIRE value of v.  If the update fails, an error will be returned.
func UpdateOne(db *sql.DB, table string, v interface{}) (sql.Result, error) {
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
			where[fieldName] = stm.fieldNameValueMap[fieldName]
		}
	}

	return updateAllWithOptions(db, table, v, nil, where)
}

func UpdateAllWithOptions(db *sql.DB, table string, v interface{}, mask []string, where map[string]interface{}) (sql.Result, error) {
	if where == nil {
		return nil, errors.New("invalid where condition: where must be non-nil. Use UnsafeUpdateAll to update all records")
	}

	return updateAllWithOptions(db, table, v, mask, where)
}

// UnsafeUpdateAll updates ALL RECORDS in the specified table with the masked value v. This is
// marked with the prefix Unsafe to remind the user that it is a destructive function and should
// be used carefully.
func UnsafeUpdateAll(db *sql.DB, table string, v interface{}, mask []string) (sql.Result, error) {
	return updateAllWithOptions(db, table, v, mask, nil)
}

func updateAllWithOptions(db *sql.DB, table string, v interface{}, mask []string, where map[string]interface{}) (sql.Result, error) {
	// Assumption: v is a pointer to a struct

	// TODO need to come up with a mask or something to decide which values actually get updated
	//   OR does schemaType need to be a struct of pointers?

	scm, err := parseSchemaMetadata(v)
	if err != nil {
		return nil, err
	}
	stm, err := parseStructMetadata(v)
	if err != nil {
		return nil, err
	}

	// TODO... consider making this a standard parameterized exec
	stmtColumns := scm.columnNames

	// TODO Implement mask
	var stmtValues []string
	for _, columnName := range stmtColumns {
		fieldName := scm.columnNameFieldNameMap[columnName]
		stmtValues = append(stmtValues, "'"+fmt.Sprintf("%v", stm.fieldNameValueMap[fieldName])+"'")
	}

	condition, err := queryConditionString(v, where, QueryOptions{})
	if err != nil {
		return nil, err
	}
	stmt := `UPDATE ` + table + ` ` +
		`SET (` + strings.Join(stmtColumns, ", ") + `) = ` +
		`(` + strings.Join(stmtValues, ", ") + `) ` +
		condition

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
