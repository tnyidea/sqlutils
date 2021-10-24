package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// UpdateOne will construct a where condition from the primarykey tags on v.  It will then
// perform an update of the record in the specified table that matches the primary key, using
// the ENTIRE value of v.  If the update fails, an error will be returned.
func UpdateOne(db *sql.DB, table string, v interface{}) (sql.Result, error) {
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

	return updateAllWithOptions(db, table, v, nil, where)
}

func UpdateAllWithOptions(db *sql.DB, table string, v interface{}, mask []string, where map[string]string) (sql.Result, error) {
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

func updateAllWithOptions(db *sql.DB, table string, v interface{}, mask []string, where map[string]string) (sql.Result, error) {
	// TODO need to come up with a mask or something to decide which values actually get updated
	//   OR does schemaType need to be a struct of pointers?

	err := checkKindStruct(v)
	if err != nil {
		return nil, err
	}

	sm := parseSchemaTypeValue(&v)

	// TODO... consider making this a standard parameterized exec
	stmtColumns := sm.columnNames

	// TODO Implement mask
	var stmtValues []string
	for _, columnName := range stmtColumns {
		fieldName := sm.columnNameFieldNameMap[columnName]
		stmtValues = append(stmtValues, "'"+sm.fieldNameStringValueMap[fieldName]+"'")
	}

	stmt := `UPDATE ` + table + ` ` +
		`SET (` + strings.Join(stmtColumns, ", ") + `) = ` +
		`(` + strings.Join(stmtValues, ", ") + `) ` +
		whereConditionString(v, where)

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
