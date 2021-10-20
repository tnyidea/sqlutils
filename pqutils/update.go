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
func UpdateOne(db *sql.DB, table string, v interface{}) error {
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
	result, err := SelectAllWithOptions(db, table, v, where, QueryOptions{})
	if err != nil {
		return err
	}
	if len(result) != 1 {
		return errors.New("invalid record for update: cannot find unique value for primary key values of v")
	}

	return updateAllWithOptions(db, table, v, where)

}

func UpdateAllWithOptions(db *sql.DB, table string, v interface{}, where map[string]string) error {
	// TODO need to come up with a mask or something to decide which values actually get updated
	//   OR does schemaType need to be a struct of pointers?
	return updateAllWithOptions(db, table, v, where)
}

func updateAllWithOptions(db *sql.DB, table string, v interface{}, where map[string]string) error {
	if where == nil {
		return errors.New("invalid where condition: where must be non-nil")
	}

	err := checkKindStruct(v)
	if err != nil {
		return err
	}

	sm := parseSchemaTypeValue(&v)

	// TODO... consider making this a standard parameterized exec
	stmtColumns := sm.columnNames
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
