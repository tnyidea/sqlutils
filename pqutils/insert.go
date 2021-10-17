package pqutils

import (
	"context"
	"database/sql"
	"strings"
)

func InsertOne(db *sql.DB, table string, v interface{}) error {
	err := checkKindStruct(v)
	if err != nil {
		return err
	}

	structSqlTags := parseStructSqlTags(&v)
	structFields := parseStructFields(&v)

	var stmtColumns = structSqlTags.columnNames
	var stmtValues []string
	for i, columnName := range structSqlTags.columnNames {
		if structSqlTags.columnKeyTypeMap[columnName] == "primarykey:serial" {
			// we need to eliminate any values that are primarykey:serial
			stmtColumns = append(stmtColumns[:i], stmtColumns[i+1:]...)
			continue
		}
		fieldName := structSqlTags.columnFieldMap[columnName]
		stmtValues = append(stmtValues, "'"+structFields.fieldStringValueMap[fieldName]+"'")
	}

	stmt := `INSERT INTO ` + table +
		`(` + strings.Join(structSqlTags.columnNames, ", ") + `) ` +
		`VALUES (` + strings.Join(stmtValues, ", ") + `)`

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

func InsertAll(db *sql.DB, table string, v []interface{}) error {
	for _, item := range v {
		err := InsertOne(db, table, item)
		if err != nil {
			return err
		}
	}

	return nil
}
