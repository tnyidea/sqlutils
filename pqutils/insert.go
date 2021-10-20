package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"github.com/lib/pq"
	"log"
	"strings"
)

func InsertOne(db *sql.DB, table string, v interface{}) error {
	// Create the connection
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	err = insertOne(conn, ctx, table, v)
	if err != nil {
		return err
	}

	return nil
}

func InsertAll(db *sql.DB, table string, v []interface{}) error {
	// Create the connection
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	for _, value := range v {
		err := insertOne(conn, ctx, table, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func BulkInsert(db *sql.DB, table string, v []interface{}) error {
	if v == nil {
		return errors.New("invalid slice: nil value recived for v. Nothing to insert")
	}

	schemaType := v[0]
	sm := parseSchemaTypeValue(&schemaType)

	// Create the connection
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	count := len(v)
	log.Println("--- Begin Bulk Insert for", count, "Items ---")

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	var stmtColumns []string
	// TODO make a util that reduces list of columnNames to strip those that use default
	for _, columnName := range sm.columnNames {
		// We need to eliminate any key columns that are primarykey:serial
		// so the database can default these values
		// TODO perhaps a more robust approach is to assert in the type to use default?
		if sm.columnKeyTypeMap[columnName] == "primarykey:serial" {
			continue
		}
		stmtColumns = append(stmtColumns, columnName)
	}
	log.Println(stmtColumns)

	// Prepare the Bulk Insert
	stmt, err := tx.Prepare(pq.CopyIn(table, stmtColumns...))
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for i, value := range v {
		sm := parseSchemaTypeValue(value)
		var stmtValues []interface{}
		for _, columnName := range stmtColumns {
			stmtValues = append(stmtValues, sm.fieldNameValueMap[sm.columnNameFieldNameMap[columnName]])
		}
		log.Println("| Adding Value", i+1, "of", count)
		_, err = stmt.ExecContext(ctx, stmtValues...)
		if err != nil {
			log.Println(err)
			_ = tx.Rollback()
			return err
		}
	}

	// Execute the Bulk Insert
	if _, err = stmt.ExecContext(ctx); err != nil {
		log.Printf("error while copying data: %s\n", err)
		_ = tx.Rollback()
		return err
	}
	if err = stmt.Close(); err != nil {
		log.Printf("error during stmt.Close(): %s\n", err)
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	log.Println("--- Bulk Insert Complete ---")

	return nil
}

// Helpers

func insertOne(conn *sql.Conn, ctx context.Context, table string, v interface{}) error {
	err := checkKindStruct(v)
	if err != nil {
		return err
	}

	sm := parseSchemaTypeValue(&v)

	var stmtColumns []string
	for _, columnName := range sm.columnNames {
		// We need to eliminate any key columns that are primarykey:serial
		// so the database can default these values
		// TODO perhaps a more robust approach is to assert in the type to use default?
		if sm.columnKeyTypeMap[columnName] == "primarykey:serial" {
			continue
		}
		stmtColumns = append(stmtColumns, columnName)
	}

	// TODO... consider making this a standard parameterized exec
	var stmtValues []string
	for _, columnName := range stmtColumns {
		fieldName := sm.columnNameFieldNameMap[columnName]
		stmtValues = append(stmtValues, "'"+sm.fieldNameStringValueMap[fieldName]+"'")
	}

	stmt := `INSERT INTO ` + table + ` ` +
		`(` + strings.Join(stmtColumns, ", ") + `) ` +
		`VALUES (` + strings.Join(stmtValues, ", ") + `)`

	// Execute the Statement
	_, err = conn.ExecContext(ctx, stmt)
	if err != nil {
		return err
	}

	return nil
}
