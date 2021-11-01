package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"log"
	"strings"
)

func InsertOne(db *sql.DB, table string, v interface{}) (interface{}, error) {
	// Create the connection
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	return insertOne(conn, ctx, table, v)
}

func InsertAll(db *sql.DB, table string, v []interface{}) ([]interface{}, []error) {
	// Create the connection
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, []error{err}
	}
	defer func() {
		_ = conn.Close()
	}()

	// TODO there is no provision for insertAll in sql.  Should we interfere with
	//   driver implementation and construct our own result?
	var results []interface{}
	var errs []error
	for _, value := range v {
		result, err := insertOne(conn, ctx, table, value)
		if err != nil {
			errs = append(errs, err)
		}
		results = append(results, result)
	}

	return results, errs
}

func BulkInsert(db *sql.DB, table string, v []interface{}) error {
	// Assumption: interface{} elements of v are pointers to structs
	if v == nil {
		return errors.New("invalid slice: nil value recived for v. Nothing to insert")
	}

	schema := v[0]
	scm, err := parseSchemaMetadata(schema)
	if err != nil {
		return err
	}

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
	for _, columnName := range scm.columnNames {
		// We need to eliminate any key columns that are primarykey:serial
		// so the database can default these values
		// TODO perhaps a more robust approach is to assert in the type to use default?
		if scm.columnKeyTypeMap[columnName] == "primarykey:serial" {
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
		stm, err := parseStructMetadata(value)
		if err != nil {
			return err
		}
		var stmtValues []interface{}
		for _, columnName := range stmtColumns {
			fieldName := scm.columnNameFieldNameMap[columnName]
			stmtValues = append(stmtValues, stm.fieldNameValueMap[fieldName])
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

func insertOne(conn *sql.Conn, ctx context.Context, table string, v interface{}) (interface{}, error) {
	// Assumption: v is a pointer to a struct

	scm, err := parseSchemaMetadata(v)
	if err != nil {
		return nil, err
	}
	stm, err := parseStructMetadata(v)
	if err != nil {
		return nil, err
	}

	var stmtColumns []string
	for _, columnName := range scm.columnNames {
		// We need to eliminate any key columns that are primarykey:serial
		// so the database can default these values
		// TODO perhaps a more robust approach is to assert in the type to use default?
		if scm.columnKeyTypeMap[columnName] == "primarykey:serial" {
			continue
		}
		stmtColumns = append(stmtColumns, columnName)
	}

	// TODO... consider making this a standard parameterized exec
	var stmtValues []string
	for _, columnName := range stmtColumns {
		fieldName := scm.columnNameFieldNameMap[columnName]
		stmtValues = append(stmtValues, "'"+fmt.Sprintf("%v", stm.fieldNameValueMap[fieldName])+"'")
	}

	// TODO Figure out how to get pointers to the key fields then construct the query
	//  to return the key fields.  Then augment v and return it??
	//  but for now we will just assume that v has an id field and we will return
	//  the created record as a struct

	stmt := `INSERT INTO ` + table + `
             (` + strings.Join(stmtColumns, ", ") + `)
		     VALUES (` + strings.Join(stmtValues, ", ") + `) ` +
		`RETURNING *`

	// Execute the Statement
	rows, err := conn.QueryContext(ctx, stmt)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var rowResult interface{}
	for rows.Next() {
		rowResult, err = unmarshalRowsResult(rows, v)
		if err != nil {
			return nil, err
		}
	}

	return rowResult, nil
}
