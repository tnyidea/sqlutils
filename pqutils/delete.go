package pqutils

import (
	"context"
	"database/sql"
)

func DeleteAll(db *sql.DB, table string, schemaType interface{}) error {
	return DeleteAllWithOptions(db, table, schemaType, nil)
}

func DeleteAllWithOptions(db *sql.DB, table string, schemaType interface{}, where map[string]string) error {
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
