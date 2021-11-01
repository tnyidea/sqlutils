package test

import (
	"context"
	"database/sql"
	"github.com/gbnyc26/sqlutils/pqutils"
	"log"
	"reflect"
	"sort"
	"testing"
)

func TestCreateTableFromType(t *testing.T) {
	config, err := configureTest()
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	db, err := sql.Open("postgres", config.DbUrl)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	err = pqutils.CreateTableFromType(db, "test_table", &testType{})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	tableColumnNames, err := testGetTableColumns(db, "test_table")
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	sort.Strings(tableColumnNames)

	typeColumnNames, err := pqutils.GetSchemaColumnNames(&testType{})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	sort.Strings(typeColumnNames)

	if !reflect.DeepEqual(tableColumnNames, typeColumnNames) {
		log.Println("error: created table columns do not match type columns")
		log.Println("tableColumns: ", tableColumnNames)
		log.Println("typeColumns", typeColumnNames)
		t.FailNow()
	}

}

func TestDropTable(t *testing.T) {
	config, err := configureTest()
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	db, err := sql.Open("postgres", config.DbUrl)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	err = pqutils.DropTable(db, "test_table")
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}

// Helpers

func testGetTableColumns(db *sql.DB, table string) (cols []string, err error) {
	query := `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1`
	//AND table_schema = current_user`

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	rows, err := conn.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var v string
		err := rows.Scan(&v)
		if err != nil {
			return nil, err
		}
		cols = append(cols, v)
	}

	return cols, nil
}
