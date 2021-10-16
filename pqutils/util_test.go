package pqutils

import (
	"context"
	"database/sql"
	"github.com/gbnyc26/configurator"
	"log"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type TestUtilConfig struct {
	DbUrl string `env:"TEST_DB_URL"`
}

func TestCreateTableFromType(t *testing.T) {
	var config TestUtilConfig
	err := configurator.SetEnvFromFile("util_test.env")
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	err = configurator.ParseEnvConfig(&config)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	testType := struct {
		Id         int    `sql:"id,primarykey"`
		FirstName  string `sql:"first_name"`
		MiddleName string `sql:"middle_name"`
		LastName   string `sql:"last_name,unique"`
	}{}

	db, err := sql.Open("postgres", config.DbUrl)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	err = CreateTableFromType(db, "test_table", &testType)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	tableColumns, err := testGetTableColumns(db, "test_table")
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	sort.Strings(tableColumns)

	sqlTags := parseSqlTagValues(testType)
	var typeColumns []string
	for _, tag := range sqlTags {
		tokens := strings.Split(tag, ",")
		typeColumns = append(typeColumns, tokens[0])
	}
	sort.Strings(typeColumns)

	if !reflect.DeepEqual(tableColumns, typeColumns) {
		log.Println("error: created table columns do not match type columns")
		log.Println("tableColumns: ", tableColumns)
		log.Println("typeColumns", typeColumns)
		t.FailNow()
	}

}

func TestDropTable(t *testing.T) {
	var config TestUtilConfig
	err := configurator.SetEnvFromFile("util_test.env")
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	err = configurator.ParseEnvConfig(&config)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	db, err := sql.Open("postgres", config.DbUrl)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	err = DropTable(db, "test_table")
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
