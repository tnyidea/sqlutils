package pqutils

import (
	"database/sql"
	"github.com/gbnyc26/configurator"
	"log"
	"testing"
)

func TestInsertOne(t *testing.T) {
	type testConfig struct {
		DbUrl string `env:"TEST_DB_URL"`
	}
	type testType struct {
		Id         int    `sql:"id,primarykey"`
		FirstName  string `sql:"first_name"`
		MiddleName string `sql:"middle_name"`
		LastName   string `sql:"last_name,unique"`
	}

	var config testConfig
	err := configurator.SetEnvFromFile("test.env")
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

	_ = DropTable(db, "test_table")

	var dataType testType
	err = CreateTableFromType(db, "test_table", &dataType)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	dataType = testType{
		FirstName:  "John",
		MiddleName: "H",
		LastName:   "Smith",
	}
	err = InsertOne(db, "test_table", dataType)
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
