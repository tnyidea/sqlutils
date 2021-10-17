package pqutils

import (
	"database/sql"
	"github.com/gbnyc26/configurator"
	"log"
	"testing"
)

func TestSelectAll(t *testing.T) {
	type testConfig struct {
		DbUrl string `env:"TEST_DB_URL"`
	}
	type testType struct {
		Id         int    `sql:"id,primarykey,serial"`
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

	var results []testType
	err = SelectAllWithOptions(results, db, "test_table", testType{}, QueryOptions{})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println("RESULTS ARE:", results)

}
