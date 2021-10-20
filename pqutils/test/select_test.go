package test

import (
	"database/sql"
	"github.com/gbnyc26/configurator"
	"github.com/gbnyc26/sqlutils/pqutils"
	"log"
	"testing"
)

func TestSelectAll(t *testing.T) {
	type testConfig struct {
		DbUrl string `env:"TEST_DB_URL"`
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

	result, err := pqutils.SelectAll(db, "test_table", testType{})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(result)
	for _, v := range result {
		w := v.(testType)
		log.Println(&w)
	}

}

func TestSelectOne(t *testing.T) {
	type testConfig struct {
		DbUrl string `env:"TEST_DB_URL"`
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

	result, err := pqutils.SelectOne(db, "test_table", testType{}, map[string]string{"LastName": "Smith"})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(result)
	w := result.(testType)
	log.Println(&w)
}
