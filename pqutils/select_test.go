package pqutils

import (
	"database/sql"
	"encoding/json"
	"github.com/gbnyc26/configurator"
	"log"
	"testing"
)

type TestType struct {
	Id         int    `json:"id" sql:"id,primarykey,serial"`
	FirstName  string `json:"firstName" sql:"first_name"`
	MiddleName string `json:"middleName" sql:"middle_name"`
	LastName   string `json:"lastName" sql:"last_name,unique"`
}

func (p *TestType) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

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

	result, err := SelectAll(db, "test_table", TestType{})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(result)
	for _, v := range result {
		w := v.(TestType)
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

	result, err := SelectOne(db, "test_table", TestType{}, TestType{FirstName: "John"})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(result)
	w := result.(TestType)
	log.Println(&w)
}
