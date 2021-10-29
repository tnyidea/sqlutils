package test

import (
	"database/sql"
	"github.com/gbnyc26/sqlutils/pqutils"
	"log"
	"testing"
)

func TestInsertOne(t *testing.T) {
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

	testValue := testType{
		FirstName:  "Jane",
		MiddleName: "H",
		LastName:   "Smith",
	}
	sqlResult, err := pqutils.InsertOne(db, "test_table", testValue)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(sqlResult)
}
