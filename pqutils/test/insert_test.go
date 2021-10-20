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

	dataType := testType{
		FirstName:  "John",
		MiddleName: "H",
		LastName:   "Smith",
	}
	err = pqutils.InsertOne(db, "test_table", dataType)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}
