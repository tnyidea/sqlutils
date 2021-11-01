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

	result, err := pqutils.InsertOne(db, "test_table", &testType{
		FirstName:  "Jane",
		MiddleName: "H",
		LastName:   "Smith",
	})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(result)
}
