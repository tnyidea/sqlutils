package test

import (
	"database/sql"
	"github.com/gbnyc26/sqlutils/pqutils"
	"log"
	"testing"
)

func TestUpdateOne(t *testing.T) {
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

	sqlResult, err := pqutils.UpdateOne(db, "test_table", &testType{
		Id:         4,
		FirstName:  "John",
		MiddleName: "H",
		LastName:   "Smith",
	})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(sqlResult)
}
