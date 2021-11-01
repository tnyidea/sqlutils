package test

import (
	"database/sql"
	"github.com/gbnyc26/sqlutils/pqutils"
	"log"
	"testing"
)

func TestDeleteOne(t *testing.T) {
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

	sqlResult, err := pqutils.DeleteOne(db, "test_table", &testType{
		Id: 4,
	})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(sqlResult)
}
