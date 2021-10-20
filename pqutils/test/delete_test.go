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

	dataType := testType{
		Id: 1,
	}
	err = pqutils.DeleteOne(db, "test_table", dataType)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}
