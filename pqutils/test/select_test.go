package test

import (
	"database/sql"
	"github.com/gbnyc26/sqlutils/pqutils"
	"log"
	"testing"
)

func TestSelectAll(t *testing.T) {
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

	rows, err := pqutils.SelectAll(db, "test_table", testType{})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	// Collect the results
	var result []testType
	for rows.Next() {
		rowResult, err := pqutils.UnmarshalRowsResult(rows, testType{})
		if err != nil {
			log.Println(err)
			t.FailNow()
		}
		result = append(result, rowResult.(testType))
	}
	log.Println(result)

}

func TestSelectOne(t *testing.T) {
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

	result, err := pqutils.SelectOne(db, "test_table", testType{Id: 2})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(result)
	w := result.(testType)
	log.Println(&w)
}

func TestSelectAllWithOptions(t *testing.T) {
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

	rows, err := pqutils.SelectAllWithOptions(db, "test_table", testType{}, map[string]string{"LastName": "Smith"}, pqutils.QueryOptions{})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	// Collect the results
	var result []testType
	for rows.Next() {
		rowResult, err := pqutils.UnmarshalRowsResult(rows, testType{})
		if err != nil {
			log.Println(err)
			t.FailNow()
		}
		result = append(result, rowResult.(testType))
	}
	log.Println(result)
}
