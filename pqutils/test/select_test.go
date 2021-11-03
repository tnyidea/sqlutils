package test

import (
	"database/sql"
	"github.com/tnyidea/sqlutils/pqutils"
	"log"
	"reflect"
	"testing"
)

func TestCountAll(t *testing.T) {
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

	count, err := pqutils.CountAll(db, "test_table")
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	if count == 0 {
		log.Println("expected: count to be non-zero. Received:", count)
	}

	log.Println("Count:", count)
}

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

	results, err := pqutils.SelectAll(db, "test_table", &testType{})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(results)

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

	testQuery := testType{Id: 2}
	result, err := pqutils.SelectOne(db, "test_table", &testQuery)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	if reflect.ValueOf(result).IsZero() {
		log.Println("expected: non-empty result for testQuery:", testQuery)
		t.FailNow()
	}

	log.Println(result)
	w := result.(testType)
	log.Println(&w)
}

func TestSelectOneNoResult(t *testing.T) {
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

	result, err := pqutils.SelectOne(db, "test_table", &testType{Id: 99})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(result)
	w := result.(testType)
	log.Println(&w)
}

func TestSelectOneMultipleResults(t *testing.T) {
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

	result, err := pqutils.SelectOne(db, "test_table", &testType{LastName: "Smith"})
	if err == nil {
		log.Println("expected: error condition for multiple results for SelectOne()")
		t.FailNow()
	}

	log.Println(err)
	log.Println(result)
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

	results, err := pqutils.SelectAllWithOptions(db, "test_table", &testType{}, map[string]interface{}{"FirstName": "John"}, pqutils.QueryOptions{})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	log.Println(results)
}

func TestSelectAllWithFullOptions(t *testing.T) {
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

	//lastName ASC 24 -1
	results, err := pqutils.SelectAllWithOptions(db, "test_table", &testType{},
		map[string]interface{}{"json:firstName": "Greg"}, pqutils.QueryOptions{
			OrderByField:  "json:id",
			OrderByOption: pqutils.OrderByOptionAscending,
			Limit:         24,
			Offset:        -1,
		})
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	log.Println(results)
}
