package pqutils

import (
	"database/sql"
	_ "github.com/lib/pq"
)

type Connection struct {
	Url string
	Db  *sql.DB
}

func NewConnection(url string) (Connection, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return Connection{}, err
	}

	return Connection{
		Url: url,
		Db:  db,
	}, nil
}
