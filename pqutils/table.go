package pqutils

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"time"
)

func CreateTableFromType(db *sql.DB, table string, v interface{}) error {
	err := checkKindStructPtr(v)
	if err != nil {
		return err
	}

	rve := reflect.ValueOf(v).Elem()

	// Index the struct
	var columnNames []string
	columnTypes := make(map[string]reflect.Kind)
	keyColumns := make(map[string]string)
	for i := 0; i < rve.NumField(); i++ {
		field := rve.Type().Field(i)
		sqlTag := field.Tag.Get("sql")
		if sqlTag == "" {
			continue
		}
		tokens := strings.Split(sqlTag, ",")
		columnName := tokens[0]
		columnNames = append(columnNames, columnName)
		columnTypes[columnName] = field.Type.Kind()

		if len(tokens) > 1 {
			keyType := strings.Join(tokens[1:], ":")
			switch keyType {
			case "primarykey":
				fallthrough
			case "primarykey:serial":
				fallthrough
			case "unique":
				keyColumns[columnName] = keyType
			}
		}
	}

	// Build column  definitions
	var columnDefinitions []string
	for _, columnName := range columnNames {
		var columnDefinition string
		columnType := columnTypes[columnName]
		switch columnType {
		case reflect.Bool:
			// Booleans are likely not key values so skip keyColumns switch
			columnDefinition = columnName + " boolean default false"

		case reflect.Float64:
			//not sure what to do here
			break

		case reflect.Int:
			fallthrough

		case reflect.Int32:
			switch keyColumns[columnName] {
			case "primarykey":
				columnDefinition = columnName + " integer primary key not null"
			case "primarykey:serial":
				columnDefinition = columnName + " serial primary key not null"
			case "unique":
				columnDefinition = columnName + " integer unique not null"
			default:
				columnDefinition = columnName + " integer default 0"
			}

		case reflect.Int64:
			switch keyColumns[columnName] {
			case "primarykey":
				columnDefinition = columnName + " bigint primary key not null"
			case "primarykey:serial":
				columnDefinition = columnName + " serial primary key not null"
			case "unique":
				columnDefinition = columnName + " bigint unique not null"
			default:
				columnDefinition = columnName + " bigint default 0"
			}

		case reflect.String:
			switch keyColumns[columnName] {
			case "primarykey":
				columnDefinition = columnName + " varchar primary key not null"
			case "primarykey:serial":
				// Not a valid case for String types
				break
			case "unique":
				columnDefinition = columnName + " varchar unique not null"
			default:
				columnDefinition = columnName + " varchar default ''"
			}

		case reflect.TypeOf(time.Time{}).Kind():
			switch keyColumns[columnName] {
			case "primarykey":
				columnDefinition = columnName + " timestamptz primary key not null"
			case "primarykey:serial":
				// Not a valid case for time.Time types
				break
			case "unique":
				columnDefinition = columnName + " timestamptz unique not null"
			default:
				columnDefinition = columnName + " timestamptz  default '0001-01-01T00:00:00Z'"
			}

		case reflect.Slice:
			// Slices are likely not key values so skip keyColumns switch
			// TODO figure out slice type first then build case statement
			//  but for now just make a varchar[]
			columnDefinition = columnName + " varchar[] default '{}'"
		}

		if columnDefinition != "" {
			columnDefinitions = append(columnDefinitions, columnDefinition)
		}
	}

	// Build create statement
	createStatement := "create table " + table + "( " +
		strings.Join(columnDefinitions, ", ") +
		");"

	// Execute the create statement
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	_, err = conn.ExecContext(ctx, createStatement)
	if err != nil {
		return err
	}

	return nil
}

func DropTable(db *sql.DB, table string) error {
	stmt := `DROP TABLE ` + table

	// Execute the Statement
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	_, err = conn.ExecContext(ctx, stmt)
	if err != nil {
		return err
	}

	return nil

}
