package pqutils

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"time"
)

func CreateTableFromType(db *sql.DB, table string, schemaType interface{}) error {
	err := checkKindStruct(schemaType)
	if err != nil {
		return err
	}

	sm := parseSchemaTypeValue(&schemaType)
	/*
		rve := reflect.ValueOf(schemaType).Elem()

		// Index the struct
		// TODO Consider replacing with structMetadata
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
	*/

	// Build column  definitions
	var columnDefinitions []string
	for _, columnName := range sm.columnNames {
		var columnDefinition string
		fieldType := sm.columnNameFieldKindMap[columnName]
		switch fieldType {
		case reflect.Bool:
			// Booleans are likely not key values so skip keyColumns switch
			columnDefinition = columnName + " BOOLEAN DEFAULT FALSE"

		case reflect.Float64:
			//not sure what to do here
			break

		case reflect.Int:
			fallthrough

		case reflect.Int32:
			switch sm.columnKeyTypeMap[columnName] {
			case "primarykey":
				columnDefinition = columnName + " INTEGER PRIMARY KEY NOT NULL"
			case "primarykey:serial":
				columnDefinition = columnName + " SERIAL PRIMARY KEY NOT NULL"
			case "unique":
				columnDefinition = columnName + " INTEGER UNIQUE NOT NULL"
			default:
				columnDefinition = columnName + " INTEGER DEFAULT 0"
			}

		case reflect.Int64:
			switch sm.columnKeyTypeMap[columnName] {
			case "primarykey":
				columnDefinition = columnName + " BIGINT PRIMARY KEY NOT NULL"
			case "primarykey:serial":
				columnDefinition = columnName + " SERIAL PRIMARY KEY NOT NULL"
			case "unique":
				columnDefinition = columnName + " BIGINT UNIQUE not null"
			default:
				columnDefinition = columnName + " BIGINT DEFAULT 0"
			}

		case reflect.String:
			switch sm.columnKeyTypeMap[columnName] {
			case "primarykey":
				columnDefinition = columnName + " VARCHAR PRIMARY KEY NOT NULL"
			case "primarykey:serial":
				// Not a valid case for String types
				break
			case "unique":
				columnDefinition = columnName + " VARCHAR UNIQUE not null"
			default:
				columnDefinition = columnName + " VARCHAR DEFAULT ''"
			}

		case reflect.TypeOf(time.Time{}).Kind():
			switch sm.columnKeyTypeMap[columnName] {
			case "primarykey":
				columnDefinition = columnName + " TIMESTAMPTZ PRIMARY KEY NOT NULL"
			case "primarykey:serial":
				// Not a valid case for time.Time types
				break
			case "unique":
				columnDefinition = columnName + " TIMESTAMPTZ UNIQUE NOT NULL"
			default:
				columnDefinition = columnName + " TIMESTAMPTZ  DEFAULT '0001-01-01T00:00:00Z'"
			}

		case reflect.Slice:
			// Slices are likely not key values so skip keyColumns switch
			// TODO figure out slice type first then build case statement
			//  but for now just make a VARCHAR[]
			columnDefinition = columnName + " VARCHAR[] DEFAULT '{}'"
		}

		if columnDefinition != "" {
			columnDefinitions = append(columnDefinitions, columnDefinition)
		}
	}

	// Build create statement
	createStatement := "CREATE TABLE " + table + "( " +
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

// DEPRECATED -- Maybe keep?

/*
func ReplaceTable(oldTable string, newTable string) (err error) {
	ctx := context.Background()
	conn, err := d.Db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	query := `
        ALTER TABLE ` + oldTable + ` RENAME TO ` + oldTable + `_old;
        ALTER TABLE ` + newTable + ` RENAME TO ` + oldTable + `;
	    DROP TABLE ` + oldTable + `_old;`
	_, err = tx.ExecContext(ctx, query)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
*/
