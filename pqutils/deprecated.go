package pqutils

/*
import (
	"context"
	"errors"
	"github.com/gbnyc26/sqlutils"
	"github.com/gbnyc26/typeutils"
	"github.com/lib/pq"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

func ValidSqlColumnsForValue(v interface{}, table string, filterCols ...string) (bool, error) {
	filterIndex := map[string]string{"include": "", "omit": ""}
	if filterCols != nil {
		for _, filter := range filterCols {
			tokens := strings.Split(filter, ":")
			if len(tokens) != 2 {
				return false, errors.New("invalid filter: " + filter)
			}
			filterIndex[tokens[0]] = tokens[1]
		}
	}

	// Value Columns
	valueCols := SqlColumnsForValue(v)
	valueColIndex := typeutils.IndexStringSlice(valueCols)
	includeFilter := strings.Split(filterIndex["include"], ",")
	for _, col := range includeFilter {
		if !valueColIndex[col] {
			valueCols = append(valueCols, col)
		}
	}

	omitColIndex := typeutils.IndexStringSlice(strings.Split(filterIndex["omit"], ","))
	var cols []string
	for _, col := range valueCols {
		if !omitColIndex[col] {
			cols = append(cols, col)
		}
	}
	valueCols = cols

	sort.Strings(valueCols)
	log.Println("Value Columns (Count", len(valueCols), "):", valueCols)

	// Table Columns
	tableCols, err := d.SqlColumnsForTable(table)
	if err != nil {
		return false, err
	}
	tableColIndex := typeutils.IndexStringSlice(tableCols)
	for _, col := range includeFilter {
		if !tableColIndex[col] {
			tableCols = append(tableCols, col)
		}
	}

	cols = nil
	for _, col := range tableCols {
		if !omitColIndex[col] {
			cols = append(cols, col)
		}
	}
	tableCols = cols

	sort.Strings(tableCols)
	log.Println("Table Columns (Count", len(tableCols), "):", tableCols)

	return reflect.DeepEqual(tableCols, valueCols), nil
}

// Marshal/Unmarshal Column Values

func SqlValueSource(v interface{}, filterCols ...string) (src []interface{}) {
	// we only need omit here, so going to assume that omit is only included in filterCols
	omitColIndex := make(map[string]bool)
	if filterCols != nil {
		tokens := strings.Split(filterCols[0], ":")
		if len(tokens) != 2 || tokens[0] != "omit" {
			log.Println("invalid filter: " + filterCols[0])
			return nil
		}
		omitColIndex = typeutils.IndexStringSlice(strings.Split(tokens[1], ","))
	}

	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Struct {
		return nil
	}

	valtype := reflect.TypeOf(v)
	for i := 0; i < val.NumField(); i++ {
		field := valtype.Field(i)
		if col, ok := field.Tag.Lookup("sql"); ok {
			if !omitColIndex[col] {
				if val.Field(i).Kind() == reflect.Slice {
					src = append(src, pq.Array(val.Field(i).Interface()))
				} else {
					src = append(src, val.Field(i).Interface())
				}
			}
		}
	}

	return src
}


// Table Operations

func CreateTableFromQuery(newTable string, sourceQuery string) (err error) {
	ctx := context.Background()
	conn, err := d.Db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	query := `
        CREATE TABLE ` + newTable + ` AS (` +
		sourceQuery +
		`);
        CREATE SEQUENCE ` + newTable + `_id_seq;
        SELECT setval('` + newTable + `_id_seq', (
           SELECT id
           FROM ` + newTable + `
           ORDER BY id DESC LIMIT 1
        ));
        ALTER TABLE ` + newTable + ` ALTER id SET NOT NULL;
        ALTER TABLE ` + newTable + ` ALTER id
           SET DEFAULT nextval('` + newTable + `_id_seq'::regclass);`
	_, err = conn.ExecContext(ctx, query)
	return err
}

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

func DeleteTable(table string) (err error) {
	ctx := context.Background()
	conn, err := d.Db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	_, err = conn.ExecContext(ctx, "DROP TABLE "+table)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "DROP SEQUENCE "+table+"_id_seq")
	return err
}

// Standard Queries

func BulkCreate(v []interface{}, table string, filterCols ...string) (err error) {
	ctx := context.Background()
	conn, err := d.Db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	count := len(v)
	log.Println("--- Begin Bulk Create for", count, "Items ---")

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	columns := SqlColumnsForValue(v[0], filterCols...)
	log.Println(columns)
	stmt, err := tx.Prepare(pq.CopyIn(table, columns...))
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	for i, value := range v {
		log.Println("| Adding Value", i+1, "of", count)
		_, err = stmt.ExecContext(ctx, SqlValueSource(value, filterCols...)...)
		if err != nil {
			log.Println(err)
			_ = tx.Rollback()
			return err
		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		log.Printf("error while copying data: %s\n", err)
		_ = tx.Rollback()
		return err
	}
	if err = stmt.Close(); err != nil {
		log.Printf("error during stmt.Close(): %s\n", err)
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	log.Println("--- Bulk Create Complete ---")

	return nil
}

func SqlFind(table string, columns string, where string) string {
	// TODO a performance test to see if the call to scan the tags adds latency
	return SqlFindAll(table, columns, where, sqlutils.QueryOptions{})
}

func SqlFindAll(table string, columns string, where string, q sqlutils.QueryOptions) string {
	return `
        SELECT id, ` + columns + `
        FROM ` + table +
		SqlQueryOptions(where, q)
}

func SqlQueryOptions(where string, q sqlutils.QueryOptions) (r string) {
	var tokens []string
	if where != "" {
		tokens = append(tokens, where)
	}
	if q.FilterColumn != "" {
		tokens = append(tokens, q.FilterColumn+" ILIKE '"+q.FilterValue+"%' ")
	}
	if len(tokens) != 0 {
		r = " WHERE " + strings.Join(tokens, " AND ")
	}

	if q.SortColumn != "" {
		r += " ORDER BY " + q.SortColumn + " " + q.SortOrder + " "
	}

	if q.Count != 0 && (q.RangeStart != 0 && q.RangeEnd != 0) {
		log.Println("invalid argument: cannot sepcify both Count and Range")
		return r
	}

	if q.Count != 0 {
		r += " LIMIT " + strconv.Itoa(q.Count)
	}

	if q.RangeStart != 0 && q.RangeEnd != 0 {
		limit := q.RangeEnd - q.RangeStart
		offset := q.RangeStart

		r += " LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset) + " "
	}

	return r
}

func Delete(table string, where string, args ...interface{}) (err error) {
	ctx := context.Background()
	conn, err := d.Db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	// Safeguard to prevent delete of entire table
	if where == "" {
		return errors.New("invalid argument: where clause cannot be undefined for DELETE")
	}
	query := "DELETE FROM " + table + " WHERE " + where
	_, err = conn.ExecContext(ctx, query, args...)
	return err
}

func DeleteAll(table string) (err error) {
	ctx := context.Background()
	conn, err := d.Db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	query := "DELETE FROM " + table
	_, err = conn.ExecContext(ctx, query)
	return err
}

*/
