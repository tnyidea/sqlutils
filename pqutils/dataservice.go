package pqutils

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/lib/pq"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type DataService struct {
	Db *sql.DB
}

// Article on connection pooling
// http://go-database-sql.org/connection-pool.html
func New(url string) (r DataService, err error) {
	// Open Connection
	db, err := sql.Open("postgres", url)
	if err != nil {
		return DataService{}, err
	}

	//r.SetMaxIdleConns(0)
	//r.SetConnMaxLifetime(time.Duration(1) * time.Minute)

	return DataService{
		Db: db,
	}, nil
}

func (d *DataService) Close() {
	_ = d.Db.Close()
}

// Columns
func SqlColumnsForValue(v interface{}, filterCols ...string) (cols []string) {
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
			if col != "" && !omitColIndex[col] {
				cols = append(cols, col)
			}
		}
	}

	return cols
}

func (d *DataService) SqlColumnsForTable(table string) (cols []string, err error) {
	ctx := context.Background()
	conn, err := d.Db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	query := `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1
        AND table_schema = current_user`
	rows, err := conn.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var v string
		err := rows.Scan(&v)
		if err != nil {
			return nil, err
		}
		cols = append(cols, v)
	}

	return cols, nil
}

func (d *DataService) ValidSqlColumnsForValue(v interface{}, table string, filterCols ...string) (bool, error) {
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

func SqlScanDestination(v interface{}) (r []interface{}) {
	val := reflect.ValueOf(v).Elem()
	if val.Kind() != reflect.Struct {
		return nil
	}

	valptr := reflect.ValueOf(v)
	valtype := val.Type()
	for i := 0; i < valtype.NumField(); i++ {
		field := valtype.Field(i)
		if col, ok := field.Tag.Lookup("sql"); ok {
			if col == "id" || col == "created_at" || col == "updated_at" {
				r = append(r, valptr.Elem().Field(i).Addr().Interface())
				continue
			}
			if col != "" {
				if valptr.Elem().Field(i).Kind() == reflect.Slice {
					r = append(r, pq.Array(valptr.Elem().Field(i).Addr().Interface()))
				} else {
					r = append(r, valptr.Elem().Field(i).Addr().Interface())
				}
			}
		}
	}

	if len(r) == 0 {
		return nil
	}

	return r
}

// Table Operations
func (d *DataService) CreateTableFromQuery(newTable string, sourceQuery string) (err error) {
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

func (d *DataService) ReplaceTable(oldTable string, newTable string) (err error) {
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

func (d *DataService) DeleteTable(table string) (err error) {
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

// Query Options
type QueryOptions struct {
	FilterColumn string
	FilterValue  string
	SortColumn   string
	SortOrder    string
	Count        int
	RangeStart   int
	RangeEnd     int
}

func (p *QueryOptions) IsZero() bool {
	return reflect.DeepEqual(*p, QueryOptions{})
}

func (p *QueryOptions) Bytes() []byte {
	b, _ := json.Marshal(p)
	return b
}

func (p *QueryOptions) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

// Standard Queries
func (d *DataService) BulkCreate(v []interface{}, table string, filterCols ...string) (err error) {
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

func (d *DataService) Count(table string, where string, args ...interface{}) (r int, err error) {
	ctx := context.Background()
	conn, err := d.Db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	query := "SELECT count(id) FROM " + table
	if where != "" {
		query += " WHERE " + where
	}
	row := conn.QueryRowContext(ctx, query, args...)

	err = row.Scan(&r)
	if err != nil {
		return 0, err
	}

	return r, nil
}

// TODO a performance test to see if the call to scan the tags adds latency
func SqlFind(table string, columns string, where string) string {
	return SqlFindAll(table, columns, where, QueryOptions{})
}

func SqlFindAll(table string, columns string, where string, q QueryOptions) string {
	return `
        SELECT id, ` + columns + `
        FROM ` + table +
		SqlQueryOptions(where, q)
}

func SqlQueryOptions(where string, q QueryOptions) (r string) {
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

func (d *DataService) Delete(table string, where string, args ...interface{}) (err error) {
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

func (d *DataService) DeleteAll(table string) (err error) {
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
