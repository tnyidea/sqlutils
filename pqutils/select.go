package pqutils

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

func SelectOne(db *sql.DB, table string, schemaType interface{}, where interface{}) (interface{}, error) {
	result, err := SelectAllWithOptions(db, table, schemaType, where, QueryOptions{Limit: 1})
	if err != nil {
		return nil, err
	}

	if len(result) != 1 {
		return reflect.New(reflect.ValueOf(schemaType).Type()).Elem().Interface(), nil
	}

	return result[0], nil
}

func SelectAllWithOptions(db *sql.DB, table string,
	schemaType interface{}, where interface{}, options QueryOptions) ([]interface{}, error) {

	//err := checkKindSlicePtr(result)
	//if err != nil {
	//	return err
	//}
	err := checkKindStruct(where)
	if err != nil {
		return nil, err
	}

	structSqlTags := parseStructSqlTags(&schemaType)
	query := `SELECT ` + strings.Join(structSqlTags.columnNames, ", ") + `
		FROM ` + table +
		whereConditionString(where) +
		options.String()

	// Execute the Query
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	// Gather column and struct information
	sm := parseStructSqlTags(&schemaType)
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Collect the results
	var result []interface{}
	for rows.Next() {
		// Scan the row result
		// TODO KEEP THIS HERE -- maybe we don't need to reuse this as we build
		//var sd []interface{}
		//for range columnTypes {
		//	// TODO add code for the array test case when we get an array back from postgres
		//	// sd = append(sd, pq.Array(field.Addr().Interface()))
		//	var v interface{}
		//	sd = append(sd, &v)
		//}
		//err := rows.Scan(sd...)
		//if err != nil {
		//    return nil, err
		//}
		//
		//rowResult := reflect.New(reflect.ValueOf(schemaType).Type())
		//log.Println(rowResult.Elem())
		//for i, columnType := range columnTypes {
		//    columnName := columnType.Name()
		//	log.Println(columnName)
		//    columnTypeName := columnType.DatabaseTypeName()
		//    switch columnTypeName {
		//    case "INT4":
		//        log.Println(*sd[i].(*interface{}))
		//		rowResult.Elem().FieldByName(columnFieldMap[columnName]).SetInt((*sd[i].(*interface{})).(int64))
		//    case "VARCHAR":
		//		log.Println(*sd[i].(*interface{}))
		//		rowResult.Elem().FieldByName(columnFieldMap[columnName]).SetString((*sd[i].(*interface{})).(string))
		//    default:
		//        return nil, errors.New("scan error: unhandled type: " + columnTypeName)
		//    }
		//}
		rowResult, err := unmarshalRowsResult(rows, columnTypes, schemaType, sm)
		if err != nil {
			return nil, err
		}
		result = append(result, rowResult)
	}

	return result, nil
}

func SelectAll(db *sql.DB, table string, schemaType interface{}) ([]interface{}, error) {
	// TODO should we instead just validate that schemaType is zero and error if not?
	if !reflect.ValueOf(schemaType).IsZero() {
		return nil, errors.New("invalid schemaType: must be a zero-value struct")
	}
	return SelectAllWithOptions(db, table, schemaType, struct{}{}, QueryOptions{})
}
