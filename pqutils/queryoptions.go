package pqutils

import (
	"reflect"
	"strconv"
	"strings"
)

const OrderByOptionAscending = "ASC"
const OrderByOptionDescending = "DESC"

type QueryOptions struct {
	OrderByColumn string
	OrderByOption string
	Limit         int
	Offset        int
}

func (p *QueryOptions) String() string {
	var s string
	if p.OrderByColumn != "" {
		s += ` ORDER BY ` + p.OrderByColumn + " " + p.OrderByOption
	}
	if p.Limit > 0 {
		s += ` LIMIT ` + strconv.Itoa(p.Limit)
	}
	if p.Offset > 0 {
		s += ` OFFSET ` + strconv.Itoa(p.Offset)
	}

	return s
}

// Helpers

func whereConditionString(schemaType interface{}, where map[string]string) string {
	if where == nil {
		return ""
	}

	sm := parseSchemaTypeValue(&schemaType)

	var conditionValues []string
	for fieldName, fieldValue := range where {
		if fieldValue != "" {
			columnName := sm.fieldNameColumnNameMap[fieldName]
			fieldKind := sm.columnNameFieldKindMap[columnName]
			var condition string
			if fieldKind == reflect.Int || fieldKind == reflect.Int64 {
				condition = columnName + "=" + fieldValue
			} else {
				condition = columnName + "='" + fieldValue + "'"
			}
			conditionValues = append(conditionValues, condition)
		}
	}

	var s string
	if conditionValues != nil {
		s = " WHERE " + strings.Join(conditionValues, " AND ")
	}

	return s
}
