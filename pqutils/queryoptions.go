package pqutils

import (
	"fmt"
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

func whereConditionString(schema interface{}, where map[string]interface{}) (string, error) {
	// Assumption: schema is a pointer to a struct

	if where == nil {
		return "", nil
	}

	scm, err := parseSchemaMetadata(schema)
	if err != nil {
		return "", err
	}

	var conditionValues []string
	for fieldName, fieldValue := range where {
		if fieldValue != "" {
			columnName := scm.fieldNameColumnNameMap[fieldName]
			fieldKind := scm.columnNameFieldKindMap[columnName]
			var condition string
			if fieldKind == reflect.Int || fieldKind == reflect.Int64 {
				condition = columnName + "=" + fmt.Sprintf("%v", fieldValue)
			} else {
				condition = columnName + "='" + fmt.Sprintf("%v", fieldValue) + "'"
			}
			conditionValues = append(conditionValues, condition)
		}
	}

	if conditionValues != nil {
		return " WHERE " + strings.Join(conditionValues, " AND "), nil
	}

	return "", nil
}
