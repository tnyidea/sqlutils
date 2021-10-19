package pqutils

import (
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

func whereConditionString(v interface{}) string {
	// TODO maybe this should be a map[FieldName]interface{}??
	// assume v is a pointer to a struct

	sm := parseStructSqlTags(&v)
	structFields := parseNonZeroStructFields(&v)
	fieldNames := structFields.fieldNames
	fieldValues := structFields.fieldStringValueMap

	var conditionValues []string
	for _, fieldName := range fieldNames {
		fieldValue := fieldValues[fieldName]
		if fieldValue != "" {
			conditionValues = append(conditionValues, sm.fieldColumnMap[fieldName]+"='"+fieldValue+"'")
		}
	}

	var s string
	if conditionValues != nil {
		s = " WHERE " + strings.Join(conditionValues, " AND ")
	}

	return s
}
