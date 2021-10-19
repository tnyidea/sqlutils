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

func whereConditionString(schemaType interface{}, where map[string]string) string {
	// TODO maybe this should be a map[FieldName]interface{}??
	// assume v is a pointer to a struct

	sm := parseStructSqlTags(&schemaType)

	var conditionValues []string
	for fieldName, fieldValue := range where {
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
