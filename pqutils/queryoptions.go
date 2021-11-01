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
	OrderByField  string
	OrderByOption string
	Limit         int
	Offset        int
}

func queryConditionString(schema interface{}, where map[string]interface{}, options QueryOptions) (string, error) {
	// Assumption: schema is a pointer to a struct

	if where == nil {
		return "", nil
	}

	scm, err := parseSchemaMetadata(schema)
	if err != nil {
		return "", err
	}
	stm, err := parseStructMetadata(schema)
	if err != nil {
		return "", err
	}

	var conditionString string
	var conditionValues []string
	for fieldName, fieldValue := range where {
		if fieldValue != "" {
			if strings.HasPrefix(fieldName, "json:") {
				fieldName = stm.jsonNameFieldNameMap[strings.TrimPrefix(fieldName, "json:")]
			}
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
		conditionString = "WHERE " + strings.Join(conditionValues, " AND ")
	}

	var optionsString string
	if options.OrderByField != "" {
		fieldName := options.OrderByField
		if strings.HasPrefix(fieldName, "json:") {
			fieldName = stm.jsonNameFieldNameMap[strings.TrimPrefix(fieldName, "json:")]
		}
		columnName := scm.fieldNameColumnNameMap[fieldName]
		optionsString += ` ORDER BY ` + columnName + " " + options.OrderByOption
	}
	if options.Limit > 0 {
		optionsString += ` LIMIT ` + strconv.Itoa(options.Limit)
	}
	if options.Offset > 0 {
		optionsString += ` OFFSET ` + strconv.Itoa(options.Offset)
	}

	return conditionString + optionsString, nil
}
