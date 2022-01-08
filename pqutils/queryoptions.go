package pqutils

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const OrderAscending = "ASC"
const OrderDescending = "DESC"

type QueryOptions struct {
	OrderBy []string
	Limit   int
	Offset  int
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
	for _, orderBy := range options.OrderBy {
		// orderBy can have the struct field name or the json fieldName with the order value
		// in colon delimited format
		//
		// Examples:  FirstName:asc  json:firstName:DESC  FirstName
		//
		// If json is not specified, assume struct field name.  The value of orderValue can either be
		// asc (ascending) or desc (descending) and we can ignore case.  If no orderValue is present assume asc
		//
		// Split orderBy into tokens, and normalize format to <StructField>:<OrderValue>
		// If len 1: parse it as the struct field name, and add ":asc" to the orderBy string
		// If len 2: check if first token is json:; If json, then translate second token to struct field and add ":asc"
		//           If NOT, then we have <StructField>:<OrderValue> format
		// If len 3: Check if first token is json:; If json, then translate first token to struct field name and keep
		//           the third token as the order value
		//           If NOT, then this is an error
		//
		// After constructing a normalized format, tokenize and validate

		var orderByString string
		tokens := strings.Split(orderBy, ":")
		switch len(tokens) {
		case 1:
			orderByString = orderBy + ":" + OrderAscending
		case 2:
			if tokens[0] == "json:" {
				orderByString = stm.jsonNameFieldNameMap[tokens[1]] + ":" + OrderAscending
			} else {
				orderByString = orderBy
			}
		case 3:
			if tokens[0] == "json:" {
				orderByString = stm.jsonNameFieldNameMap[tokens[1]] + ":" + strings.ToUpper(tokens[2])
				break
			}
			// Just fall through to error if first token is not json:
			fallthrough
		default:
			return "", errors.New("invalid format for QueryOptions.OrderBy: " + orderBy)
		}

		// We should now have <StructField>:<OrderValue>, so validate and process
		tokens = strings.Split(orderByString, ":")
		if _, ok := scm.fieldNameColumnNameMap[tokens[0]]; !ok {
			return "", errors.New("invalid fieldName for QueryOptions.OrderBy: " + orderBy)
		}
		if tokens[1] != OrderAscending && tokens[1] != OrderDescending {
			return "", errors.New("invalid ordering for QueryOptions.OrderBy. Must be 'asc' or 'desc': " + orderBy)
		}

		columnName := scm.fieldNameColumnNameMap[tokens[0]]
		orderValue := tokens[1]
		optionsString += ` ORDER BY ` + columnName + " " + orderValue

		// TODO For first version we will only loop once to take the first value, then upgrade for multi column
		break
	}
	if options.Limit > 0 {
		optionsString += ` LIMIT ` + strconv.Itoa(options.Limit)
	}
	if options.Offset > 0 {
		optionsString += ` OFFSET ` + strconv.Itoa(options.Offset)
	}

	return conditionString + optionsString, nil
}
