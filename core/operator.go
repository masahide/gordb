// go-rdb Operator
package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

type Operator func(reflect.Kind, Value, reflect.Kind, Value) (bool, error)

func (ss *Operator) UnmarshalJSON(data []byte) error {
	// Extract the string from data.
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("operator should be a string, got %s", data)
	}

	// The rest is equivalen to Operator.
	got, ok := map[string]Operator{
		">":  GreaterThan,
		">=": NotLessThan,
		"<":  LessThan,
		"<=": NotGreaterThan,
		"==": Equal,
		"!=": NotEqual,
	}[s]
	if !ok {
		return fmt.Errorf("invalid operator %q", s)
	}
	*ss = got
	return nil
}

var ErrDifferentType = errors.New("Different type.")
var ErrUnkownType = errors.New("Unkown type.")

func typeCheck(a, b Value) (aValue, bValue Value, Kind reflect.Kind, err error) {
	var bKind reflect.Kind
	Kind, aValue = VtoFS(a)
	bKind, bValue = VtoFS(b)
	if Kind != bKind {
		err = ErrDifferentType
	}
	return
}

func GreaterThan(akind reflect.Kind, a Value, bkind reflect.Kind, b Value) (bool, error) {
	if akind == bkind {
		switch akind {
		case reflect.Int64:
			return a.(int64) > b.(int64), nil
		case reflect.Float64:
			return a.(float64) > b.(float64), nil
		case reflect.Int:
			return a.(int) > b.(int), nil
		case reflect.String:
			return a.(string) > b.(string), nil
		}
	}
	aValue, bValue, kind, err := typeCheck(a, b)
	if err != nil {
		return false, err
	}
	switch kind {
	case reflect.Float64:
		return aValue.(float64) > bValue.(float64), nil
	case reflect.String:
		return aValue.(string) > bValue.(string), nil
	}
	return false, ErrUnkownType
}

func NotGreaterThan(akind reflect.Kind, a Value, bkind reflect.Kind, b Value) (bool, error) {
	if akind == bkind {
		switch akind {
		case reflect.Int64:
			return a.(int64) <= b.(int64), nil
		case reflect.Float64:
			return a.(float64) <= b.(float64), nil
		case reflect.Int:
			return a.(int) <= b.(int), nil
		case reflect.String:
			return a.(string) <= b.(string), nil
		}
	}
	aValue, bValue, kind, err := typeCheck(a, b)
	if err != nil {
		return false, err
	}
	switch kind {
	case reflect.Float64:
		return aValue.(float64) <= bValue.(float64), nil
	case reflect.String:
		return aValue.(string) <= bValue.(string), nil
	}
	return false, ErrUnkownType
}

func LessThan(akind reflect.Kind, a Value, bkind reflect.Kind, b Value) (bool, error) {
	if akind == bkind {
		switch akind {
		case reflect.Int64:
			return a.(int64) < b.(int64), nil
		case reflect.Float64:
			return a.(float64) < b.(float64), nil
		case reflect.Int:
			return a.(int) < b.(int), nil
		case reflect.String:
			return a.(string) < b.(string), nil
		}
	}
	aValue, bValue, kind, err := typeCheck(a, b)
	if err != nil {
		return false, err
	}
	switch kind {
	case reflect.Float64:
		return aValue.(float64) < bValue.(float64), nil
	case reflect.String:
		return aValue.(string) < bValue.(string), nil
	}
	return false, ErrUnkownType
}

func NotLessThan(akind reflect.Kind, a Value, bkind reflect.Kind, b Value) (bool, error) {
	if akind == bkind {
		switch akind {
		case reflect.Int64:
			return a.(int64) >= b.(int64), nil
		case reflect.Float64:
			return a.(float64) >= b.(float64), nil
		case reflect.Int:
			return a.(int) >= b.(int), nil
		case reflect.String:
			return a.(string) >= b.(string), nil
		}
	}
	aValue, bValue, kind, err := typeCheck(a, b)
	if err != nil {
		return false, err
	}
	switch kind {
	case reflect.Float64:
		return aValue.(float64) >= bValue.(float64), nil
	case reflect.String:
		return aValue.(string) >= bValue.(string), nil
	}
	return false, ErrUnkownType
}

func Equal(akind reflect.Kind, a Value, bkind reflect.Kind, b Value) (bool, error) {
	if akind == bkind {
		return a == b, nil
	}
	aValue, bValue, kind, err := typeCheck(a, b)
	if err != nil {
		return false, err
	}
	switch kind {
	case reflect.Float64:
		return aValue.(float64) == bValue.(float64), nil
	case reflect.String:
		return aValue.(string) == bValue.(string), nil
	}
	return false, ErrUnkownType
}

func NotEqual(akind reflect.Kind, a Value, bkind reflect.Kind, b Value) (bool, error) {
	if akind == bkind {
		return a != b, nil
	}
	aValue, bValue, kind, err := typeCheck(a, b)
	if err != nil {
		return false, err
	}
	switch kind {
	case reflect.Float64:
		return aValue.(float64) != bValue.(float64), nil
	case reflect.String:
		return aValue.(string) != bValue.(string), nil
	}
	return false, ErrUnkownType
}
