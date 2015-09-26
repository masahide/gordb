// go-rdb Operator
package core

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type Operator func(Value, Value) bool

func (ss *Operator) UnmarshalJSON(data []byte) error {
	// Extract the string from data.
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("operator should be a string, got %s", data)
	}

	// The rest is equivalen to Operator.
	got, ok := map[string]Operator{
		">":  GreaterThan,
		"<":  LessThan,
		"==": Equal,
	}[s]
	if !ok {
		return fmt.Errorf("invalid operator %q", s)
	}
	*ss = got
	return nil
}

func GreaterThan(a, b Value) bool {
	aType, aValue := VtoFS(a)
	bType, bValue := VtoFS(b)
	if aType != bType {
		return false
	}
	switch aType {
	case reflect.Float64:
		return aValue.(float64) > bValue.(float64)
	case reflect.String:
		return aValue.(string) > bValue.(string)
	}
	return false
}
func LessThan(a, b Value) bool {
	aType, aValue := VtoFS(a)
	bType, bValue := VtoFS(b)
	if aType != bType {
		return false
	}
	switch aType {
	case reflect.Float64:
		return aValue.(float64) < bValue.(float64)
	case reflect.String:
		return aValue.(string) < bValue.(string)
	}
	return false
}
func Equal(a, b Value) bool {
	aType, aValue := VtoFS(a)
	bType, bValue := VtoFS(b)
	if aType != bType {
		return false
	}
	switch aType {
	case reflect.Float64:
		return aValue.(float64) == bValue.(float64)
	case reflect.String:
		return aValue.(string) == bValue.(string)
	}
	return false
}
