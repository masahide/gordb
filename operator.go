// go-rdb Operator
package gordb

import "reflect"

type Operator func(Value, Value) bool

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
