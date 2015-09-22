// go-rdb Operator
package gordb

type Operator func(Value, Value) bool

func GreaterThan(a, b Value) bool {
	aType, aValue := VtoFS(a)
	bType, bValue := VtoFS(b)
	if aType != bType {
		return false
	}
	switch aType {
	case FloatType:
		return aValue.(float64) > bValue.(float64)
	case StringType:
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
	case FloatType:
		return aValue.(float64) < bValue.(float64)
	case StringType:
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
	case FloatType:
		return aValue.(float64) == bValue.(float64)
	case StringType:
		return aValue.(string) == bValue.(string)
	}
	return false
}
