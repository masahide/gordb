// go-rdb Value
package gordb

import "reflect"

type Value interface{}

func VtoFS(s Value) (reflect.Kind, Value) {
	switch t := s.(type) {
	case int:
		return reflect.Float64, float64(t)
	case int64:
		return reflect.Float64, float64(t)
	case int32:
		return reflect.Float64, float64(t)
	case float64:
		return reflect.Float64, t
	case float32:
		return reflect.Float64, float64(t)
	case uint:
		return reflect.Float64, float64(t)
	case uint64:
		return reflect.Float64, float64(t)
	case uint32:
		return reflect.Float64, float64(t)
	case string:
		return reflect.String, t
	case bool:
		f := float64(0)
		if t {
			f = float64(1)
		}
		return reflect.Float64, f
	default:
		return reflect.Invalid, t
	}
}
