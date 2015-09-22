// go-rdb Value
package gordb

type Value interface{}

type ValueType int

const (
	StringType ValueType = 1 + iota
	FloatType
	OtherType
)

func VtoFS(s Value) (ValueType, Value) {
	switch t := s.(type) {
	case int:
		return FloatType, float64(t)
	case int64:
		return FloatType, float64(t)
	case int32:
		return FloatType, float64(t)
	case float64:
		return FloatType, t
	case float32:
		return FloatType, float64(t)
	case uint:
		return FloatType, float64(t)
	case uint64:
		return FloatType, float64(t)
	case uint32:
		return FloatType, float64(t)
	case string:
		return StringType, t
	case bool:
		f := float64(0)
		if t {
			f = float64(1)
		}
		return FloatType, f
	default:
		return OtherType, t
	}
}
