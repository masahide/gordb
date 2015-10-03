// go-rdb Operator
package core

import (
	"reflect"
	"testing"
)

func TestTypeCheck(t *testing.T) {

	//aValue, bValue, kind, err := typeCheck(Value("string"), Value(0))
	_, _, _, err := typeCheck(Value("string"), Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
}

func TestGreaterThan(t *testing.T) {
	_, err := GreaterThan(reflect.String, Value([]string{""}), 0, Value([]string{"hoge"}))
	if err != ErrUnkownType {
		t.Error("err != ErrUnkownType", err)
	}
	_, err = GreaterThan(reflect.String, Value("string"), reflect.Int, Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
	ng, _ := GreaterThan(reflect.Int, Value(0), reflect.Int, Value(0))
	if ng != (0 > 0) {
		t.Error(" ng != (0 > 0)")
	}
	ng, _ = GreaterThan(reflect.Int, Value(1), reflect.Int, Value(0))
	if ng != (1 > 0) {
		t.Error(" ng != (1 > 0)")
	}
	ng, _ = GreaterThan(reflect.Int, Value(0), reflect.Int, Value(1))
	if ng != (0 > 1) {
		t.Error(" ng != (0 > 1)")
	}
	ng, _ = GreaterThan(reflect.String, Value("hoge"), reflect.String, Value("hoge"))
	if ng != ("hoge" > "hoge") {
		t.Error("ng != (\"hoge\" > \"hoge\")")
	}
	ng, _ = GreaterThan(reflect.String, Value("hoge"), reflect.String, Value("fuga"))
	if ng != ("hoge" > "fuga") {
		t.Error("ng != (\"hoge\" > \"fuga\")")
	}
	ng, _ = GreaterThan(reflect.String, Value("fuga"), reflect.String, Value("hoge"))
	if ng != ("fuga" > "hoge") {
		t.Error("ng != (\"fuga\" > \"hoge\")")
	}
}
func TestNotGreaterThan(t *testing.T) {
	_, err := NotGreaterThan(0, Value([]string{"string"}), reflect.String, Value([]string{"hoge"}))
	if err != ErrUnkownType {
		t.Error("err != ErrUnkownType")
	}
	_, err = NotGreaterThan(reflect.String, Value("string"), reflect.Int, Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
	ng, _ := NotGreaterThan(reflect.Int, Value(0), reflect.Int, Value(0))
	if ng != (0 <= 0) {
		t.Error(" ng != (0 <= 0)")
	}
	ng, _ = NotGreaterThan(reflect.Int, Value(1), reflect.Int, Value(0))
	if ng != (1 <= 0) {
		t.Error(" ng != (1 <= 0)")
	}
	ng, _ = NotGreaterThan(reflect.Int, Value(0), reflect.Int, Value(1))
	if ng != (0 <= 1) {
		t.Error(" ng != (0 <= 1)")
	}
	ng, _ = NotGreaterThan(reflect.String, Value("hoge"), reflect.String, Value("hoge"))
	if ng != ("hoge" <= "hoge") {
		t.Error("ng != (\"hoge\" <= \"hoge\")")
	}
	ng, _ = NotGreaterThan(reflect.String, Value("hoge"), reflect.String, Value("fuga"))
	if ng != ("hoge" <= "fuga") {
		t.Error("ng != (\"hoge\" <= \"fuga\")")
	}
	ng, _ = NotGreaterThan(reflect.String, Value("fuga"), reflect.String, Value("hoge"))
	if ng != ("fuga" <= "hoge") {
		t.Error("ng != (\"fuga\" <= \"hoge\")")
	}
}

func TestLessThan(t *testing.T) {
	_, err := LessThan(0, Value([]string{""}), reflect.String, Value([]string{"hoge"}))
	if err != ErrUnkownType {
		t.Error("err != ErrUnkownType")
	}
	_, err = LessThan(reflect.String, Value("string"), reflect.Int, Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
	ng, _ := LessThan(reflect.Int, Value(0), reflect.Int, Value(0))
	if ng != (0 < 0) {
		t.Error(" ng != (0 < 0)")
	}
	ng, _ = LessThan(reflect.Int, Value(1), reflect.Int, Value(0))
	if ng != (1 < 0) {
		t.Error(" ng != (1 < 0)")
	}
	ng, _ = LessThan(reflect.Int, Value(0), reflect.Int, Value(1))
	if ng != (0 < 1) {
		t.Error(" ng != (0 < 1)")
	}
	ng, _ = LessThan(reflect.String, Value("hoge"), reflect.String, Value("hoge"))
	if ng != ("hoge" < "hoge") {
		t.Error("ng != (\"hoge\" < \"hoge\")")
	}
	ng, _ = LessThan(reflect.String, Value("hoge"), reflect.String, Value("fuga"))
	if ng != ("hoge" < "fuga") {
		t.Error("ng != (\"hoge\" < \"fuga\")")
	}
	ng, _ = LessThan(reflect.String, Value("fuga"), reflect.String, Value("hoge"))
	if ng != ("fuga" < "hoge") {
		t.Error("ng != (\"fuga\" < \"hoge\")")
	}
}

func TestNotLessThan(t *testing.T) {
	_, err := NotLessThan(0, Value([]string{""}), reflect.String, Value([]string{"hoge"}))
	if err != ErrUnkownType {
		t.Error("err != ErrUnkownType")
	}
	_, err = NotLessThan(reflect.String, Value("string"), reflect.Int, Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
	ng, _ := NotLessThan(reflect.Int, Value(0), reflect.Int, Value(0))
	if ng != (0 >= 0) {
		t.Error(" ng != (0 >= 0)")
	}
	ng, _ = NotLessThan(reflect.Int, Value(1), reflect.Int, Value(0))
	if ng != (1 >= 0) {
		t.Error(" ng != (1 >= 0)")
	}
	ng, _ = NotLessThan(reflect.Int, Value(0), reflect.Int, Value(1))
	if ng != (0 >= 1) {
		t.Error(" ng != (0 >= 1)")
	}
	ng, _ = NotLessThan(reflect.String, Value("hoge"), reflect.String, Value("hoge"))
	if ng != ("hoge" >= "hoge") {
		t.Error("ng != (\"hoge\" >= \"hoge\")")
	}
	ng, _ = NotLessThan(reflect.String, Value("hoge"), reflect.String, Value("fuga"))
	if ng != ("hoge" >= "fuga") {
		t.Error("ng != (\"hoge\" >= \"fuga\")")
	}
	ng, _ = NotLessThan(reflect.String, Value("fuga"), reflect.String, Value("hoge"))
	if ng != ("fuga" >= "hoge") {
		t.Error("ng != (\"fuga\" >= \"hoge\")")
	}
}

func TestEqual(t *testing.T) {
	_, err := Equal(0, Value([]string{""}), reflect.String, Value([]string{"hoge"}))
	if err != ErrUnkownType {
		t.Error("err != ErrUnkownType")
	}
	_, err = Equal(reflect.String, Value("string"), reflect.Int, Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
	ng, _ := Equal(reflect.Int, Value(0), reflect.Int, Value(0))
	if ng != (0 == 0) {
		t.Error(" ng != (0 == 0)")
	}
	ng, _ = Equal(reflect.Int, Value(1), reflect.Int, Value(0))
	if ng != (1 == 0) {
		t.Error(" ng != (1 == 0)")
	}
	ng, _ = Equal(reflect.Int, Value(0), reflect.Int, Value(1))
	if ng != (0 == 1) {
		t.Error(" ng != (0 == 1)")
	}
	ng, _ = Equal(reflect.String, Value("hoge"), reflect.String, Value("hoge"))
	if ng != ("hoge" == "hoge") {
		t.Error("ng != (\"hoge\" == \"hoge\")")
	}
	ng, _ = Equal(reflect.String, Value("hoge"), reflect.String, Value("fuga"))
	if ng != ("hoge" == "fuga") {
		t.Error("ng != (\"hoge\" == \"fuga\")")
	}
	ng, _ = Equal(reflect.String, Value("fuga"), reflect.String, Value("hoge"))
	if ng != ("fuga" == "hoge") {
		t.Error("ng != (\"fuga\" == \"hoge\")")
	}
}

func TestNotEqual(t *testing.T) {
	_, err := NotEqual(0, Value([]string{""}), reflect.String, Value([]string{"hoge"}))
	if err != ErrUnkownType {
		t.Error("err != ErrUnkownType")
	}
	_, err = NotEqual(reflect.String, Value("string"), reflect.Int, Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
	ng, _ := NotEqual(reflect.Int, Value(0), reflect.Int, Value(0))
	if ng != (0 != 0) {
		t.Error(" ng != (0 != 0)")
	}
	ng, _ = NotEqual(reflect.Int, Value(1), reflect.Int, Value(0))
	if ng != (1 != 0) {
		t.Error(" ng != (1 != 0)")
	}
	ng, _ = NotEqual(reflect.Int, Value(0), reflect.Int, Value(1))
	if ng != (0 != 1) {
		t.Error(" ng != (0 != 1)")
	}
	ng, _ = NotEqual(reflect.String, Value("hoge"), reflect.String, Value("hoge"))
	if ng != ("hoge" != "hoge") {
		t.Error("ng != (\"hoge\" != \"hoge\")")
	}
	ng, _ = NotEqual(reflect.String, Value("hoge"), reflect.String, Value("fuga"))
	if ng != ("hoge" != "fuga") {
		t.Error("ng != (\"hoge\" != \"fuga\")")
	}
	ng, _ = NotEqual(reflect.String, Value("fuga"), reflect.String, Value("hoge"))
	if ng != ("fuga" != "hoge") {
		t.Error("ng != (\"fuga\" != \"hoge\")")
	}
}
