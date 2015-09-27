// go-rdb Operator
package core

import "testing"

func TestTypeCheck(t *testing.T) {

	//aValue, bValue, kind, err := typeCheck(Value("string"), Value(0))
	_, _, _, err := typeCheck(Value("string"), Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
}

func TestNotGreaterThan(t *testing.T) {
	_, err := NotGreaterThan(Value("string"), Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
	ng, _ := NotGreaterThan(Value(0), Value(0))
	if ng != (0 <= 0) {
		t.Error(" ng != (0 <= 0)")
	}
	ng, _ = NotGreaterThan(Value(1), Value(0))
	if ng != (1 <= 0) {
		t.Error(" ng != (1 <= 0)")
	}
	ng, _ = NotGreaterThan(Value(0), Value(1))
	if ng != (0 <= 1) {
		t.Error(" ng != (0 <= 1)")
	}
	ng, _ = NotGreaterThan(Value("hoge"), Value("hoge"))
	if ng != ("hoge" <= "hoge") {
		t.Error("ng != (\"hoge\" <= \"hoge\")")
	}
	ng, _ = NotGreaterThan(Value("hoge"), Value("fuga"))
	if ng != ("hoge" <= "fuga") {
		t.Error("ng != (\"hoge\" <= \"fuga\")")
	}
	ng, _ = NotGreaterThan(Value("fuga"), Value("hoge"))
	if ng != ("fuga" <= "hoge") {
		t.Error("ng != (\"fuga\" <= \"hoge\")")
	}
}

func TestNotLessThan(t *testing.T) {
	_, err := NotLessThan(Value("string"), Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
	ng, _ := NotLessThan(Value(0), Value(0))
	if ng != (0 >= 0) {
		t.Error(" ng != (0 >= 0)")
	}
	ng, _ = NotLessThan(Value(1), Value(0))
	if ng != (1 >= 0) {
		t.Error(" ng != (1 >= 0)")
	}
	ng, _ = NotLessThan(Value(0), Value(1))
	if ng != (0 >= 1) {
		t.Error(" ng != (0 >= 1)")
	}
	ng, _ = NotLessThan(Value("hoge"), Value("hoge"))
	if ng != ("hoge" >= "hoge") {
		t.Error("ng != (\"hoge\" >= \"hoge\")")
	}
	ng, _ = NotLessThan(Value("hoge"), Value("fuga"))
	if ng != ("hoge" >= "fuga") {
		t.Error("ng != (\"hoge\" >= \"fuga\")")
	}
	ng, _ = NotLessThan(Value("fuga"), Value("hoge"))
	if ng != ("fuga" >= "hoge") {
		t.Error("ng != (\"fuga\" >= \"hoge\")")
	}
}

func TestEqual(t *testing.T) {
	_, err := Equal(Value("string"), Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
	ng, _ := Equal(Value(0), Value(0))
	if ng != (0 == 0) {
		t.Error(" ng != (0 == 0)")
	}
	ng, _ = Equal(Value(1), Value(0))
	if ng != (1 == 0) {
		t.Error(" ng != (1 == 0)")
	}
	ng, _ = Equal(Value(0), Value(1))
	if ng != (0 == 1) {
		t.Error(" ng != (0 == 1)")
	}
	ng, _ = Equal(Value("hoge"), Value("hoge"))
	if ng != ("hoge" == "hoge") {
		t.Error("ng != (\"hoge\" == \"hoge\")")
	}
	ng, _ = Equal(Value("hoge"), Value("fuga"))
	if ng != ("hoge" == "fuga") {
		t.Error("ng != (\"hoge\" == \"fuga\")")
	}
	ng, _ = Equal(Value("fuga"), Value("hoge"))
	if ng != ("fuga" == "hoge") {
		t.Error("ng != (\"fuga\" == \"hoge\")")
	}
}

func TestNotEqual(t *testing.T) {
	_, err := NotEqual(Value("string"), Value(0))
	if err != ErrDifferentType {
		t.Error("err != ErrDifferentType")
	}
	ng, _ := NotEqual(Value(0), Value(0))
	if ng != (0 != 0) {
		t.Error(" ng != (0 != 0)")
	}
	ng, _ = NotEqual(Value(1), Value(0))
	if ng != (1 != 0) {
		t.Error(" ng != (1 != 0)")
	}
	ng, _ = NotEqual(Value(0), Value(1))
	if ng != (0 != 1) {
		t.Error(" ng != (0 != 1)")
	}
	ng, _ = NotEqual(Value("hoge"), Value("hoge"))
	if ng != ("hoge" != "hoge") {
		t.Error("ng != (\"hoge\" != \"hoge\")")
	}
	ng, _ = NotEqual(Value("hoge"), Value("fuga"))
	if ng != ("hoge" != "fuga") {
		t.Error("ng != (\"hoge\" != \"fuga\")")
	}
	ng, _ = NotEqual(Value("fuga"), Value("hoge"))
	if ng != ("fuga" != "hoge") {
		t.Error("ng != (\"fuga\" != \"hoge\")")
	}
}
