package csv

import (
	"reflect"
	"testing"
)

func TestRecordToData(t *testing.T) {
	if _, err := recordToData(nil, nil); err != ErrSchemaNil {
		t.Error(err)
	}
}
func TestLoadCsv(t *testing.T) {
	if _, err := LoadCsv(""); err == nil {
		t.Error("err == nil")
	}
	if _, err := LoadCsv("csv.go"); err == nil {
		t.Error("err == nil")
	}
}
func TestInferenceType(t *testing.T) {
	if s, _ := inferenceType(""); s != reflect.Invalid {
		t.Errorf("s !=0 s:%s", s)
	}
	if s, _ := inferenceType("string"); s != reflect.String {
		t.Errorf("s :%s", s)
	}
	if s, _ := inferenceType("100"); s != reflect.Int64 {
		t.Errorf("s :%s", s)
	}
	if s, _ := inferenceType("0.1"); s != reflect.Float64 {
		t.Errorf("s :%s", s)
	}
}
