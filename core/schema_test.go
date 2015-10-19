package core

import (
	"bytes"
	"encoding/json"
	"log"
	"reflect"
	"testing"
)

func TestJsonMarshalRelation(t *testing.T) {
	want := []byte(`{"attrs":["name","age","job"],"data":[["田中",34,"デザイナー"],["佐藤",21,"マネージャー"]]}`)
	/*
		var r = &Relation{
			Attrs: Schema{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
				Data: [][]Value{
					[]Value{"田中", int64(34), "デザイナー"},
					[]Value{"佐藤", int64(21), "マネージャー"},
				},
		}
	*/
	schema := Schema{
		Attrs: []Attr{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Index: map[string]int{"name": 0, "arg": 1, "job": 2},
	}
	var r = &Relation{
		Attrs: &schema,
		Data: [][]Value{
			[]Value{"田中", int64(34), "デザイナー"},
			[]Value{"佐藤", int64(21), "マネージャー"},
		},
	}
	b, err := json.Marshal(r)
	if err != nil {
		log.Fatal(err)
	}
	if bytes.Compare(b, want) != 0 {
		t.Errorf("Does not match '\nresult:%s,\n want:%s", b, want)
	}
}

func TestGetKind(t *testing.T) {
	s := Schema{}
	res := s.GetKind("hoge")
	if res != 0 {
		t.Error("res != 0")
	}
}
