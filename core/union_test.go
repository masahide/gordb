package core

import (
	"encoding/json"
	"log"
	"reflect"
	"strings"
	"testing"
)

func TestJsonUnionDecode(t *testing.T) {
	schema := Schema{
		Attrs: []Attr{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Index: map[string]int{"name": 0, "age": 1, "job": 2},
	}
	var want = &Relation{
		Attrs: &schema,
		Data: [][]Value{
			[]Value{"田中", int64(34), "デザイナー"},
			[]Value{"田中", int64(34), "デザイナー"},
		},
	}

	const jsonStream = `{
		"selection": {
			"input": { 
				"union":{
					"inputs":[{"relation": {"name": "test/staff1"}}, {"relation": {"name": "test/staff1"}}]
				}
			},	
			"attr": "name", "selector": "==", "arg": "田中"
		}
	}`
	m := Stream{}
	if err := json.NewDecoder(strings.NewReader(jsonStream)).Decode(&m); err != nil {
		log.Fatal(err)
	}
	result, err := StreamToRelation(m, testData2)
	if err != nil {
		t.Error(err)
	}
	if result == nil {
		t.Error("result is nil")
		return
	}
	if !reflect.DeepEqual(result.Data, want.Data) {
		t.Errorf("Does not match result:%# v, want:%# v", result.Data, want.Data)
	}

}
