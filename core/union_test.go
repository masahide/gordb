package core

import (
	"encoding/json"
	"log"
	"reflect"
	"strings"
	"testing"
)

/*
	"selection": {
		"input": { "union":{
				"input1":{"relation": {"name": "test/staff1"}},
				"input2":{"relation": {"name": "test/rank1"}}
		}},
		"attr": "name", "selector": ">", "arg": 0
	}
*/

func TestJsonUnionDecode(t *testing.T) {
	var want = &Relation{
		Attrs: Schema{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Data: [][]Value{
			[]Value{"田中", int64(34), "デザイナー"},
			[]Value{"田中", int64(34), "デザイナー"},
		},
	}

	const jsonStream = `{
		"selection": {
			"input": { 
				"union":{
					"input1":{"relation": {"name": "test/staff1"}},
					"input2":{"relation": {"name": "test/staff1"}}
				}
			},	
			"attr": "name", "selector": "==", "arg": "田中"
		}
	}`
	m := Stream{}
	if err := json.NewDecoder(strings.NewReader(jsonStream)).Decode(&m); err != nil {
		log.Fatal(err)
	}
	result, err := StreamToRelation(m, testData)
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
