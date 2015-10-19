package core

import (
	"encoding/json"
	"log"
	"reflect"
	"strings"
	"testing"
)

func TestJsonIndexedSelectionStream(t *testing.T) {
	schema := Schema{
		Attrs: []Attr{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Index: map[string]int{"name": 0, "age": 1, "job": 2},
	}
	var want = &Relation{
		Attrs: &schema,
		Data: [][]Value{
			[]Value{"佐藤", int64(21), "マネージャー"},
			[]Value{"田中", int64(34), "デザイナー"},
		},
	}
	const jsonStream = `{ "iselection": {
			"input":  {"name":"test/staff1"},
			"attr": "age", "selector": ">", "arg": 20
	}}`
	m := Stream{}
	if err := json.NewDecoder(strings.NewReader(jsonStream)).Decode(&m); err != nil {
		log.Fatal(err)
	}
	result, err := StreamToRelation(m, testData2)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT * FROM Staff WHERE age > 20'\nresult:% #v,\n want:% #v", result, want)
	}
}

func TestIndexedSelectionInit(t *testing.T) {
	s := &IndexedSelectionStream{Arg: int(1), Input: &Relation{}}
	err := s.Init(&Node{})
	if err == nil {
		t.Error("err == nil")
	}
	s = &IndexedSelectionStream{Arg: int64(1), Input: &Relation{}}
	err = s.Init(&Node{})
	if err == nil {
		t.Error("err == nil")
	}
	s = &IndexedSelectionStream{Arg: "hoge", Input: &Relation{}}
	err = s.Init(&Node{})
	if err == nil {
		t.Error("err == nil")
	}

	s = &IndexedSelectionStream{Arg: true, Input: &Relation{}}
	err = s.Init(&Node{})
	if err == nil {
		t.Error("err == nil")
	}
	s = &IndexedSelectionStream{Arg: false, Input: &Relation{}}
	err = s.Init(&Node{})
	if err == nil {
		t.Error("err == nil")
	}
}
