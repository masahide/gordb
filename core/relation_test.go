package core

import (
	"encoding/json"
	"log"
	"reflect"
	"strings"
	"testing"
)

var testStaffSchema = Schema{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}}
var testStaff = &Relation{
	Name:  "staff",
	index: 0,
	Attrs: testStaffSchema,
	Data: []Tuple{
		Tuple{attrs: testStaffSchema, data: map[string]Value{"name": "清水", "age": int64(17), "job": "エンジニア"}},
		Tuple{attrs: testStaffSchema, data: map[string]Value{"name": "田中", "age": int64(34), "job": "デザイナー"}},
		Tuple{attrs: testStaffSchema, data: map[string]Value{"name": "佐藤", "age": int64(21), "job": "マネージャー"}},
	},
}
var testStaff3 *Relation
var testRank3 *Relation

var testRankSchema = Schema{Attr{"name", reflect.String}, Attr{"rank", reflect.Int64}}
var testRank = &Relation{
	Name:  "rank",
	index: 0,
	Attrs: testRankSchema,
	Data: []Tuple{
		Tuple{attrs: testRankSchema, data: map[string]Value{"name": "清水", "runk": int64(78)}},
		Tuple{attrs: testRankSchema, data: map[string]Value{"name": "田中", "runk": int64(46)}},
		Tuple{attrs: testRankSchema, data: map[string]Value{"name": "佐藤", "runk": int64(33)}},
	},
}

var testData1 = &Node{
	Name:     "root",
	FullPath: Relations{},
	Nodes: Nodes{
		"test": &Node{
			FullPath: Relations{},
			Name:     "test",
			Relations: Relations{
				"staff1": testStaff,
				"rank1":  testRank,
			},
		},
		"20150926": &Node{
			FullPath: Relations{},
			Nodes: Nodes{
				"data": &Node{
					FullPath: Relations{},
					Name:     "data",
					Relations: Relations{
						"staff2": testStaff,
						"rank2":  testRank,
					},
				},
			},
			Relations: Relations{
				"staff3": testStaff,
				"rank3":  testRank,
			},
		},
	},
}

var testData2 = NewNode("root")

func init() {
	err := testData2.SetRelations("test", Relations{"staff1": testStaff, "rank1": testRank})
	if err != nil {
		log.Fatalln(err)
	}
	err = testData2.SetRelations("20150926/data", Relations{"staff2": testStaff, "rank2": testRank})
	if err != nil {
		log.Fatalln(err)
	}
	testRank3 = testRank.Clone()
	testRank3.Name = "rank3"
	testStaff3 = testStaff.Clone()
	testStaff3.Name = "staff3"
	err = testData2.SetRelation("20150926", testRank3)
	if err != nil {
		log.Fatalln(err)
	}
	err = testData2.SetRelation("20150926", testStaff3)
	if err != nil {
		log.Fatalln(err)
	}
}

func TestGetRelation1(t *testing.T) {

	r, err := testData1.GetRelation("/test/rank1")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(r, testRank) {
		t.Errorf("Does not match %# v, want:%# v", r, testRank)
	}
	r, err = testData2.GetRelation("/test/rank1")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(r, testRank) {
		t.Errorf("Does not match %# v, want:%# v", r, testRank)
	}
}

func TestGetRelation2(t *testing.T) {

	r, err := testData1.GetRelation("20150926/data/staff2")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(r, testStaff) {
		t.Errorf("Does not match %# v, want:%# v", r, testStaff)
	}
	r, err = testData2.GetRelation("20150926/data/staff2")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(r, testStaff) {
		t.Errorf("Does not match %# v, want:%# v", r, testStaff)
	}
}

func TestGetRelation3(t *testing.T) {

	r, err := testData2.GetRelation("20150926/staff3")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(r, testStaff3) {
		t.Errorf("Does not match %# v, want:%# v", r, testStaff3)
	}
}

func TestJsonSelectionStream(t *testing.T) {
	var want = &Relation{
		Attrs: Schema{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		/*
			Data: [][]Value{
				[]Value{"田中", int64(34), "デザイナー"},
				[]Value{"佐藤", int64(21), "マネージャー"},
			},
		*/
	}
	const jsonStream = `{ "selection": {
			"input": { 
				"relation": {"name":"test/staff1"}},
				"attr": "age", "selector": ">", "arg": 20
	}}`
	m := Stream{}
	if err := json.NewDecoder(strings.NewReader(jsonStream)).Decode(&m); err != nil {
		log.Fatal(err)
	}
	result, _ := StreamToRelation(m, testData2)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT * FROM Staff WHERE age > 20'\nresult:% #v,\n want:% #v", result, want)
	}
}

func TestJsonProjectionStream(t *testing.T) {
	var want = &Relation{
		Attrs: Schema{Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		/*
			Data: [][]Value{
				[]Value{int64(17), "エンジニア"},
				[]Value{int64(34), "デザイナー"},
				[]Value{int64(21), "マネージャー"},
			},
		*/
	}
	const jsonStream = `{ "projection": {
			"input": { "relation": {"name":"test/staff1"}},
			"attrs": [ "age","job" ]
	}}`
	m := Stream{}
	if err := json.NewDecoder(strings.NewReader(jsonStream)).Decode(&m); err != nil {
		log.Fatal(err)
	}
	result, _ := StreamToRelation(m, testData2)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT age,job FROM Staff'\nresult:% #v,\n want:% #v", result, want)
	}
}
