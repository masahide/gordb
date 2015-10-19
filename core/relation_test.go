package core

import (
	"encoding/json"
	"log"
	"reflect"
	"strings"
	"testing"
)

var testStaffSchema = Schema{
	Attrs: []Attr{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
	Index: map[string]int{"name": 0, "age": 1, "job": 2},
}
var testStaff = &Relation{
	Name:  "staff",
	index: 0,
	Attrs: &testStaffSchema,
	Data: [][]Value{
		[]Value{"清水", int64(17), "エンジニア"},
		[]Value{"田中", int64(34), "デザイナー"},
		[]Value{"佐藤", int64(21), "マネージャー"},
	},
}
var testStaff3 *Relation
var testRank3 *Relation

var testRankSchema = Schema{
	Attrs: []Attr{Attr{"name", reflect.String}, Attr{"rank", reflect.Int64}},
	Index: map[string]int{"name": 0, "rank": 1},
}
var testRank = &Relation{
	Name:  "rank",
	index: 0,
	Attrs: &testRankSchema,
	Data: [][]Value{
		[]Value{"清水", int64(78)},
		[]Value{"田中", int64(46)},
		[]Value{"佐藤", int64(33)},
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
	testStaff.CreateIndex()
	testRank.CreateIndex()
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
	schema := Schema{
		Attrs: []Attr{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Index: map[string]int{"name": 0, "age": 1, "job": 2},
	}
	var want = &Relation{
		Attrs: &schema,
		Data: [][]Value{
			[]Value{"田中", int64(34), "デザイナー"},
			[]Value{"佐藤", int64(21), "マネージャー"},
		},
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
	schema := Schema{
		Attrs: []Attr{Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Index: map[string]int{"age": 0, "job": 1},
	}
	var want = &Relation{
		Attrs: &schema,
		Data: [][]Value{
			[]Value{int64(17), "エンジニア"},
			[]Value{int64(34), "デザイナー"},
			[]Value{int64(21), "マネージャー"},
		},
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
	php := result.MarshalPHP(PhpOptions{KvFmt: true, MapKey: "age"})
	phpwant := map[interface{}]interface{}{
		"Name":  "",
		"Attrs": map[interface{}]interface{}{0: "age", 1: "job"},
		"Data": map[interface{}]interface{}{
			int64(34): map[interface{}]interface{}{"age": int64(34), "job": "デザイナー"},
			int64(21): map[interface{}]interface{}{"age": int64(21), "job": "マネージャー"},
			int64(17): map[interface{}]interface{}{"age": int64(17), "job": "エンジニア"},
		},
	}
	if !reflect.DeepEqual(php, phpwant) {
		t.Errorf("Does not match php:%#v,\n wantphp:%#v", php, phpwant)
	}
	php2 := result.MarshalPHP(PhpOptions{})
	phpwant2 := map[interface{}]interface{}{
		"Name":  "",
		"Attrs": map[interface{}]interface{}{0: "age", 1: "job"},
		"Data": map[interface{}]interface{}{
			0: map[interface{}]interface{}{0: int64(17), 1: "エンジニア"},
			1: map[interface{}]interface{}{0: int64(34), 1: "デザイナー"},
			2: map[interface{}]interface{}{0: int64(21), 1: "マネージャー"},
		},
	}
	if !reflect.DeepEqual(php2, phpwant2) {
		t.Errorf("Does not match php:%#v,\n wantphp:%#v", php2, phpwant2)
	}
}

var testStaff2 = &Relation{
	Name:  "staff",
	index: 0,
	Attrs: &testStaffSchema,
	Data: [][]Value{
		[]Value{"清水", int64(17), "エンジニア"},
		[]Value{"佐藤", int64(35), "マネージャー"},
		[]Value{"田中", int64(34), "デザイナー"},
		[]Value{"佐藤", int64(21), "マネージャー"},
		[]Value{"佐藤", int64(34), "マネージャー"},
	},
}

func init() {
	testStaff2.CreateIndex()
}

func TestCreateIndex(t *testing.T) {
	want := []indexArrays{
		indexArrays{
			indexArray{key: "佐藤", ptr: 1},
			indexArray{key: "佐藤", ptr: 3},
			indexArray{key: "佐藤", ptr: 4},
			indexArray{key: "清水", ptr: 0},
			indexArray{key: "田中", ptr: 2},
		},
		indexArrays{
			indexArray{key: int64(17), ptr: 0},
			indexArray{key: int64(21), ptr: 3},
			indexArray{key: int64(34), ptr: 2},
			indexArray{key: int64(34), ptr: 4},
			indexArray{key: int64(35), ptr: 1},
		},
		indexArrays{
			indexArray{key: "エンジニア", ptr: 0},
			indexArray{key: "デザイナー", ptr: 2},
			indexArray{key: "マネージャー", ptr: 1},
			indexArray{key: "マネージャー", ptr: 3},
			indexArray{key: "マネージャー", ptr: 4},
		},
	}
	if !reflect.DeepEqual(testStaff2.staticIndex, want) {
		t.Errorf("Does not match \nstaticIndex:%v,\n       want:%v", testStaff2.staticIndex, want)
	}
}
func TestFindSameValueInDesc(t *testing.T) {
	res := testStaff2.findSameValueInDesc("hoge", 0, 0)
	if res != 0 {
		t.Errorf("res != 0 res:%v", res)
	}
	res = testStaff2.findSameValueInDesc("name", 0, "佐藤")
	if res != 0 {
		t.Errorf("res != 0 res:%v", res)
	}
	res = testStaff2.findSameValueInDesc("name", 3, "清水")
	if res != 3 {
		t.Errorf("res != 3 res:%v", res)
	}
	res = testStaff2.findSameValueInDesc("name", 4, "田中1")
	if res != 5 {
		t.Errorf("res != 5 res:%v", res)
	}

}
func TestFindSameValueInAsc(t *testing.T) {
	res := testStaff2.findSameValueInAsc("hoge", 0, 0)
	if res != 0 {
		t.Error("res != 0")
	}
	res = testStaff2.findSameValueInAsc("name", 0, "佐藤")
	if res != 2 {
		t.Errorf("res != 2 res:%v", res)
	}
	res = testStaff2.findSameValueInAsc("name", 3, "清水")
	if res != 3 {
		t.Errorf("res != 3 res:%v", res)
	}
	res = testStaff2.findSameValueInAsc("name", 4, "田中1")
	if res != 3 {
		t.Errorf("res != 3 res:%v", res)
	}

}

func TestSearchLessThan(t *testing.T) {
	var want []int
	want = []int{0, 3, 2, 4, 1}
	res := SearchLessThan(testStaff2, "age", int64(100), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{}
	res = SearchLessThan(testStaff2, "age", int64(16), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{0, 3, 2, 4, 1}
	res = SearchLessThan(testStaff2, "age", int64(17), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{0}
	res = SearchLessThan(testStaff2, "age", int64(18), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{0}
	res = SearchLessThan(testStaff2, "age", int64(20), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{0}
	res = SearchLessThan(testStaff2, "age", int64(20), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{0}
	res = SearchLessThan(testStaff2, "age", int64(21), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{0, 3}
	res = SearchLessThan(testStaff2, "age", int64(22), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{0, 3}
	res = SearchLessThan(testStaff2, "age", int64(34), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{0, 3, 2, 4, 1}
	res = SearchNotGreaterThan(testStaff2, "age", int64(100), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{0, 3, 2, 4, 1}
	res = SearchNotGreaterThan(testStaff2, "age", int64(35), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{}
	res = SearchNotGreaterThan(testStaff2, "age", int64(1), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
}
func TestSearchGreaterThan(t *testing.T) {
	var want []int
	want = []int{}
	res := SearchGreaterThan(testStaff2, "age", int64(100), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{1}
	res = SearchGreaterThan(testStaff2, "age", int64(34), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{2, 4, 1}
	res = SearchGreaterThan(testStaff2, "age", int64(33), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{2, 4, 1}
	res = SearchNotLessThan(testStaff2, "age", int64(34), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{0, 3, 2, 4, 1}
	res = SearchNotLessThan(testStaff2, "age", int64(1), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{}
	res = SearchNotLessThan(testStaff2, "age", int64(36), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{}
	res = SearchMulti(testStaff2, "age", int64(1), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{2, 4}
	res = SearchMulti(testStaff2, "age", int64(34), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{1}
	res = SearchMulti(testStaff2, "age", int64(35), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
	want = []int{}
	res = SearchMulti(testStaff2, "age", int64(36), reflect.Int64)
	if !reflect.DeepEqual(res, want) {
		t.Errorf("Does not match \nres:%v,\n       want:%v", res, want)
	}
}
