package core

import (
	"reflect"
	"testing"
)

func TestRelationalStream_Staff(t *testing.T) {
	schema := Schema{
		Attrs: []Attr{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Index: map[string]int{"name": 0, "age": 1, "job": 2},
	}
	var want = &Relation{
		Attrs: &schema,
		Data: [][]Value{
			[]Value{"清水", int64(17), "エンジニア"},
			[]Value{"田中", int64(34), "デザイナー"},
			[]Value{"佐藤", int64(21), "マネージャー"},
		},
	}
	original := &Relation{Name: "test/staff1"}
	result, err := StreamToRelation(Stream{Relation: original}, testData1)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(original.Data, result.Data) {
		t.Errorf("Does not match 'SELECT * FROM Staff' original:%# v, want:%# v", result.Data, want.Data)
	}
}

func TestRelationalStream_Rank(t *testing.T) {
	schema := Schema{
		Attrs: []Attr{Attr{"name", reflect.String}, Attr{"rank", reflect.Int64}},
		Index: map[string]int{"name": 0, "rank": 1},
	}
	var want = &Relation{
		Name:  "rank",
		index: 0,
		Attrs: &schema,
		Data: [][]Value{
			[]Value{"清水", int64(78)},
			[]Value{"田中", int64(46)},
			[]Value{"佐藤", int64(33)},
		},
	}
	original := &Relation{Name: "test/rank1"}
	result, err := StreamToRelation(Stream{Relation: original}, testData2)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result.Data, want.Data) {
		t.Errorf("Does not match 'SELECT * FROM Rank' original:%# v, want:%# v", result.Data, want.Data)
	}
}

func TestSelectionStream(t *testing.T) {
	schema := Schema{
		Attrs: []Attr{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Index: map[string]int{"name": 0, "age": 1, "job": 2},
	}
	var want = &Relation{
		index: 0,
		Attrs: &schema,
		Data: [][]Value{
			[]Value{"田中", int64(34), "デザイナー"},
			[]Value{"佐藤", int64(21), "マネージャー"},
		},
	}
	stream2 := &SelectionStream{Input: Stream{Relation: &Relation{Name: "test/staff1"}}, Attr: "age", Selector: GreaterThan, Arg: 20}
	result, _ := StreamToRelation(Stream{Selection: stream2}, testData2)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT * FROM Staff WHERE age > 20'\nresult:% #v,\n want:% #v", result, want)
	}
}

func TestProjectionStream(t *testing.T) {
	schema := Schema{
		Attrs: []Attr{Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Index: map[string]int{"age": 0, "job": 1},
	}
	var want = &Relation{
		index: 0,
		Attrs: &schema,
		Data: [][]Value{
			[]Value{int64(17), "エンジニア"},
			[]Value{int64(34), "デザイナー"},
			[]Value{int64(21), "マネージャー"},
		},
	}
	stream2 := &ProjectionStream{Stream{Relation: &Relation{Name: "test/staff1"}}, []string{"age", "job"}}
	result, _ := StreamToRelation(Stream{Projection: stream2}, testData2)
	if !reflect.DeepEqual(result.Data, want.Data) {
		t.Errorf("Does not match 'SELECT age,job FROM Staff'\nresult:% #v,\n want:% #v", result.Data, want.Data)
	}
	if !reflect.DeepEqual(result.Attrs, want.Attrs) {
		t.Errorf("Does not match 'SELECT age,job FROM Staff'\nresult:% #v,\n want:% #v", result.Attrs, want.Attrs)
	}
}

func TestJoinStream(t *testing.T) {
	schema := Schema{
		Attrs: []Attr{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}, Attr{"rank", reflect.Int64}},
		Index: map[string]int{"name": 0, "age": 1, "job": 2, "rank": 3},
	}
	var want = &Relation{
		index: 0,
		Attrs: &schema,
		Data: [][]Value{
			[]Value{"清水", int64(17), "エンジニア", int64(78)},
			[]Value{"田中", int64(34), "デザイナー", int64(46)},
			[]Value{"佐藤", int64(21), "マネージャー", int64(33)},
		},
	}
	stream3 := &JoinStream{
		Input1:   Stream{Relation: &Relation{Name: "test/staff1"}},
		Attr1:    "name",
		Input2:   Stream{Relation: &Relation{Name: "test/rank1"}},
		Attr2:    "name",
		Selector: Equal,
	}
	result, _ := StreamToRelation(Stream{Join: stream3}, testData2)
	if !reflect.DeepEqual(result.Data, want.Data) {
		t.Errorf("Does not match 'SELECT * FROM Staff, Rank WHERE staff.name = rank.name' result:%#v, want: %#v", result.Data, want.Data)
	}
}

func TestCrossJoinStream(t *testing.T) {
	schema := Schema{
		Attrs: []Attr{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}, Attr{"name2", reflect.String}, Attr{"rank", reflect.Int64}},
		Index: map[string]int{"name": 0, "age": 1, "job": 2, "name2": 3, "rank": 4},
	}
	var want = &Relation{
		index: 0,
		Attrs: &schema,
		Data: [][]Value{
			[]Value{"清水", int64(17), "エンジニア", "清水", int64(78)},
			[]Value{"清水", int64(17), "エンジニア", "田中", int64(46)},
			[]Value{"清水", int64(17), "エンジニア", "佐藤", int64(33)},
			[]Value{"田中", int64(34), "デザイナー", "清水", int64(78)},
			[]Value{"田中", int64(34), "デザイナー", "田中", int64(46)},
			[]Value{"田中", int64(34), "デザイナー", "佐藤", int64(33)},
			[]Value{"佐藤", int64(21), "マネージャー", "清水", int64(78)},
			[]Value{"佐藤", int64(21), "マネージャー", "田中", int64(46)},
			[]Value{"佐藤", int64(21), "マネージャー", "佐藤", int64(33)},
		},
		/*
			Data: [][]Value{
				[]Value{"清水", int64(17), "エンジニア", "清水", int64(78)},
				[]Value{"清水", int64(17), "エンジニア", "田中", int64(46)},
				[]Value{"清水", int64(17), "エンジニア", "佐藤", int64(33)},
				[]Value{"田中", int64(34), "デザイナー", "清水", int64(78)},
				[]Value{"田中", int64(34), "デザイナー", "田中", int64(46)},
				[]Value{"田中", int64(34), "デザイナー", "佐藤", int64(33)},
				[]Value{"佐藤", int64(21), "マネージャー", "清水", int64(78)},
				[]Value{"佐藤", int64(21), "マネージャー", "田中", int64(46)},
				[]Value{"佐藤", int64(21), "マネージャー", "佐藤", int64(33)},
			},
		*/
	}
	stream3 := &CrossJoinStream{
		Input1: Stream{Relation: &Relation{Name: "test/staff1"}},
		Input2: Stream{Rename: &RenameStream{Stream{Relation: &Relation{Name: "test/rank1"}}, "name", "name2"}}}
	result, _ := StreamToRelation(Stream{CrossJoin: stream3}, testData2)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT * FROM Staff CROSS JOIN Rank' result:% #v", result)
	}
}

func TestEmpty(t *testing.T) {
	var want = &Relation{
		index: 0,
		Attrs: NewSchema(),
		Data:  [][]Value{},
	}
	stream := Stream{
		Selection: &SelectionStream{
			Input: Stream{
				CrossJoin: &CrossJoinStream{
					Input1: Stream{Relation: &Relation{Name: "test/staff1"}},
					Input2: Stream{
						Rename: &RenameStream{
							Stream{Relation: &Relation{Name: "test/rank1"}},
							"name", "name2"},
					},
				},
			},
			Attr: "age", Selector: GreaterThan, Arg: 100,
		},
	}
	result, _ := StreamToRelation(stream, testData2)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT * FROM Staff CROSS JOIN Rank where age > 100' result:% #v, want: % #v", result, want)
	}
}
