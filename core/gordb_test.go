package core

import (
	"reflect"
	"testing"
)

func TestRelationalStream_Staff(t *testing.T) {
	var SELECT_FROM_Staff = &Relation{
		Attrs: Schema{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Data: [][]Value{
			[]Value{"清水", int64(17), "エンジニア"},
			[]Value{"田中", int64(34), "デザイナー"},
			[]Value{"佐藤", int64(21), "マネージャー"},
		},
	}

	original := &Relation{Name: "test/staff1"}
	result, err := StreamToRelation(Stream{Relation: original}, testData)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result.Data, SELECT_FROM_Staff.Data) {
		t.Errorf("Does not match 'SELECT * FROM Staff' original:%# v, want:%# v", result.Data, SELECT_FROM_Staff.Data)
	}
}

func TestRelationalStream_Rank(t *testing.T) {
	var want = &Relation{
		Name:  "rank",
		index: 0,
		Attrs: Schema{Attr{"name", reflect.String}, Attr{"rank", reflect.Int64}},
		Data: [][]Value{
			[]Value{"清水", int64(78)},
			[]Value{"田中", int64(46)},
			[]Value{"佐藤", int64(33)},
		},
	}
	original := &Relation{Name: "test/rank1"}
	result, err := StreamToRelation(Stream{Relation: original}, testData)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(result.Data, want.Data) {
		t.Errorf("Does not match 'SELECT * FROM Rank' original:%# v, want:%# v", result.Data, want.Data)
	}
}

func TestSelectionStream(t *testing.T) {
	var want = &Relation{
		index: 0,
		Attrs: Schema{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Data: [][]Value{
			[]Value{"田中", int64(34), "デザイナー"},
			[]Value{"佐藤", int64(21), "マネージャー"},
		},
	}
	stream2 := &SelectionStream{Stream{Relation: &Relation{Name: "test/staff1"}}, "age", GreaterThan, 20}
	result, _ := StreamToRelation(Stream{Selection: stream2}, testData)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT * FROM Staff WHERE age > 20'\nresult:% #v,\n want:% #v", result, want)
	}
}

func TestProjectionStream(t *testing.T) {
	var want = &Relation{
		index: 0,
		Attrs: Schema{Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Data: [][]Value{
			[]Value{int64(17), "エンジニア"},
			[]Value{int64(34), "デザイナー"},
			[]Value{int64(21), "マネージャー"},
		},
	}
	stream2 := &ProjectionStream{Stream{Relation: &Relation{Name: "test/staff1"}}, []string{"age", "job"}}
	result, _ := StreamToRelation(Stream{Projection: stream2}, testData)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT age,job FROM Staff'\nresult:% #v,\n want:% #v", result, want)
	}
}

func TestJoinStream(t *testing.T) {
	var SELECT_FROM_Staff_Rank_WHERE_staff_name_rank_name = &Relation{
		index: 0,
		Attrs: Schema{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}, Attr{"rank", reflect.Int64}},
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
	result, _ := StreamToRelation(Stream{Join: stream3}, testData)
	if !reflect.DeepEqual(result, SELECT_FROM_Staff_Rank_WHERE_staff_name_rank_name) {
		t.Errorf("Does not match 'SELECT * FROM Staff, Rank WHERE staff.name = rank.name' result:% #v", result)
	}
}

func TestCrossJoinStream(t *testing.T) {
	var want = &Relation{
		index: 0,
		Attrs: Schema{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}, Attr{"name2", reflect.String}, Attr{"rank", reflect.Int64}},
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
	}
	stream3 := &CrossJoinStream{
		Input1: Stream{Relation: &Relation{Name: "test/staff1"}},
		Input2: Stream{Rename: &RenameStream{Stream{Relation: &Relation{Name: "test/rank1"}}, "name", "name2"}}}
	result, _ := StreamToRelation(Stream{CrossJoin: stream3}, testData)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT * FROM Staff CROSS JOIN Rank' result:% #v", result)
	}
}

func TestEmpty(t *testing.T) {
	var want = &Relation{
		index: 0,
		Attrs: Schema{},
		Data:  [][]Value{},
	}
	stream := Stream{
		Selection: &SelectionStream{
			Stream{
				CrossJoin: &CrossJoinStream{
					Input1: Stream{Relation: &Relation{Name: "test/staff1"}},
					Input2: Stream{
						Rename: &RenameStream{
							Stream{Relation: &Relation{Name: "test/rank1"}},
							"name", "name2"},
					},
				},
			},
			"age", GreaterThan, 100,
		},
	}
	result, _ := StreamToRelation(stream, testData)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT * FROM Staff CROSS JOIN Rank where age > 100' result:% #v", result)
	}
}