package gordb

import (
	"os"
	"reflect"
	"testing"
)

func TestCSVRelationalStream_Staff(t *testing.T) {
	var SELECT_FROM_Staff = &Relation{
		index: 0,
		Attrs: Schema{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Data: [][]Value{
			[]Value{"清水", int64(17), "エンジニア"},
			[]Value{"田中", int64(34), "デザイナー"},
			[]Value{"佐藤", int64(21), "マネージャー"},
		},
	}
	//&Relation{index: 0, Attrs:Schema{Attr{Name:"name", Kind: 0x18}, Attr{Name:"age", Kind: 0x6}, Attr{Name:"job", Kind: 0x18}}, Data:[][]Value{[]Value{"清水",  17, "エンジニア"}, []Value{"田中",  34, "デザイナー"}, []Value{"佐藤",  21, "マネージャー"}}}

	staff := fopen("staff.csv")
	defer staff.Close()
	original, err := NewCSVRelationalStream(staff)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(original, SELECT_FROM_Staff) {
		t.Errorf("Does not match 'SELECT * FROM Staff' original:%# v, want:%# v", original.Data, SELECT_FROM_Staff.Data)
	}
}

func TestCSVRelationalStream_Rank(t *testing.T) {
	var SELECT_FROM_Rank = &Relation{
		index: 0,
		Attrs: Schema{Attr{"name", reflect.String}, Attr{"rank", reflect.Int64}},
		Data: [][]Value{
			[]Value{"清水", int64(78)},
			[]Value{"田中", int64(46)},
			[]Value{"佐藤", int64(33)},
		},
	}
	rank := fopen("rank.csv")
	defer rank.Close()
	original, err := NewCSVRelationalStream(rank)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(original, SELECT_FROM_Rank) {
		t.Errorf("Does not match 'SELECT * FROM Rank' original:%# v, want:%# v", original, SELECT_FROM_Rank)
	}
}

func TestSelectionStream(t *testing.T) {
	var SELECT_FROM_Staff_WHERE_age_20 = &Relation{
		index: 0,
		Attrs: Schema{Attr{"name", reflect.String}, Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Data: [][]Value{
			[]Value{"田中", int64(34), "デザイナー"},
			[]Value{"佐藤", int64(21), "マネージャー"},
		},
	}
	staff := fopen("staff.csv")
	defer staff.Close()
	stream1, err := NewCSVRelationalStream(staff)
	if err != nil {
		t.Error(err)
	}
	stream2 := &SelectionStream{stream1, "age", GreaterThan, 20}
	result := StreamToRelation(stream2)
	if !reflect.DeepEqual(result, SELECT_FROM_Staff_WHERE_age_20) {
		t.Errorf("Does not match 'SELECT * FROM Staff WHERE age > 20'\nresult:% #v,\n want:% #v", result, SELECT_FROM_Staff_WHERE_age_20)
	}
}

func TestProjectionStream(t *testing.T) {
	var SELECT_age_job_FROM_Staff = &Relation{
		index: 0,
		Attrs: Schema{Attr{"age", reflect.Int64}, Attr{"job", reflect.String}},
		Data: [][]Value{
			[]Value{int64(17), "エンジニア"},
			[]Value{int64(34), "デザイナー"},
			[]Value{int64(21), "マネージャー"},
		},
	}
	staff := fopen("staff.csv")
	defer staff.Close()
	stream1, err := NewCSVRelationalStream(staff)
	if err != nil {
		t.Error(err)
	}
	stream2 := &ProjectionStream{stream1, []string{"age", "job"}}
	result := StreamToRelation(stream2)
	if !reflect.DeepEqual(result, SELECT_age_job_FROM_Staff) {
		t.Errorf("Does not match 'SELECT age,job FROM Staff'\nresult:% #v,\n want:% #v", result, SELECT_age_job_FROM_Staff)
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
	staff := fopen("staff.csv")
	defer staff.Close()
	rank := fopen("rank.csv")
	defer rank.Close()
	stream1, err := NewCSVRelationalStream(staff)
	if err != nil {
		t.Error(err)
	}
	stream2, err := NewCSVRelationalStream(rank)
	if err != nil {
		t.Error(err)
	}
	stream3 := &JoinStream{Input1: stream1, Attr1: "name", Input2: stream2, Attr2: "name", Selector: Equal}
	result := StreamToRelation(stream3)
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
	staff := fopen("staff.csv")
	defer staff.Close()
	rank := fopen("rank.csv")
	defer rank.Close()
	stream1, err := NewCSVRelationalStream(staff)
	if err != nil {
		t.Error(err)
	}
	stream2, err := NewCSVRelationalStream(rank)
	if err != nil {
		t.Error(err)
	}
	stream3 := &CrossJoinStream{Input1: stream1, Input2: &RenameStream{stream2, "name", "name2"}}
	result := StreamToRelation(stream3)
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
	staff := fopen("staff.csv")
	defer staff.Close()
	rank := fopen("rank.csv")
	defer rank.Close()
	stream1, err := NewCSVRelationalStream(staff)
	if err != nil {
		t.Error(err)
	}
	stream2, err := NewCSVRelationalStream(rank)
	if err != nil {
		t.Error(err)
	}
	stream3 := &CrossJoinStream{Input1: stream1, Input2: &RenameStream{stream2, "name", "name2"}}
	stream4 := &SelectionStream{stream3, "age", GreaterThan, 100}
	result := StreamToRelation(stream4)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT * FROM Staff CROSS JOIN Rank where age > 100' result:% #v", result)
	}
}

func TestStreamToRelation(t *testing.T) {
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
	staff := fopen("staff.csv")
	defer staff.Close()
	rank := fopen("rank.csv")
	defer rank.Close()
	stream1, err := NewCSVRelationalStream(staff)
	if err != nil {
		t.Error(err)
	}
	stream2, err := NewCSVRelationalStream(rank)
	if err != nil {
		t.Error(err)
	}
	relation1 := StreamToRelation(stream1)
	relation2 := StreamToRelation(stream2)
	stream3 := &CrossJoinStream{Input1: relation1, Input2: &RenameStream{relation2, "name", "name2"}}
	result := StreamToRelation(stream3)
	if !reflect.DeepEqual(result, want) {
		t.Errorf("Does not match 'SELECT * FROM Staff CROSS JOIN Rank' result:% #v", result)
	}
}

func fopen(fn string) *os.File {
	f, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	return f
}
