package gordb

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/k0kubun/pp"
)

func TestCSVRelationalStream_Staff(t *testing.T) {
	var SELECT_FROM_Staff = &Relation{
		index: 0,
		Field: []string{"name", "age", "job"},
		Data: [][]Value{
			[]Value{"清水", "17", "エンジニア"},
			[]Value{"田中", "34", "デザイナー"},
			[]Value{"佐藤", "21", "マネージャー"},
		},
	}

	staff := fopen("staff.csv")
	defer staff.Close()
	original := NewCSVRelationalStream(staff)
	if !reflect.DeepEqual(original, SELECT_FROM_Staff) {
		t.Errorf("Does not match 'SELECT * FROM Staff' original:% #v", original)
	}
}

func TestCSVRelationalStream_Rank(t *testing.T) {
	var SELECT_FROM_Rank = &Relation{
		index: 0,
		Field: []string{"name", "rank"},
		Data: [][]Value{
			[]Value{"清水", "78"},
			[]Value{"田中", "46"},
			[]Value{"佐藤", "33"},
		},
	}
	rank := fopen("rank.csv")
	defer rank.Close()
	original := NewCSVRelationalStream(rank)
	if !reflect.DeepEqual(original, SELECT_FROM_Rank) {
		t.Errorf("Does not match 'SELECT * FROM Rank' original:% #v", original)
	}
}

func TestSelectionStream(t *testing.T) {
	var SELECT_FROM_Staff_WHERE_age_20 = &Relation{
		index: 0,
		Field: []string{"name", "age", "job"},
		Data: [][]Value{
			[]Value{"田中", "34", "デザイナー"},
			[]Value{"佐藤", "21", "マネージャー"},
		},
	}
	staff := fopen("staff.csv")
	defer staff.Close()
	relation1 := NewCSVRelationalStream(staff)
	relation2 := &SelectionStream{relation1, "age", GreaterThan, "20"}
	result := StreamToRelation(relation2)
	if !reflect.DeepEqual(result, SELECT_FROM_Staff_WHERE_age_20) {
		t.Errorf("Does not match 'SELECT * FROM Staff WHERE age > 20'\nresult:% #v,\n want:% #v", result, SELECT_FROM_Staff_WHERE_age_20)
	}
	pp.Print()
}

func TestProjectionStream(t *testing.T) {
	var SELECT_age_job_FROM_Staff = &Relation{
		index: 0,
		Field: []string{"age", "job"},
		Data: [][]Value{
			[]Value{"17", "エンジニア"},
			[]Value{"34", "デザイナー"},
			[]Value{"21", "マネージャー"},
		},
	}
	staff := fopen("staff.csv")
	defer staff.Close()
	relation1 := NewCSVRelationalStream(staff)
	relation2 := &ProjectionStream{relation1, []string{"age", "job"}}
	result := StreamToRelation(relation2)
	if !reflect.DeepEqual(result, SELECT_age_job_FROM_Staff) {
		t.Errorf("Does not match 'SELECT age,job FROM Staff'\nresult:% #v,\n want:% #v", result, SELECT_age_job_FROM_Staff)
	}
}

func TestJoinStream(t *testing.T) {
	var SELECT_FROM_Staff_Rank_WHERE_staff_name_rank_name = &Relation{
		index: 0,
		Field: []string{"name", "age", "job", "rank"},
		Data: [][]Value{
			[]Value{"清水", "17", "エンジニア", "78"},
			[]Value{"田中", "34", "デザイナー", "46"},
			[]Value{"佐藤", "21", "マネージャー", "33"},
		},
	}
	staff := fopen("staff.csv")
	defer staff.Close()
	rank := fopen("rank.csv")
	defer rank.Close()
	relation1 := NewCSVRelationalStream(staff)
	relation2 := NewCSVRelationalStream(rank)
	relation3 := &JoinStream{Input1: relation1, Attr1: "name", Input2: relation2, Attr2: "name", Selector: Equal}
	result := StreamToRelation(relation3)
	if !reflect.DeepEqual(result, SELECT_FROM_Staff_Rank_WHERE_staff_name_rank_name) {
		t.Errorf("Does not match 'SELECT * FROM Staff, Rank WHERE staff.name = rank.name' result:% #v", result)
	}
}

func TestCrossJoinStream(t *testing.T) {
	var SELECT_FROM_Staff_CROSS_JOIN_Rank = &Relation{
		index: 0,
		Field: []string{"name", "age", "job", "name2", "rank"},
		Data: [][]Value{
			[]Value{"清水", "17", "エンジニア", "清水", "78"},
			[]Value{"清水", "17", "エンジニア", "田中", "46"},
			[]Value{"清水", "17", "エンジニア", "佐藤", "33"},
			[]Value{"田中", "34", "デザイナー", "清水", "78"},
			[]Value{"田中", "34", "デザイナー", "田中", "46"},
			[]Value{"田中", "34", "デザイナー", "佐藤", "33"},
			[]Value{"佐藤", "21", "マネージャー", "清水", "78"},
			[]Value{"佐藤", "21", "マネージャー", "田中", "46"},
			[]Value{"佐藤", "21", "マネージャー", "佐藤", "33"},
		},
	}
	staff := fopen("staff.csv")
	defer staff.Close()
	rank := fopen("rank.csv")
	defer rank.Close()
	relation1 := NewCSVRelationalStream(staff)
	relation2 := NewCSVRelationalStream(rank)
	relation3 := &CrossJoinStream{Input1: relation1, Input2: &RenameStream{relation2, "name", "name2"}}
	result := StreamToRelation(relation3)
	fmt.Println("")
	if !reflect.DeepEqual(result, SELECT_FROM_Staff_CROSS_JOIN_Rank) {
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
