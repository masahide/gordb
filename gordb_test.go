package gordb

import (
	"fmt"
	"os"
	"testing"
)

func TestCSVRelationalStream(t *testing.T) {
	staff := fopen("staff.csv")
	defer staff.Close()
	original := NewCSVRelationalStream(staff)
	fmt.Println("SELECT * FROM Staff")
	StreamToString(original)
	rank := fopen("rank.csv")
	defer rank.Close()
	original = NewCSVRelationalStream(rank)
	fmt.Println("SELECT * FROM Rank")
	StreamToString(original)
}

func TestSelectionStream(t *testing.T) {
	staff := fopen("staff.csv")
	defer staff.Close()
	fmt.Println("SELECT * FROM Staff WHERE age > 20")
	relation1 := NewCSVRelationalStream(staff)
	relation2 := &SelectionStream{relation1, "age", GreaterThan, "20"}
	StreamToString(relation2)
}

func TestProjectionStream(t *testing.T) {
	staff := fopen("staff.csv")
	defer staff.Close()
	fmt.Println("SELECT age,job FROM Staff")
	relation1 := NewCSVRelationalStream(staff)
	relation2 := &ProjectionStream{relation1, []string{"age", "job"}}
	StreamToString(relation2)
}

func TestJoinStream(t *testing.T) {
	staff := fopen("staff.csv")
	defer staff.Close()
	rank := fopen("rank.csv")
	defer rank.Close()
	fmt.Println("SELECT * FROM Staff, Rank WHERE staff.name = rank.name")
	relation1 := NewCSVRelationalStream(staff)
	relation2 := NewCSVRelationalStream(rank)
	relation3 := &JoinStream{Input1: relation1, Attr1: "name", Input2: relation2, Attr2: "name", Selector: Equal}
	StreamToString(relation3)
}

func TestCrossJoinStream(t *testing.T) {
	staff := fopen("staff.csv")
	defer staff.Close()
	rank := fopen("rank.csv")
	defer rank.Close()
	fmt.Println("SELECT * FROM Staff CROSS JOIN Rank")
	relation1 := NewCSVRelationalStream(staff)
	relation2 := NewCSVRelationalStream(rank)
	relation3 := &CrossJoinStream{Input1: relation1, Input2: &RenameStream{relation2, "name", "name2"}}
	StreamToString(relation3)
}

func fopen(fn string) *os.File {
	f, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	return f
}
