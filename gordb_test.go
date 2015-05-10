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
	printData(original)
	rank := fopen("rank.csv")
	defer rank.Close()
	original = NewCSVRelationalStream(rank)
	fmt.Println("SELECT * FROM Rank")
	printData(original)
}

func TestSelectionStream(t *testing.T) {
	staff := fopen("staff.csv")
	defer staff.Close()
	fmt.Println("SELECT * FROM Staff WHERE age > 20")
	relation1 := NewCSVRelationalStream(staff)
	relation2 := NewSelectionStream(relation1, "age", greaterThan, "20")
	printData(relation2)
}

func TestProjectionStream(t *testing.T) {
	staff := fopen("staff.csv")
	defer staff.Close()
	fmt.Println("SELECT age,job FROM Staff")
	relation1 := NewCSVRelationalStream(staff)
	relation2 := NewProjectionStream(relation1, []string{"age", "job"})
	printData(relation2)
}

func TestJoinStream(t *testing.T) {
	staff := fopen("staff.csv")
	defer staff.Close()
	rank := fopen("rank.csv")
	defer rank.Close()
	fmt.Println("SELECT * FROM Staff, Rank WHERE staff.name = rank.name")
	relation1 := NewCSVRelationalStream(staff)
	relation2 := NewCSVRelationalStream(rank)
	relation3 := NewJoinStream(relation1, "name", relation2, "name", equal)
	printData(relation3)
}

func printData(s Stream) {
	var cols []string
	isHeaderWritten := false
	for s.HasNext() {
		row := s.Next()
		if !isHeaderWritten {
			cols = make([]string, 0, len(row))
			for col, _ := range row {
				cols = append(cols, col)
			}
			fmt.Printf("|")
			for _, col := range cols {
				fmt.Printf("%14s|", col)
			}
			fmt.Printf("\n")
			isHeaderWritten = true
		}
		fmt.Printf("|")
		for _, col := range cols {
			fmt.Printf("%14s|", row[col])
		}
		fmt.Printf("\n")
	}
	s.Close()
}

func fopen(fn string) *os.File {
	f, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	return f
}
