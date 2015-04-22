package gordb

import (
	"fmt"
	"os"
	"testing"
)

func TestNewCSVRelationalStream(t *testing.T) {
	f, err := os.Open("1.csv")
	if err != nil {
		panic(err)
		//t.Errorf("os.Open: %s", err)
	}
	original := NewCSVRelationalStream(f)
	fmt.Println("<p>SELECT * FROM Staff</p>")
	printData(original)
}

func printData(s Stream) {
	isHeaderWritten := false
	for s.HasNext() {
		row := s.Next()
		if !isHeaderWritten {
			fmt.Printf("|")
			for col, _ := range row {
				fmt.Printf("%14s|", col)
			}
			fmt.Printf("\n")
			isHeaderWritten = true
		}
		fmt.Printf("|")
		for col, _ := range row {
			fmt.Printf("%14s|", row[col])
		}
		fmt.Printf("\n")
	}
	s.Close()
}
