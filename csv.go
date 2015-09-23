package gordb

import (
	"encoding/csv"
	"io"
)

func recordToData(records [][]string) [][]Value {
	result := make([][]Value, len(records))
	for i, row := range records {
		result[i] = make([]Value, len(row))
		for j, v := range row {
			result[i][j] = Value(v)
		}
	}
	return result
}

// CSVRelational
func NewCSVRelationalStream(r io.Reader) *Relation {
	reader := csv.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}
	return &Relation{
		Field: rows[0],
		Data:  recordToData(rows[1:]),
	}
}
