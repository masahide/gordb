// go-rdb
package gordb

import (
	"encoding/csv"
	"io"
)

type Stream interface {
	Next() map[string]string
	HasNext() bool
	Close()
}

type CSVRelationalStream struct {
	index  int
	header []string
	data   [][]string
}

func NewCSVRelationalStream(r io.Reader) *CSVRelationalStream {
	reader := csv.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}
	return &CSVRelationalStream{
		index:  0,
		header: rows[0],
		data:   rows,
	}
}

func (s *CSVRelationalStream) Next() map[string]string {
	tuple := map[string]string{}
	s.index++
	for i, key := range s.header {
		tuple[key] = s.data[s.index][i]
	}
	return tuple
}

func (s *CSVRelationalStream) HasNext() bool {
	return (s.index + 1) < len(s.data)
}
func (s *CSVRelationalStream) Close() {
	s.data = nil
}
