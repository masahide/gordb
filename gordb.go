// go-rdb
package gordb

import (
	"encoding/csv"
	"io"
	"strconv"
)

type Value string
type Tuple map[string]Value

func vtoi(s Value) int {
	i, err := strconv.Atoi(string(s))
	if err != nil {
		return 0
	}
	return i
}

type Operator func(Value, Value) bool

func greaterThan(a, b Value) bool {
	return vtoi(a) > vtoi(b)
}
func lessThan(a, b Value) bool {
	return vtoi(a) < vtoi(b)
}
func equal(a, b Value) bool {
	return vtoi(a) == vtoi(b)
}

type Stream interface {
	Next() Tuple
	HasNext() bool
	Close()
}

type CSVRelationalStream struct {
	index  int
	header []string
	data   [][]Value
}

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

func NewCSVRelationalStream(r io.Reader) *CSVRelationalStream {
	reader := csv.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}
	return &CSVRelationalStream{
		index:  0,
		header: rows[0],
		data:   recordToData(rows),
	}
}

func (s *CSVRelationalStream) Next() Tuple {
	tuple := Tuple{}
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

type SelectionStream struct {
	input     Stream
	attribute string
	selector  Operator
	arg       Value
}

func NewSelectionStream(input Stream, attribute string, selector Operator, arg string) *SelectionStream {
	return &SelectionStream{input, attribute, selector, Value(arg)}
}

func (s *SelectionStream) Next() Tuple {
	tuple := s.input.Next()
	if s.selector(tuple[s.attribute], s.arg) {
		return tuple
	}
	if s.input.HasNext() {
		return s.Next()
	}
	return nil
}
func (s *SelectionStream) HasNext() bool {
	return s.input.HasNext()
}

func (s *SelectionStream) Close() {
	s.input.Close()
}

type ProjectionStream struct {
	input      Stream
	attributes []string
}

func NewProjectionStream(input Stream, attributes []string) *ProjectionStream {
	return &ProjectionStream{input, attributes}
}

func (s *ProjectionStream) Next() Tuple {
	tuple := s.input.Next()
	result := Tuple{}
	for _, attribute := range s.attributes {
		result[attribute] = tuple[attribute]
	}
	return result
}
func (s *ProjectionStream) HasNext() bool {
	return s.input.HasNext()
}

func (s *ProjectionStream) Close() {
	s.input.Close()
}
