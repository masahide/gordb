// go-rdb
package gordb

import (
	"encoding/csv"
	"io"
	"strconv"
)

type Operator func(string, string) bool

func atoi(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return i
}
func greaterThan(a, b string) bool {
	return atoi(a) > atoi(b)
}
func lessThan(a, b string) bool {
	return atoi(a) < atoi(b)
}
func equal(a, b string) bool {
	return atoi(a) == atoi(b)
}

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

type SelectionStream struct {
	input     Stream
	attribute string
	selector  Operator
	arg       string
}

func NewSelectionStream(input Stream, attribute string, selector Operator, arg string) *SelectionStream {
	return &SelectionStream{input, attribute, selector, arg}
}

func (s *SelectionStream) Next() map[string]string {
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

func (s *ProjectionStream) Next() map[string]string {
	tuple := s.input.Next()
	result := map[string]string{}
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
