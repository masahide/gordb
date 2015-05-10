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

// CSVRelational
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

// Selection
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

// Projection
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

// Rename
type RenameStream struct {
	input     Stream
	attribute string
	name      string
}

func NewRenameStream(input Stream, attribute, name string) *RenameStream {
	return &RenameStream{input, attribute, name}
}
func (s *RenameStream) Next() Tuple {
	result := Tuple{}
	tuple := s.input.Next()
	for key := range tuple {
		if key == s.attribute {
			result[s.name] = tuple[key]
			continue
		}
		result[key] = tuple[key]
	}
	return result

}
func (s *RenameStream) HasNext() bool {
	return s.input.HasNext()
}
func (s *RenameStream) Close() {
	s.input.Close()
}

// Union
type UnionStream struct {
	input1 Stream
	input2 Stream
}

func NewUnionStream(input1, input2 Stream) *UnionStream {
	return &UnionStream{input1, input2}
}
func (s *UnionStream) Next() Tuple {
	if s.input1.HasNext() {
		return s.input1.Next()
	} else if s.input2.HasNext() {
		return s.input2.Next()
	}
	return nil
}
func (s *UnionStream) HasNext() bool {
	if s.input1.HasNext() {
		return true
	} else if s.input2.HasNext() {
		return true
	}
	return false
}
func (s *UnionStream) Close() {
	s.input1.Close()
	s.input2.Close()
}

// Join
type JoinStream struct {
	input1, input2         Stream
	attribute1, attribute2 string
	selector               Operator

	index        int
	tuples       []Tuple
	currentTuple Tuple
}

func NewJoinStream(input1 Stream, attribute1 string, input2 Stream, attribute2 string, selector Operator) *JoinStream {
	return &JoinStream{
		input1:     input1,
		input2:     input2,
		attribute1: attribute1,
		attribute2: attribute2,
		selector:   selector,
	}
}
func (s *JoinStream) Next() Tuple {
	if len(s.tuples) <= s.index {
		s.index = 0
		s.currentTuple = nil
	}
	if s.currentTuple == nil {
		if s.input1.HasNext() {
			s.currentTuple = s.input1.Next()
		}
		if s.currentTuple == nil {
			return nil
		}
	}
	targetTuple := s.tuples[s.index]
	s.index++
	if s.selector(s.currentTuple[s.attribute1], targetTuple[s.attribute2]) {
		result := Tuple{}
		for key := range s.currentTuple {
			result[key] = s.currentTuple[key]
		}
		for key := range targetTuple {
			result[key] = targetTuple[key]
		}
		return result
	}
	if s.HasNext() {
		return s.Next()
	} else {
		return nil
	}
}
func (s *JoinStream) HasNext() bool {
	if s.tuples == nil {
		s.tuples = []Tuple{}
		for s.input2.HasNext() {
			s.tuples = append(s.tuples, s.input2.Next())
		}
	}
	if len(s.tuples) > s.index {
		return true
	}
	return s.input1.HasNext()
}
func (s *JoinStream) Close() {
	s.input1.Close()
	s.input2.Close()
}
