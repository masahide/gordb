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

func GreaterThan(a, b Value) bool {
	return vtoi(a) > vtoi(b)
}
func LessThan(a, b Value) bool {
	return vtoi(a) < vtoi(b)
}
func Equal(a, b Value) bool {
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
	Input    Stream
	Attr     string
	Selector Operator
	Arg      Value
}

func (s *SelectionStream) Next() Tuple {
	tuple := s.Input.Next()
	if s.Selector(tuple[s.Attr], s.Arg) {
		return tuple
	}
	if s.Input.HasNext() {
		return s.Next()
	}
	return nil
}
func (s *SelectionStream) HasNext() bool {
	return s.Input.HasNext()
}
func (s *SelectionStream) Close() {
	s.Input.Close()
}

// Projection
type ProjectionStream struct {
	Input Stream
	Attrs []string
}

func (s *ProjectionStream) Next() Tuple {
	tuple := s.Input.Next()
	result := Tuple{}
	for _, Attr := range s.Attrs {
		result[Attr] = tuple[Attr]
	}
	return result
}
func (s *ProjectionStream) HasNext() bool {
	return s.Input.HasNext()
}
func (s *ProjectionStream) Close() {
	s.Input.Close()
}

// Rename
type RenameStream struct {
	Input Stream
	Attr  string
	Name  string
}

func (s *RenameStream) Next() Tuple {
	result := Tuple{}
	tuple := s.Input.Next()
	for key := range tuple {
		if key == s.Attr {
			result[s.Name] = tuple[key]
			continue
		}
		result[key] = tuple[key]
	}
	return result

}
func (s *RenameStream) HasNext() bool {
	return s.Input.HasNext()
}
func (s *RenameStream) Close() {
	s.Input.Close()
}

// Union
type UnionStream struct {
	Input1 Stream
	Input2 Stream
}

func (s *UnionStream) Next() Tuple {
	if s.Input1.HasNext() {
		return s.Input1.Next()
	} else if s.Input2.HasNext() {
		return s.Input2.Next()
	}
	return nil
}
func (s *UnionStream) HasNext() bool {
	if s.Input1.HasNext() {
		return true
	} else if s.Input2.HasNext() {
		return true
	}
	return false
}
func (s *UnionStream) Close() {
	s.Input1.Close()
	s.Input2.Close()
}

// Join
type JoinStream struct {
	Input1, Input2 Stream
	Attr1, Attr2   string
	Selector       Operator

	index        int
	tuples       []Tuple
	currentTuple Tuple
}

func (s *JoinStream) Next() Tuple {
	if len(s.tuples) <= s.index {
		s.index = 0
		s.currentTuple = nil
	}
	if s.currentTuple == nil {
		if s.Input1.HasNext() {
			s.currentTuple = s.Input1.Next()
		}
		if s.currentTuple == nil {
			return nil
		}
	}
	targetTuple := s.tuples[s.index]
	s.index++
	if s.Selector(s.currentTuple[s.Attr1], targetTuple[s.Attr2]) {
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
		for s.Input2.HasNext() {
			s.tuples = append(s.tuples, s.Input2.Next())
		}
	}
	if len(s.tuples) > s.index {
		return true
	}
	return s.Input1.HasNext()
}
func (s *JoinStream) Close() {
	s.Input1.Close()
	s.Input2.Close()
}

// CrossJoin
type CrossJoinStream struct {
	Input1, Input2 Stream

	index        int
	tuples       []Tuple
	currentTuple Tuple
}

func (s *CrossJoinStream) Next() Tuple {
	if len(s.tuples) <= s.index {
		s.index = 0
		s.currentTuple = nil
	}
	if s.currentTuple == nil {
		if s.Input1.HasNext() {
			s.currentTuple = s.Input1.Next()
		}
		if s.currentTuple == nil {
			return nil
		}
	}
	targetTuple := s.tuples[s.index]
	s.index++
	result := Tuple{}
	for key := range s.currentTuple {
		result[key] = s.currentTuple[key]
	}
	for key := range targetTuple {
		result[key] = targetTuple[key]
	}
	return result
}
func (s *CrossJoinStream) HasNext() bool {
	if s.tuples == nil {
		s.tuples = []Tuple{}
		for s.Input2.HasNext() {
			s.tuples = append(s.tuples, s.Input2.Next())
		}
	}
	if len(s.tuples) > s.index {
		return true
	}
	return s.Input1.HasNext()
}
func (s *CrossJoinStream) Close() {
	s.Input1.Close()
	s.Input2.Close()
}
