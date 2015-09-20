// go-rdb
package gordb

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
)

type Value interface{}

type Tuple struct {
	index []string
	data  map[string]Value
}

func NewTuple() *Tuple {
	return &Tuple{index: make([]string, 0, 100), data: map[string]Value{}}
}
func (t *Tuple) Set(key string, value Value) {
	if _, ok := t.data[key]; !ok {
		t.index = append(t.index, key)
	}
	t.data[key] = value
}
func (t *Tuple) Get(key string) Value {
	v, _ := t.data[key]
	return v
}

func (t *Tuple) Len() int {
	return len(t.data)
}

func (t *Tuple) Index() []string {
	return t.index
}

func (t *Tuple) Iterator(cb func(i int, key string, value Value) error) error {
	for i, key := range t.Index() {
		if err := cb(i, key, t.data[key]); err != nil {
			return err
		}
	}
	return nil
}

func vtof(s Value) (f float64) {
	switch t := s.(type) {
	case int:
		f = float64(t)
	case float64:
		f = t
	case float32:
		f = float64(t)
	case string:
		i, err := strconv.Atoi(t)
		if err != nil {
			i = 0
		}
		f = float64(i)
	case bool:
		if t {
			f = float64(1)
		}
	default:
	}
	return f
}

type Operator func(Value, Value) bool

func GreaterThan(a, b Value) bool {
	return vtof(a) > vtof(b)
}
func LessThan(a, b Value) bool {
	return vtof(a) < vtof(b)
}
func Equal(a, b Value) bool {
	return vtof(a) == vtof(b)
}

type Stream interface {
	Next() *Tuple
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
func (s *CSVRelationalStream) Next() *Tuple {
	tuple := NewTuple()
	s.index++
	for i, key := range s.header {
		tuple.Set(key, s.data[s.index][i])
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

func (s *SelectionStream) Next() *Tuple {
	tuple := s.Input.Next()
	if s.Selector(tuple.Get(s.Attr), s.Arg) {
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

func (s *ProjectionStream) Next() *Tuple {
	tuple := s.Input.Next()
	result := NewTuple()
	for _, Attr := range s.Attrs {
		result.Set(Attr, tuple.Get(Attr))
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

func (s *RenameStream) Next() *Tuple {
	result := NewTuple()
	tuple := s.Input.Next()
	tuple.Iterator(func(i int, key string, value Value) error {
		if key == s.Attr {
			result.Set(s.Name, value)
			return nil
		}
		result.Set(key, value)
		return nil
	})
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

func (s *UnionStream) Next() *Tuple {
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
	tuples       []*Tuple
	currentTuple *Tuple
}

func (s *JoinStream) Next() *Tuple {
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
	if s.Selector(s.currentTuple.Get(s.Attr1), targetTuple.Get(s.Attr2)) {
		result := NewTuple()
		s.currentTuple.Iterator(func(i int, key string, value Value) error {
			result.Set(key, value)
			return nil
		})
		targetTuple.Iterator(func(i int, key string, value Value) error {
			result.Set(key, value)
			return nil
		})
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
		s.tuples = []*Tuple{}
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
	tuples       []*Tuple
	currentTuple *Tuple
}

func (s *CrossJoinStream) Next() *Tuple {
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
	result := NewTuple()
	s.currentTuple.Iterator(func(i int, key string, value Value) error {
		result.Set(key, value)
		return nil
	})
	targetTuple.Iterator(func(i int, key string, value Value) error {
		result.Set(key, value)
		return nil
	})
	return result
}
func (s *CrossJoinStream) HasNext() bool {
	if s.tuples == nil {
		s.tuples = []*Tuple{}
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

func StreamToString(s Stream) string {
	out := ""
	isHeaderWritten := false
	for s.HasNext() {
		row := s.Next()
		if !isHeaderWritten {
			out += fmt.Sprintf("|")
			for _, col := range row.Index() {
				out += fmt.Sprintf("%14s|", col)
			}
			out += fmt.Sprintf("\n")
			isHeaderWritten = true
		}
		out += fmt.Sprintf("|")
		for _, col := range row.Index() {
			out += fmt.Sprintf("%14s|", row.Get(col))
		}
		out += fmt.Sprintf("\n")
	}
	s.Close()
	return out
}
