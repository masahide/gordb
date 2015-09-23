// go-rdb
package gordb

const TupleCapacity = 100

type Stream interface {
	Next() *Tuple
	HasNext() bool
	Close()
}

type Relation struct {
	index int
	Field []string
	Data  [][]Value
}

func (r *Relation) HasNext() bool {
	return r.index < len(r.Data)
}
func (r *Relation) Close() {
	r.index = 0
}

func (r *Relation) Next() *Tuple {
	tuple := NewTuple()
	for i, key := range r.Field {
		tuple.Set(key, r.Data[r.index][i])
	}
	r.index++
	return tuple
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
		s.tuples = make([]*Tuple, 0, TupleCapacity)
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
		s.tuples = make([]*Tuple, 0, TupleCapacity)
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

func StreamToRelation(s Stream) *Relation {
	result := &Relation{
		Field: nil,
		Data:  make([][]Value, 0, TupleCapacity),
	}

	if !s.HasNext() {
		return result
	}
	row := s.Next()
	result.Field = makeField(row.Headers())
	for {
		result.Data = append(result.Data, makeValues(row))
		if !s.HasNext() {
			break
		}
		row = s.Next()
	}
	s.Close()
	return result
}

func makeField(headers []string) []string {
	field := make([]string, 0, TupleCapacity)
	for _, col := range headers {
		field = append(field, col)
	}
	return field
}
func makeValues(t *Tuple) []Value {
	m := make([]Value, 0, t.Len())
	for _, col := range t.Headers() {
		m = append(m, t.Get(col))
	}
	return m
}
