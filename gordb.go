// go-rdb
package gordb

const TupleCapacity = 100

// Selection
type SelectionStream struct {
	Input    Stream   `json:"input"`
	Attr     string   `json:"attr"`
	Selector Operator `json:"selector"`
	Arg      Value    `json:"arg"`
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

func (s *SelectionStream) Init(n *Node) error {
	return s.Input.Init(n)
}

func (s *SelectionStream) Close() {
	s.Input.Close()
}

// Projection
type ProjectionStream struct {
	Input Stream   `json:"input"`
	Attrs []string `json:"attrs"`
}

func (s *ProjectionStream) Next() *Tuple {
	tuple := s.Input.Next()
	result := NewTuple()
	for _, Attr := range s.Attrs {
		result.Set(tuple.GetAttr(Attr), tuple.Get(Attr))
	}
	return result
}
func (s *ProjectionStream) HasNext() bool {
	return s.Input.HasNext()
}
func (s *ProjectionStream) Init(n *Node) error {
	return s.Input.Init(n)
}

func (s *ProjectionStream) Close() {
	s.Input.Close()
}

// Rename
type RenameStream struct {
	Input Stream `json:"input"`
	Attr  string `json:"from"`
	Name  string `json:"to"`
}

func (s *RenameStream) Next() *Tuple {
	result := NewTuple()
	tuple := s.Input.Next()
	tuple.Iterator(func(i int, attr Attr, value Value) error {
		if attr.Name == s.Attr {
			result.Set(Attr{s.Name, attr.Kind}, value)
			return nil
		}
		result.Set(attr, value)
		return nil
	})
	return result

}
func (s *RenameStream) HasNext() bool {
	return s.Input.HasNext()
}
func (s *RenameStream) Init(n *Node) error {
	return s.Input.Init(n)
}
func (s *RenameStream) Close() {
	s.Input.Close()
}

// Union
type UnionStream struct {
	Input1 Stream `json:"input1"`
	Input2 Stream `json:"input2"`
}

func (s *UnionStream) Next() *Tuple {
	switch {
	case s.Input1.HasNext():
		return s.Input1.Next()
	case s.Input2.HasNext():
		return s.Input2.Next()
	}
	return nil
}
func (s *UnionStream) HasNext() bool {
	switch {
	case s.Input1.HasNext():
		return true
	case s.Input2.HasNext():
		return true
	}
	return false
}
func (s *UnionStream) Init(n *Node) error {
	if err := s.Input1.Init(n); err != nil {
		return err
	}
	return s.Input2.Init(n)
}
func (s *UnionStream) Close() {
	s.Input1.Close()
	s.Input2.Close()
}

// Join
type JoinStream struct {
	Input1   Stream   `json:"input1"`
	Input2   Stream   `json:"input2"`
	Attr1    string   `json:"attr1"`
	Attr2    string   `json:"attr2"`
	Selector Operator `json:"selector"`

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
		s.currentTuple.Iterator(func(i int, f Attr, value Value) error {
			result.Set(f, value)
			return nil
		})
		targetTuple.Iterator(func(i int, f Attr, value Value) error {
			result.Set(f, value)
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
func (s *JoinStream) Init(n *Node) error {
	if err := s.Input1.Init(n); err != nil {
		return err
	}
	return s.Input2.Init(n)
}
func (s *JoinStream) Close() {
	s.Input1.Close()
	s.Input2.Close()
}

// CrossJoin
type CrossJoinStream struct {
	Input1 Stream `json:"input1"`
	Input2 Stream `json:"input2"`

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
	s.currentTuple.Iterator(func(i int, f Attr, value Value) error {
		result.Set(f, value)
		return nil
	})
	targetTuple.Iterator(func(i int, f Attr, value Value) error {
		result.Set(f, value)
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
func (s *CrossJoinStream) Init(n *Node) error {
	if err := s.Input1.Init(n); err != nil {
		return err
	}
	return s.Input2.Init(n)
}
func (s *CrossJoinStream) Close() {
	s.Input1.Close()
	s.Input2.Close()
}

func StreamToRelation(s Stream, n *Node) (*Relation, error) {
	err := s.Init(n)
	if err != nil {
		return nil, err
	}
	result := &Relation{
		Attrs: make(Schema, 0, TupleCapacity),
		Data:  make([][]Value, 0, TupleCapacity),
	}
	lastRow := NewTuple()
	for s.HasNext() {
		row := s.Next()
		if row == nil {
			continue
		}
		lastRow = row
		result.Data = append(result.Data, makeValues(lastRow))
	}
	result.Attrs = lastRow.Attrs()
	s.Close()
	return result, nil
}

func makeValues(t *Tuple) []Value {
	m := make([]Value, 0, t.Len())
	for _, col := range t.Attrs() {
		m = append(m, t.Get(col.Name))
	}
	return m
}
