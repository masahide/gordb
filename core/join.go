// go-rdb
package core

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
