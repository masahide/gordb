// go-rdb
package core

// CrossJoin
type CrossJoinStream struct {
	Input1 Stream `json:"input1"`
	Input2 Stream `json:"input2"`

	index        int
	tuples       []*Tuple
	currentTuple *Tuple
}

func (s *CrossJoinStream) Next() (*Tuple, error) {
	var err error
	if len(s.tuples) <= s.index {
		s.index = 0
		s.currentTuple = nil
	}
	if s.currentTuple == nil {
		if s.Input1.HasNext() {
			s.currentTuple, err = s.Input1.Next()
		}
		if s.currentTuple == nil || err != nil {
			return nil, err
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
	return result, nil
}
func (s *CrossJoinStream) HasNext() bool {
	if s.tuples == nil {
		s.tuples = make([]*Tuple, 0, TupleCapacity)
		for s.Input2.HasNext() {
			next, err := s.Input2.Next()
			if err != nil {
				continue
			}
			s.tuples = append(s.tuples, next)
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
