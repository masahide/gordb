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

func (s *JoinStream) Next() (result *Tuple, err error) {
	if len(s.tuples) <= s.index {
		s.index = 0
		s.currentTuple = nil
	}
	if s.currentTuple == nil {
		if s.Input1.HasNext() {
			s.currentTuple, err = s.Input1.Next()
		}
		if s.currentTuple == nil {
			return
		}
	}
	targetTuple := s.tuples[s.index]
	s.index++
	res, err := s.Selector(s.currentTuple.Get(s.Attr1), targetTuple.Get(s.Attr2))
	if err != nil {
		return
	}
	if res {
		result = NewTuple()
		for _, attr := range s.currentTuple.Attrs {
			result.Set(attr, s.currentTuple.Data[attr.Name])
		}
		for _, attr := range targetTuple.Attrs {
			result.Set(attr, targetTuple.Data[attr.Name])
		}
		return
	}
	if s.HasNext() {
		return s.Next()
	}
	return nil, nil
}
func (s *JoinStream) HasNext() bool {
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
