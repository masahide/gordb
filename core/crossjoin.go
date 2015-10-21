// go-rdb
package core

// CrossJoin
type CrossJoinStream struct {
	Inputs       []Stream `json:"inputs"`
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
		if s.Inputs[0].HasNext() {
			s.currentTuple, err = s.Inputs[0].Next()
		}
		if s.currentTuple == nil || err != nil {
			return nil, err
		}
	}
	targetTuple := s.tuples[s.index]
	s.index++
	result := NewTuple()

	for i, attr := range s.currentTuple.Attrs {
		result.Set(attr, s.currentTuple.Data[i])
	}
	for i, attr := range targetTuple.Attrs {
		result.Set(attr, targetTuple.Data[i])
	}
	return result, nil
}
func (s *CrossJoinStream) HasNext() bool {
	if s.tuples == nil {
		s.tuples = make([]*Tuple, 0, TupleCapacity)
		for s.Inputs[1].HasNext() {
			next, err := s.Inputs[1].Next()
			if err != nil {
				continue
			}
			s.tuples = append(s.tuples, next)
		}
	}
	if len(s.tuples) > s.index {
		return true
	}
	return s.Inputs[0].HasNext()
}
func (s *CrossJoinStream) Init(n *Node) error {
	if len(s.Inputs) != 2 {
		return ErrUnexpectedInputNumber
	}
	if err := s.Inputs[0].Init(n); err != nil {
		return err
	}
	return s.Inputs[1].Init(n)
}
func (s *CrossJoinStream) Close() {
	s.Inputs[0].Close()
	s.Inputs[1].Close()
}
