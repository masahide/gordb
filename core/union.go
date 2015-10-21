// go-rdb
package core

// Union
type UnionStream struct {
	Inputs []Stream `json:"inputs"`
}

func (s *UnionStream) Next() (*Tuple, error) {
	for i := range s.Inputs {
		if s.Inputs[i].HasNext() {
			return s.Inputs[i].Next()
		}
	}
	return nil, nil
}
func (s *UnionStream) HasNext() bool {
	for i := range s.Inputs {
		if s.Inputs[i].HasNext() {
			return true
		}
	}
	return false
}
func (s *UnionStream) Init(n *Node) error {
	for i := range s.Inputs {
		if err := s.Inputs[i].Init(n); err != nil {
			return err
		}
	}
	return nil
}
func (s *UnionStream) Close() {
	for i := range s.Inputs {
		s.Inputs[i].Close()
	}
}
