// go-rdb
package core

// Union
type UnionStream struct {
	Input1 Stream `json:"input1"`
	Input2 Stream `json:"input2"`
}

func (s *UnionStream) Next() (*Tuple, error) {
	switch {
	case s.Input1.HasNext():
		return s.Input1.Next()
	case s.Input2.HasNext():
		return s.Input2.Next()
	}
	return nil, nil
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
