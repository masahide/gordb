// go-rdb
package core

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
