// go-rdb
package core

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
