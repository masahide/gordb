// go-rdb
package core

// Rename
type RenameStream struct {
	Input Stream `json:"input"`
	Attr  string `json:"from"`
	Name  string `json:"to"`
}

func (s *RenameStream) Next() (*Tuple, error) {
	result := NewTuple()
	tuple, err := s.Input.Next()
	if err != nil {
		return tuple, err
	}
	for _, attr := range tuple.Attrs {
		if attr.Name == s.Attr {
			result.Set(Attr{s.Name, attr.Kind}, tuple.Data[attr.Name])
		} else {
			result.Set(attr, tuple.Data[attr.Name])
		}
	}
	return result, err

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
