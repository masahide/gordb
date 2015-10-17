// go-rdb
package core

import "reflect"

// Selection
type SelectionStream struct {
	Input     Stream   `json:"input"`
	Attr      string   `json:"attr"`
	Selector  Operator `json:"selector"`
	Arg       Value    `json:"arg"`
	kind      reflect.Kind
	inputKind reflect.Kind
}

func (s *SelectionStream) Next() (*Tuple, error) {
	tuple, err := s.Input.Next()
	if err != nil {
		return nil, err
	}
	if s.inputKind == 0 {
		s.inputKind = tuple.Schema.GetKind(s.Attr)
	}
	result, err := s.Selector(s.inputKind, tuple.Get(s.Attr), s.kind, s.Arg)
	if err != nil {
		return nil, err
	}
	if result {
		return tuple, nil
	}
	if s.Input.HasNext() {
		return s.Next()
	}
	return nil, nil
}
func (s *SelectionStream) HasNext() bool {
	return s.Input.HasNext()
}

func (s *SelectionStream) Init(n *Node) error {
	switch t := s.Arg.(type) {
	case int:
		s.Arg = int64(t)
	case int64:
		s.Arg = t
	case float64:
		if s.Arg == float64(t) {
			s.Arg = int64(t)
		}
	case string:
		s.Arg = t
	case bool:
		s.Arg = int64(0)
		if t {
			s.Arg = int64(1)
		}
	}
	s.kind = CheckType(s.Arg)
	return s.Input.Init(n)
}

func (s *SelectionStream) Close() {
	s.Input.Close()
}
