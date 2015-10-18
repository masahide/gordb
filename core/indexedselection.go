// go-rdb
package core

import "reflect"

// Selection
type IndexedSelectionStream struct {
	Input             *Relation       `json:"input"`
	Attr              string          `json:"attr"`
	Selector          IndexedOperator `json:"selector"`
	Arg               Value           `json:"arg"`
	indexSearchResult []int
	index             int
	inputKind         reflect.Kind
}

func (s *IndexedSelectionStream) Next() (*Tuple, error) {
	ptr := s.indexSearchResult[s.index]
	tuple := &Tuple{
		Schema: s.Input.Attrs,
		Data:   s.Input.Data[ptr],
	}
	s.Input.index++
	return tuple, nil
}
func (s *IndexedSelectionStream) HasNext() bool {
	return s.Input.index < len(s.indexSearchResult)
}

func (s *IndexedSelectionStream) Init(n *Node) error {
	s.index = 0
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
	err := s.Input.Init(n)
	if err != nil {
		return err
	}
	if s.inputKind == 0 {
		s.inputKind = s.Input.Attrs.GetKind(s.Attr)
	}
	kind := CheckType(s.Arg)
	if kind != s.inputKind {
		return ErrDifferentType
	}
	s.indexSearchResult = s.Selector(s.Input, s.Attr, s.Arg, kind)
	return nil
}

func (s *IndexedSelectionStream) Close() {
	s.Input.Close()
}
