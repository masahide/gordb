package core

import "errors"

type Stream struct {
	stream
	Relation         *Relation               `json:"relation"`
	IndexedSelection *IndexedSelectionStream `json:"iselection"`
	Selection        *SelectionStream        `json:"selection"`
	Projection       *ProjectionStream       `json:"projection"`
	Rename           *RenameStream           `json:"rename"`
	Union            *UnionStream            `json:"union"`
	Join             *JoinStream             `json:"join"`
	CrossJoin        *CrossJoinStream        `json:"crossjoin"`
}

var ErrUnkownStreamType = errors.New("unkown stream type")

func (s *Stream) Init(n *Node) error {
	c := s.getStream()
	if c == nil {
		return ErrUnkownStreamType
	}
	return c.Init(n)
}

func (s *Stream) getStream() stream {
	if s.stream != nil {
		return s.stream
	}
	switch {
	case s.Relation != nil:
		s.stream = s.Relation
	case s.Selection != nil:
		s.stream = s.Selection
	case s.IndexedSelection != nil:
		s.stream = s.IndexedSelection
	case s.Projection != nil:
		s.stream = s.Projection
	case s.Rename != nil:
		s.stream = s.Rename
	case s.Union != nil:
		s.stream = s.Union
	case s.Join != nil:
		s.stream = s.Join
	case s.CrossJoin != nil:
		s.stream = s.CrossJoin
	}
	return s.stream
}

type stream interface {
	Init(*Node) error
	Next() (*Tuple, error)
	HasNext() bool
	Close()
}
