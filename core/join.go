// go-rdb
package core

import (
	"errors"
	"reflect"
)

var ErrUnexpectedInputNumber = errors.New("Unexpected input number.")
var ErrUnexpectedAttrNumber = errors.New("Unexpected attribute number.")

// Join
type JoinStream struct {
	Inputs   []Stream `json:"inputs"`
	Attrs    []string `json:"attrs"`
	Selector Operator `json:"selector"`

	index        int
	tuples       []*Tuple
	currentTuple *Tuple
	currentKind  reflect.Kind
	targetKind   reflect.Kind
}

func (s *JoinStream) Next() (result *Tuple, err error) {
	if len(s.tuples) <= s.index {
		s.index = 0
		s.currentTuple = nil
	}
	if s.currentTuple == nil {
		if s.Inputs[0].HasNext() {
			s.currentTuple, err = s.Inputs[0].Next()
		}
		if s.currentTuple == nil {
			return
		}
		s.currentKind = s.currentTuple.Schema.GetKind(s.Attrs[0])
	}
	targetTuple := s.tuples[s.index]
	if s.targetKind == 0 {
		s.targetKind = targetTuple.Schema.GetKind(s.Attrs[1])
	}
	s.index++
	res, err := s.Selector(s.currentKind, s.currentTuple.Get(s.Attrs[0]), s.targetKind, targetTuple.Get(s.Attrs[1]))
	if err != nil {
		return
	}
	if res {
		result = NewTuple()
		for i, attr := range s.currentTuple.Attrs {
			result.Set(attr, s.currentTuple.Data[i])
		}
		for i, attr := range targetTuple.Attrs {
			result.Set(attr, targetTuple.Data[i])
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
func (s *JoinStream) Init(n *Node) error {
	if s.Selector == nil {
		s.Selector = Equal
	}
	if len(s.Inputs) != 2 {
		return ErrUnexpectedInputNumber
	}
	if len(s.Attrs) != 2 {
		return ErrUnexpectedAttrNumber
	}
	if err := s.Inputs[0].Init(n); err != nil {
		return err
	}
	return s.Inputs[1].Init(n)
}
func (s *JoinStream) Close() {
	s.Inputs[0].Close()
	s.Inputs[1].Close()
}
